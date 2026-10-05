//! The exporter: `server --export` (D11, D12).
//!
//! Exactly one server of a deployment runs it, with the only R2 write key. It seals the
//! committed blocks above the manifest's last chunk, from its tail, into the next chunk:
//!
//! 1. a block is added to the chunk being written only once it is at or below the L1
//!    finalized head and has its receipts (D11), so a chunk is sealed when all its blocks
//!    are;
//! 2. the chunk ends where the writer's deterministic rule ends it (D4);
//! 3. it is uploaded, then the manifest segment listing it is written (D8); the tails of
//!    every server then drop its blocks;
//! 4. the source writes a new hash-index generation when one is due (D9).
//!
//! It resumes from the manifest: the next chunk starts after its last one. Boundaries are
//! deterministic and names content-addressed, so a crash re-seals the same chunk under the
//! same name, and repeating an upload is harmless.

use std::time::Duration;

use alloy_primitives::BlockNumber;
use op_indexer_chunks::{ChunkRecord, ChunkWriter};
use op_indexer_primitives::ReadLimits;
use op_indexer_storage::ArchiveStore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::source::ChunkSource;
use crate::{R2Archive, ServerError};

/// How often the tail is looked at for blocks that became sealable.
const EXPORT_INTERVAL: Duration = Duration::from_secs(10);
/// Blocks read from the tail at once.
const READ_LIMITS: ReadLimits = ReadLimits {
    items: 256,
    bytes: 64 * 1024 * 1024,
    lowest: 0,
};
/// Shortest time between two warnings that the tail does not hold the next block to seal.
const MISSING_WARN_INTERVAL: Duration = Duration::from_mins(10);

/// Seals chunks from the tail of an [`R2Archive`] and lists them in its manifest.
#[derive(Debug)]
pub struct Exporter<S> {
    archive: R2Archive<S>,
    /// Recorded in each manifest segment it writes.
    exporter_id: String,
}

impl<S: ChunkSource> Exporter<S> {
    /// An exporter for `archive`, named `exporter_id` in the manifest.
    #[must_use]
    pub const fn new(archive: R2Archive<S>, exporter_id: String) -> Self {
        Self {
            archive,
            exporter_id,
        }
    }

    /// Exports until `cancel` fires. A chunk being written when it fires is dropped: the next
    /// run writes it again from the tail.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if a chunk cannot be written or uploaded, or the manifest or
    /// the tail cannot be read or written.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), ServerError> {
        let chain = self.archive.chain();
        let mut next = self.next_block();
        let mut writer: Option<ChunkWriter> = None;
        let mut warned: Option<tokio::time::Instant> = None;
        info!(next, exporter = %self.exporter_id, "exporter started");
        let mut tick = tokio::time::interval(EXPORT_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                _ = tick.tick() => {}
            }
            let tail = self.archive.tail();
            let Some(finalized) = tail.heads().await?.finalized else {
                continue;
            };
            loop {
                let blocks = tail.blocks(next, READ_LIMITS).await?;
                if blocks.is_empty() {
                    let behind = tail
                        .range()
                        .await?
                        .is_some_and(|(first, _)| first.number > next);
                    if behind && warned.is_none_or(|at| at.elapsed() >= MISSING_WARN_INTERVAL) {
                        warned = Some(tokio::time::Instant::now());
                        warn!(
                            next,
                            "the tail does not hold the next block to seal; it is sealed once \
                             range sync stores it"
                        );
                    }
                    break;
                }
                let mut sealable = true;
                for block in blocks {
                    // D11: finalized on L1, with its receipts.
                    if next > finalized.number || block.encoded.receipts.is_none() {
                        sealable = false;
                        break;
                    }
                    let record =
                        ChunkRecord::new(&block).map_err(ServerError::chunk("chunk record"))?;
                    let chunk = writer.get_or_insert_with(|| ChunkWriter::new(chain, next));
                    let ends = chunk
                        .push(record)
                        .map_err(ServerError::chunk("chunk write"))?;
                    next = next.saturating_add(1);
                    if ends && let Some(chunk) = writer.take() {
                        self.seal(chunk).await?;
                    }
                }
                if !sealable || cancel.is_cancelled() {
                    break;
                }
            }
        }
    }

    /// The block after the manifest's last chunk.
    fn next_block(&self) -> BlockNumber {
        self.archive
            .sealed()
            .last()
            .map_or(0, |last| last.number.saturating_add(1))
    }

    /// Uploads a full chunk and lists it; the tail then drops its blocks.
    async fn seal(&self, writer: ChunkWriter) -> Result<(), ServerError> {
        let sealed = writer
            .finish()
            .map_err(ServerError::chunk("chunk finish"))?;
        let chunk = self
            .archive
            .source()
            .publish(sealed, &self.exporter_id)
            .await
            .map_err(ServerError::source("chunk publish"))?;
        info!(
            first = chunk.first,
            last = chunk.last,
            "chunk sealed and listed"
        );
        self.archive.prune_tail().await
    }
}
