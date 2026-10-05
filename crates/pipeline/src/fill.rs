//! Missed blocks: finding the holes in the unsafe chain between the archive's tip and the head,
//! and storing the blocks peers sent for them.
//!
//! Every [`CHECK_INTERVAL`] the fill task looks for the lowest hole above the archive's last
//! block, within [`MAX_FILL_SPAN`] of the head: the heights from the first missing one up to
//! the parent of the next stored canonical block, whose hash that block names, so the span is
//! trusted through it. Holes come from gossip that skipped blocks, a restart, or a node that
//! starts above its archive (a new server's gossip chain begins thousands of blocks above the
//! sealed range). It asks the fetcher for the span, again only after [`ASK_AGAIN`] if it is
//! still there, and stores what comes back highest block first, so each closes the gap below
//! the canonical block above it (the store's fork choice, "closes a gap below the head").
//! The unsafe store's memory cap bounds what fills hold.
//!
//! It does not fetch (the fetcher does, verified by the hash chain down from the span's top).

use std::time::{Duration, Instant};

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, EncodedBlock, FillRequest};
use op_indexer_storage::{ArchiveStore, StorageError, Store, UnsafeStore};
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::PipelineError;
use crate::ingest::INSERT;
use crate::recover::{RecoverError, recover_encoded};
use crate::retry::{RetryError, retry};

/// Heights below the unsafe head a hole may reach to be filled: about 12 hours of a 1 s
/// chain, a few hundred MB at most of unsafe blocks. Older holes are left to range sync.
const MAX_FILL_SPAN: u64 = 32_768;
/// How often the unsafe chain is looked at for holes.
const CHECK_INTERVAL: Duration = Duration::from_secs(10);
/// How long a span asked for is not asked for again while it is being fetched.
const ASK_AGAIN: Duration = Duration::from_mins(2);

/// The lowest hole in the unsafe chain above the archive's last block (or [`MAX_FILL_SPAN`]
/// below the head, if that is higher): from its first missing height up to the parent of the
/// next stored canonical block. `None` without a hole, or if a store cannot be read.
async fn lowest_hole<U: UnsafeStore, A: ArchiveStore>(
    store: &U,
    archive: &A,
) -> Option<FillRequest> {
    let head = store.head().await.ok()??;
    let tip = archive.range().await.ok()?.map(|(_, tip)| tip.number);
    let floor = tip
        .map_or(0, |tip| tip.saturating_add(1))
        .max(head.number.saturating_sub(MAX_FILL_SPAN));
    let span = usize::try_from(head.number.saturating_sub(floor))
        .ok()?
        .saturating_add(1);
    // The canonical run up from the floor ends at the first missing height.
    let run = store.canonical_headers(floor, span, true).await.ok()?;
    let first = run
        .last()
        .map_or(floor, |held| held.block.number.saturating_add(1));
    let mut above = first.saturating_add(1);
    while above <= head.number {
        let stored = store.canonical_headers(above, 1, true).await.ok()?;
        if let Some(stored) = stored.first() {
            let top = BlockRef {
                number: above.checked_sub(1)?,
                hash: stored.parent_hash,
            };
            return Some(FillRequest { top, first });
        }
        above = above.saturating_add(1);
    }
    None
}

/// Asks for the lowest hole on `requests` every [`CHECK_INTERVAL`], and stores the blocks
/// received on `filled` (each batch a consecutive span, ascending, verified by the fetcher),
/// until the channel closes or `cancel` fires.
///
/// # Errors
///
/// Returns [`PipelineError::Storage`] if the unsafe store fails in a way retrying cannot fix,
/// and [`PipelineError::Task`] if sender recovery panics.
pub(crate) async fn run<U: UnsafeStore, A: ArchiveStore>(
    store: U,
    archive: A,
    requests: mpsc::Sender<FillRequest>,
    mut filled: mpsc::Receiver<Vec<EncodedBlock>>,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    let mut check = interval(CHECK_INTERVAL);
    check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first height of the span asked for last, and when.
    let mut asked: Option<(BlockNumber, Instant)> = None;
    loop {
        let batch = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            _ = check.tick() => {
                if let Some(request) = lowest_hole(&store, &archive).await
                    && asked.is_none_or(|(first, at)| first != request.first || at.elapsed() >= ASK_AGAIN)
                    && requests.try_send(request).is_ok()
                {
                    debug!(from = request.first, to = request.top.number, "asking peers for missed blocks");
                    asked = Some((request.first, Instant::now()));
                }
                continue;
            }
            batch = filled.recv() => batch,
        };
        // Closed: the fetcher has stopped.
        let Some(batch) = batch else { return Ok(()) };
        let blocks = match recover_encoded(batch).await {
            Ok(blocks) => blocks,
            Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
            Err(err) => {
                warn!(%err, "blocks fetched for a missed span dropped");
                continue;
            }
        };
        let (Some(lowest), Some(highest)) = (blocks.first(), blocks.last()) else {
            continue;
        };
        let (from, to) = (lowest.block.header.number, highest.block.header.number);
        let mut stored = 0_usize;
        // Highest first: each fills the height below a canonical block.
        for block in blocks.iter().rev() {
            match retry(&cancel, Store::Unsafe, "unsafe insert", || {
                store.insert(block)
            })
            .await
            {
                Ok(outcome) => stored = stored.saturating_add(usize::from(outcome.stored)),
                Err(RetryError::Cancelled) => return Ok(()),
                Err(RetryError::Storage(
                    err @ (StorageError::InvalidBlock { .. }
                    | StorageError::UnsupportedTransaction { .. }),
                )) => {
                    warn!(number = block.block.header.number, %err, "fetched block dropped");
                    break;
                }
                Err(RetryError::Storage(source)) => {
                    return Err(PipelineError::Storage {
                        operation: INSERT,
                        source,
                    });
                }
            }
        }
        info!(
            from,
            to, stored, "missed unsafe blocks fetched from execution peers"
        );
    }
}
