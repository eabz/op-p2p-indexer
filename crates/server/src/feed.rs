//! Caller-owned read-ahead for sealed history.
//!
//! Each range reader owns its producer and bounded buffer. Dropping the reader aborts
//! remote reads immediately. Readers share memory and open-stream budgets, never cursors.

use std::sync::Arc;

use alloy_primitives::BlockNumber;
use futures_util::StreamExt;
use op_indexer_primitives::{ArchivedBlock, ReadLimits};
use op_indexer_storage::StorageError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::archive::{Sealed, remote, size};
use crate::source::ChunkSource;

/// KiB one feed holds ahead of its reader (16 MiB): a couple of hundred blocks, longer to
/// serve than R2's time to the first byte. KiB are the unit of the permits.
const MAX_FEED_KIB: u32 = 16 * 1024;
/// What one open chunk stream holds, with the server's read options (two ranges of about a
/// segment in flight, each compressed and decoded, and the decoded batch handed on).
pub(crate) const STREAM_BYTES: u64 = 32 << 20;
/// Blocks queued in one feed, whatever their size.
const FEED_BLOCKS: usize = 4096;

/// A block and the read-ahead places it holds until it is taken.
type Fed = Result<(ArchivedBlock, [OwnedSemaphorePermit; 2]), StorageError>;

/// The feeds of every reader.
#[derive(Debug, Clone)]
pub(crate) struct Feeds {
    /// KiB of decoded blocks all feeds may hold.
    read_ahead: Arc<Semaphore>,
    /// Chunk streams open at once.
    streams: Arc<Semaphore>,
}

/// One reader's feed.
#[derive(Debug)]
pub(crate) struct Feed {
    blocks: mpsc::Receiver<Fed>,
    tasks: JoinSet<()>,
}

impl Feeds {
    /// Feeds within `budget` bytes in all.
    pub(crate) fn new(budget: u64) -> Self {
        let half = budget / 2;
        let kib = usize::try_from(half / 1024).unwrap_or(usize::MAX);
        let streams = usize::try_from(half / STREAM_BYTES).unwrap_or(usize::MAX);
        Self {
            read_ahead: Arc::new(Semaphore::new(
                kib.clamp(MAX_FEED_KIB as usize, Semaphore::MAX_PERMITS),
            )),
            streams: Arc::new(Semaphore::new(streams.clamp(1, Semaphore::MAX_PERMITS))),
        }
    }

    /// Starts a feed of the sealed chunks from `from` to the last one listed now.
    pub(crate) fn start<S: ChunkSource>(
        &self,
        source: &S,
        sealed: &Sealed,
        from: BlockNumber,
    ) -> Feed {
        let chunks = Arc::clone(&sealed.chunks);
        let (sender, blocks) = mpsc::channel(FEED_BLOCKS);
        let source = source.clone();
        let read_ahead = Arc::clone(&self.read_ahead);
        let streams = Arc::clone(&self.streams);
        let own = Arc::new(Semaphore::new(MAX_FEED_KIB as usize));
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            for chunk in chunks.iter().filter(|chunk| chunk.last >= from) {
                // Held while this chunk's stream is open.
                let Ok(_open) = Arc::clone(&streams).acquire_owned().await else {
                    return;
                };
                let mut stream = source.stream(chunk, from.max(chunk.first));
                // Each chunk's stream yields its blocks in order from the one asked for.
                while let Some(block) = stream.next().await {
                    let fed = match block.map_err(remote("chunk read")) {
                        Ok(block) => {
                            let kib = u32::try_from(size(&block) / 1024)
                                .unwrap_or(u32::MAX)
                                .saturating_add(1);
                            // Closed only with the feeds, which the store outlives.
                            let (Ok(global), Ok(local)) = (
                                Arc::clone(&read_ahead)
                                    .acquire_many_owned(kib.min(MAX_FEED_KIB))
                                    .await,
                                Arc::clone(&own)
                                    .acquire_many_owned(kib.min(MAX_FEED_KIB))
                                    .await,
                            ) else {
                                return;
                            };
                            Ok((block, [global, local]))
                        }
                        Err(err) => Err(err),
                    };
                    let failed = fed.is_err();
                    if sender.send(fed).await.is_err() || failed {
                        return;
                    }
                }
            }
        });
        Feed { blocks, tasks }
    }
}

impl Feed {
    /// Takes the next bounded batch, releasing each block's read-ahead permits.
    pub(crate) async fn read(
        &mut self,
        limits: ReadLimits,
    ) -> Result<Vec<ArchivedBlock>, StorageError> {
        let mut blocks = Vec::new();
        let mut bytes = 0_usize;
        while blocks.len() < limits.items && bytes < limits.bytes {
            let Some(block) = self.blocks.recv().await else {
                if let Some(Err(err)) = self.tasks.join_next().await {
                    return Err(remote("range reader task")(std::io::Error::other(err)));
                }
                break;
            };
            let (block, _places) = block?;
            bytes = bytes.saturating_add(usize::try_from(size(&block)).unwrap_or(usize::MAX));
            blocks.push(block);
        }
        Ok(blocks)
    }
}
