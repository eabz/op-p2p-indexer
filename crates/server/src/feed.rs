//! Read-ahead for consumers' sequential reads of sealed history (D4, `docs/serving.md` 5.2).
//!
//! A consumer (a subscription catching up, a Flight `DoGet`) reads the committed chain with
//! repeated [`ArchiveStore::blocks`](op_indexer_storage::ArchiveStore::blocks) calls, each
//! from the block after the last one it got. The first call starts a *feed*: a task that
//! streams the sealed chunks one after the other from that block, verified and decompressed,
//! into a buffer the next calls take from. The feed runs ahead of its reader by at most
//! [`MAX_FEED_KIB`], and all feeds together by [`MAX_READ_AHEAD_KIB`], so the next chunk's
//! GET starts while the reader is still in this one. Nothing is written to disk and nothing is
//! kept once read: this is read-ahead, not a cache.
//!
//! A feed is found again by the block its reader asks for next. Feeds not read for
//! [`FEED_IDLE`] are dropped, which drops their GETs; at most [`MAX_FEEDS`] run at once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use alloy_primitives::BlockNumber;
use futures_util::StreamExt;
use op_indexer_primitives::{ArchivedBlock, ReadLimits};
use op_indexer_storage::StorageError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;

use crate::archive::{Sealed, remote, size};
use crate::source::{ChunkRange, ChunkSource};

/// KiB one feed holds ahead of its reader (64 MiB): about a quarter of a chunk, a second or
/// two of serving, longer than R2's time to the first byte.
const MAX_FEED_KIB: u32 = 64 * 1024;
/// KiB all feeds hold ahead of their readers (512 MiB). KiB are the unit of the permits.
const MAX_READ_AHEAD_KIB: u32 = 512 * 1024;
/// Feeds at once; past it the one read longest ago is dropped.
const MAX_FEEDS: usize = 64;
/// A feed not read for this long is dropped.
const FEED_IDLE: Duration = Duration::from_mins(1);
/// Blocks queued in one feed, whatever their size.
const FEED_BLOCKS: usize = 4096;

/// A block and the read-ahead places it holds until it is taken.
type Fed = Result<(ArchivedBlock, [OwnedSemaphorePermit; 2]), StorageError>;

/// The feeds of every reader.
#[derive(Debug, Clone)]
pub(crate) struct Feeds {
    feeds: Arc<Mutex<HashMap<BlockNumber, Feed>>>,
    read_ahead: Arc<Semaphore>,
}

/// One reader's feed.
#[derive(Debug)]
struct Feed {
    blocks: mpsc::Receiver<Fed>,
    task: JoinHandle<()>,
    used: Instant,
}

impl Drop for Feed {
    fn drop(&mut self) {
        // Its GETs go with it.
        self.task.abort();
    }
}

impl Feeds {
    pub(crate) fn new() -> Self {
        Self {
            feeds: Arc::default(),
            read_ahead: Arc::new(Semaphore::new(MAX_READ_AHEAD_KIB as usize)),
        }
    }

    /// Reads sealed blocks from `from` up to `limits`, from the feed that waits at `from` or
    /// a new one. The run ends at the end of the sealed range.
    pub(crate) async fn read<S: ChunkSource>(
        &self,
        source: &S,
        sealed: &Sealed,
        from: BlockNumber,
        limits: ReadLimits,
    ) -> Result<Vec<ArchivedBlock>, StorageError> {
        let found = self
            .feeds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&from);
        let mut feed = match found {
            Some(feed) => feed,
            None => self.start(source, sealed, from),
        };
        let mut blocks = Vec::new();
        let mut bytes: usize = 0;
        let mut ended = false;
        while blocks.len() < limits.items && bytes < limits.bytes {
            match feed.blocks.recv().await {
                Some(Ok((block, _places))) => {
                    bytes =
                        bytes.saturating_add(usize::try_from(size(&block)).unwrap_or(usize::MAX));
                    blocks.push(block);
                }
                // The feed is dropped with its error: the next read starts over.
                Some(Err(err)) => return Err(err),
                None => {
                    ended = true;
                    break;
                }
            }
        }
        if !ended {
            let next = from.saturating_add(u64::try_from(blocks.len()).unwrap_or(u64::MAX));
            feed.used = Instant::now();
            self.keep(next, feed);
        }
        Ok(blocks)
    }

    /// Starts a feed of the sealed chunks from `from` to the last one listed now.
    fn start<S: ChunkSource>(&self, source: &S, sealed: &Sealed, from: BlockNumber) -> Feed {
        let chunks: Vec<ChunkRange> = sealed
            .chunks
            .iter()
            .filter(|chunk| chunk.last >= from)
            .copied()
            .collect();
        let (sender, blocks) = mpsc::channel(FEED_BLOCKS);
        let source = source.clone();
        let read_ahead = Arc::clone(&self.read_ahead);
        let own = Arc::new(Semaphore::new(MAX_FEED_KIB as usize));
        let task = tokio::spawn(async move {
            for chunk in chunks {
                let mut stream = source.stream(&chunk, from.max(chunk.first));
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
                                    .acquire_many_owned(kib.min(MAX_READ_AHEAD_KIB))
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
        Feed {
            blocks,
            task,
            used: Instant::now(),
        }
    }

    /// Keeps `feed` for its reader's next read, at `next`.
    fn keep(&self, next: BlockNumber, feed: Feed) {
        let mut feeds = self.feeds.lock().unwrap_or_else(PoisonError::into_inner);
        feeds.retain(|_, feed| feed.used.elapsed() < FEED_IDLE);
        if feeds.len() >= MAX_FEEDS
            && let Some(oldest) = feeds
                .iter()
                .min_by_key(|(_, feed)| feed.used)
                .map(|(next, _)| *next)
        {
            feeds.remove(&oldest);
        }
        feeds.insert(next, feed);
    }
}
