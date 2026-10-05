//! Caller-owned read-ahead for sealed history.
//!
//! Each range reader owns its producer and bounded buffer. Dropping the reader aborts
//! remote reads immediately. Readers share memory and open-stream budgets, never cursors.

use std::sync::Arc;

use alloy_primitives::BlockNumber;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use op_indexer_chunks::{Lend, StreamReads};
use op_indexer_primitives::{ArchivedBlock, ReadLimits};
use op_indexer_storage::StorageError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::archive::{Sealed, remote, size};
use crate::source::ChunkSource;

/// KiB one feed holds ahead of its reader (16 MiB): a couple of hundred blocks, longer to
/// serve than R2's time to the first byte. KiB are the unit of the permits.
const MAX_FEED_KIB: u32 = 16 * 1024;
/// Bytes of segments one GET of a chunk stream covers: about a segment (1 MiB compressed),
/// so that a range decoded at once stays small.
const RANGE_BYTES: u64 = 1 << 20;
/// GETs a chunk stream always has in flight, and those it may have while the budget lends:
/// R2 answers a GET in 100 to 200 ms, so a stream reads about `in flight × RANGE_BYTES` per
/// round trip.
const BASE_IN_FLIGHT: usize = 2;
const MAX_IN_FLIGHT: usize = 12;
/// MiB one open chunk stream holds at least: its base GETs, compressed, and three ranges
/// decoded (two being decoded, one handed on), at most eight times their size (blocks
/// compress up to about sevenfold): `(BASE_IN_FLIGHT + 3 × 8) × 1 MiB`. A GET it is lent past
/// them holds a range's MiB more.
const STREAM_MIB: u32 = 26;
/// MiB a GET lent past a stream's base holds: its range, compressed.
const RANGE_MIB: u32 = 1;
/// Decoded bytes of the current chunk left below which the next chunk's stream opens: a few
/// round trips of reading, so its index and first ranges arrive before the boundary.
const AHEAD_BYTES: u64 = 128 << 20;
/// A chunk's blocks, as the source streams them.
type ChunkStream = BoxStream<'static, std::io::Result<ArchivedBlock>>;
/// How the first block is read: one range.
pub(crate) const ONE_RANGE: StreamReads = StreamReads::fixed(RANGE_BYTES, 1);
/// How peers' requests read a chunk: narrow, outside the read budget (`budget` limits them).
pub(crate) const PEER_READS: StreamReads = StreamReads::fixed(RANGE_BYTES, BASE_IN_FLIGHT);
/// Blocks queued in one feed, whatever their size.
const FEED_BLOCKS: usize = 4096;

/// A block and the read-ahead places it holds until it is taken.
type Fed = Result<(ArchivedBlock, [OwnedSemaphorePermit; 2]), StorageError>;

/// Shared read-ahead and open-stream budgets for independently owned reader feeds.
#[derive(Debug, Clone)]
pub(crate) struct Feeds {
    /// KiB of decoded blocks all feeds may hold.
    read_ahead: Arc<Semaphore>,
    /// MiB the open chunk streams may hold: each takes [`STREAM_MIB`] while open, and is lent
    /// a range's for each GET past its base while some are left. Released MiB go to a stream
    /// waiting to open first, so lending never keeps a reader out.
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
        let mib = usize::try_from(half >> 20).unwrap_or(usize::MAX);
        Self {
            read_ahead: Arc::new(Semaphore::new(
                kib.clamp(MAX_FEED_KIB as usize, Semaphore::MAX_PERMITS),
            )),
            streams: Arc::new(Semaphore::new(
                mib.clamp(STREAM_MIB as usize, Semaphore::MAX_PERMITS),
            )),
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
        // Every chunk stream of the feed borrows past its base from the open streams' MiB.
        let reads = StreamReads {
            range_bytes: RANGE_BYTES,
            in_flight: BASE_IN_FLIGHT,
            lend: Some(Lend {
                budget: Arc::clone(&streams),
                per_get: RANGE_MIB,
                max_in_flight: MAX_IN_FLIGHT,
            }),
        };
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            // The chunks are in block order: from the one holding `from` on.
            let first = chunks.partition_point(|chunk| chunk.last < from);
            let mut chunks = chunks.get(first..).unwrap_or_default().iter().peekable();
            // The next chunk's stream, opened ahead with its place, if the budget had room.
            let mut ahead: Option<(ChunkStream, OwnedSemaphorePermit)> = None;
            while let Some(chunk) = chunks.next() {
                let start = from.max(chunk.first);
                // Its place is held while this chunk's stream is open.
                let (mut stream, _open) = if let Some(opened) = ahead.take() {
                    opened
                } else {
                    let Ok(open) = Arc::clone(&streams).acquire_many_owned(STREAM_MIB).await else {
                        return;
                    };
                    (source.stream(chunk, start, reads.clone()), open)
                };
                let (mut read, mut bytes) = (0_u64, 0_u64);
                // Each chunk's stream yields its blocks in order from the one asked for.
                while let Some(block) = stream.next().await {
                    let fed = match block.map_err(remote("chunk read")) {
                        Ok(block) => {
                            let size = size(&block);
                            (read, bytes) = (read.saturating_add(1), bytes.saturating_add(size));
                            let kib = u32::try_from(size / 1024)
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
                    // Near this chunk's end, the next one's index and first ranges are read, so
                    // that the boundary costs no round trips: only with a place to spare,
                    // which a stream waiting to open gets first.
                    let left = chunk
                        .last
                        .saturating_sub(start.saturating_add(read).saturating_sub(1));
                    if ahead.is_none()
                        && left.saturating_mul(bytes / read.max(1)) <= AHEAD_BYTES
                        && let Some(next) = chunks.peek()
                        && let Ok(open) = Arc::clone(&streams).try_acquire_many_owned(STREAM_MIB)
                    {
                        ahead = Some((
                            source.stream(next, from.max(next.first), reads.clone()),
                            open,
                        ));
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
