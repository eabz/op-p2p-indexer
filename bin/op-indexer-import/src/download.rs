//! The `download` step: fetches every chunk of the range that is not on disk yet.
//!
//! Chunks are fetched with a fixed number of requests in flight. Each answer is written to
//! the chunk's temporary file as it arrives and as it travelled, in the service's content
//! encoding, so memory stays small whatever a chunk holds and nothing is compressed here.
//! The file gets its final name only once every block of the chunk has arrived (a chunk may
//! take several requests: an answer may cover less than was asked). Nothing is parsed and
//! nothing is verified: the request window is spent on the transfer.
//!
//! A chunk that fails for a reason that may pass (the service busy or limiting, a broken
//! connection) is fetched again from its start a few times, with capped, jittered backoff.
//! When a chunk fails for good, or the disk is nearly full, no new chunk is started, the
//! requests in flight finish and are written, and the step ends with a summary of what is
//! missing. It never spins.

use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use eyre::WrapErr;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chunk::{self, ChunkFile};
use crate::progress::{self, Rate};
use crate::source::{Encoding, HyperSync, Meters, SourceError};
use crate::state::{Chunk, LOW_SPACE_BYTES, MIN_SPACE_BYTES, Plan, State, write_atomic};

/// Attempts per chunk before the step stops.
const MAX_ATTEMPTS: u32 = 6;
/// Wait before the second attempt; doubled for each further one.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
/// Longest wait between two attempts.
const BACKOFF_CAP: Duration = Duration::from_secs(20);
/// Pieces of an answer waiting to be written, per chunk. A piece is what the HTTP client
/// hands over at once, tens of kilobytes; with the decoder that finds the cursor a request
/// in flight holds about a megabyte.
const WRITE_QUEUE_PIECES: usize = 16;
/// Open files a request in flight needs: its connection and its chunk file.
const FILES_PER_REQUEST: u64 = 2;
/// Open files the process needs besides: the standard streams, the lock, the directories
/// being listed, the L1 lookup's connections.
const FILES_BESIDES: u64 = 64;

/// Why a chunk was not downloaded.
#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("the service answered blocks {from}..{to} without advancing")]
    NoProgress { from: u64, to: u64 },
    #[error("failed to write the chunk: {0}")]
    Io(#[from] io::Error),
    #[error("write task failed: {0}")]
    Task(#[from] JoinError),
}

/// Makes sure the process may hold the files `requests` requests in flight need, raising its
/// soft limit up to the hard one if it has to.
///
/// # Errors
///
/// Returns an error, with the command that raises the limit, if the hard limit is too low
/// for `requests`.
pub(crate) fn ensure_open_files(requests: u64) -> eyre::Result<()> {
    #[cfg(unix)]
    {
        use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
        let needed = requests
            .saturating_mul(FILES_PER_REQUEST)
            .saturating_add(FILES_BESIDES);
        let limit = getrlimit(Resource::Nofile);
        if limit.current.is_none_or(|current| current >= needed) {
            return Ok(());
        }
        eyre::ensure!(
            limit.maximum.is_none_or(|maximum| maximum >= needed),
            "--requests {requests} needs about {needed} open files and this shell allows {}: \
             run `ulimit -n {needed}` first, or lower --requests",
            limit.maximum.unwrap_or_default()
        );
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(needed),
                maximum: limit.maximum,
            },
        )
        .wrap_err_with(|| {
            format!("failed to raise the open-files limit: run `ulimit -n {needed}` first")
        })?;
        info!(open_files = needed, "raised the open-files limit");
    }
    Ok(())
}

impl DownloadError {
    /// Whether another attempt at the chunk may succeed: the source says so, or the process
    /// or the system had no file to give for the chunk just then.
    fn may_pass(&self) -> bool {
        match self {
            Self::Source(err) => err.is_retryable(),
            Self::Io(err) => {
                #[cfg(unix)]
                {
                    use rustix::io::Errno;
                    let code = err.raw_os_error();
                    code == Some(Errno::MFILE.raw_os_error())
                        || code == Some(Errno::NFILE.raw_os_error())
                }
                #[cfg(not(unix))]
                {
                    let _ = err;
                    false
                }
            }
            Self::NoProgress { .. } | Self::Task(_) => false,
        }
    }
}

/// Downloads the missing chunks of `plan` from `source`, `requests` at a time, until all are
/// on disk, `cancel` fires, a chunk fails for good, or the disk is nearly full.
///
/// # Errors
///
/// Returns an error if the range is not complete when the step ends. Run it again to
/// continue.
pub(crate) async fn run(
    source: &HyperSync,
    state: &State,
    plan: &Plan,
    requests: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let missing = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || {
            plan.chunks()
                // A chunk already verified needs no download, even if `raw/` was deleted.
                .filter(|chunk| {
                    !state.raw_path(*chunk).exists()
                        && chunk::check(&state.verified_path(*chunk)) != ChunkFile::Present
                })
                .collect::<Vec<_>>()
        })
        .await?
    };
    let meters = Arc::new(Meters::default());
    let mut done = Progress::new(&missing, plan.chain.bedrock_block, Arc::clone(&meters));
    info!(
        chunks = done.total_chunks,
        blocks = done.total_blocks,
        requests,
        "download starting"
    );

    let mut queue = missing.into_iter();
    let mut tasks = JoinSet::new();
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Why no new chunk is started any more; the chunks in flight still finish.
    let mut stopped: Option<String> = None;

    loop {
        while stopped.is_none()
            && tasks.len() < requests
            && let Some(chunk) = queue.next()
        {
            let (source, path) = (source.clone(), state.raw_path(chunk));
            let meters = Arc::clone(&meters);
            tasks.spawn(async move { (chunk, fetch_chunk(&source, &meters, chunk, path).await) });
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                stopped = Some("stopped by signal".to_owned());
                break;
            }
            finished = tasks.join_next() => match finished {
                Some(Ok((chunk, Ok(written)))) => done.chunk_done(chunk, written),
                Some(Ok((chunk, Err(err)))) => {
                    warn!(from = chunk.from, to = chunk.to, %err, "chunk failed");
                    stopped.get_or_insert_with(|| {
                        format!("blocks {}..{}: {err}", chunk.from, chunk.to)
                    });
                }
                Some(Err(err)) => {
                    stopped.get_or_insert_with(|| format!("download task failed: {err}"));
                }
                None => break,
            },
            _ = tick.tick() => {
                let state = state.clone();
                let free = tokio::task::spawn_blocking(move || state.free_bytes()).await?;
                let free = free.wrap_err("failed to read the free disk space")?;
                done.log(tasks.len(), free);
                if free.is_some_and(|free| free < MIN_SPACE_BYTES) {
                    stopped.get_or_insert_with(|| {
                        format!(
                            "less than {} GiB free on the state directory's disk",
                            MIN_SPACE_BYTES >> 30
                        )
                    });
                }
            }
        }
    }
    // Only a signal leaves tasks here: their requests are dropped, and a chunk being written
    // leaves at most a temporary file.
    tasks.shutdown().await;

    let missing = done.summary();
    match stopped {
        None => Ok(()),
        Some(reason) => Err(eyre::eyre!(
            "download incomplete, {missing} of {} chunks missing: {reason}; run it again to \
             continue",
            done.total_chunks
        )),
    }
}

/// The blocks of one era of the chain, before the Bedrock block or from it on: they differ
/// in size by an order of magnitude, so what is left is estimated for each on its own.
#[derive(Debug)]
struct Era {
    /// Blocks still to download.
    left_blocks: u64,
    blocks: u64,
    /// Bytes of the chunk files written.
    bytes: u64,
    blocks_rate: Rate,
    bytes_rate: Rate,
}

impl Era {
    fn new() -> Self {
        Self {
            left_blocks: 0,
            blocks: 0,
            bytes: 0,
            blocks_rate: Rate::new(),
            bytes_rate: Rate::new(),
        }
    }

    /// Bytes still to download, from the bytes per block of the last minute (blocks grow
    /// within an era too), else of the run; `None` if blocks are left and none was downloaded
    /// yet, so their size is not known.
    fn left_bytes(&mut self) -> Option<u64> {
        let blocks_per_sec = self.blocks_rate.per_sec(self.blocks);
        let bytes_per_sec = self.bytes_rate.per_sec(self.bytes);
        if self.left_blocks == 0 {
            return Some(0);
        }
        let bytes_per_block = bytes_per_sec
            .checked_div(blocks_per_sec)
            .or_else(|| self.bytes.checked_div(self.blocks))?;
        Some(self.left_blocks.saturating_mul(bytes_per_block))
    }
}

/// What the step has done so far, for the progress lines and the summary.
#[derive(Debug)]
struct Progress {
    started: Instant,
    total_chunks: usize,
    total_blocks: u64,
    chunks: usize,
    /// First block of the second era.
    bedrock_block: u64,
    /// Before the Bedrock block, and from it on.
    eras: [Era; 2],
    /// What the requests in flight count as it happens.
    meters: Arc<Meters>,
    blocks_rate: Rate,
    wire_rate: Rate,
    /// Microseconds of decoding, so its recent share of a core can be told.
    decode_rate: Rate,
}

impl Progress {
    fn new(missing: &[Chunk], bedrock_block: u64, meters: Arc<Meters>) -> Self {
        let mut progress = Self {
            started: Instant::now(),
            total_chunks: missing.len(),
            total_blocks: missing.iter().map(|chunk| chunk.blocks()).sum(),
            chunks: 0,
            bedrock_block,
            eras: [Era::new(), Era::new()],
            meters,
            blocks_rate: Rate::new(),
            wire_rate: Rate::new(),
            decode_rate: Rate::new(),
        };
        for chunk in missing {
            let era = progress.era(*chunk);
            era.left_blocks = era.left_blocks.saturating_add(chunk.blocks());
        }
        progress
    }

    /// The era of `chunk`; a chunk never crosses the Bedrock block.
    const fn era(&mut self, chunk: Chunk) -> &mut Era {
        let [legacy, bedrock] = &mut self.eras;
        if chunk.from < self.bedrock_block {
            legacy
        } else {
            bedrock
        }
    }

    const fn chunk_done(&mut self, chunk: Chunk, disk_bytes: u64) {
        self.chunks = self.chunks.saturating_add(1);
        let era = self.era(chunk);
        era.left_blocks = era.left_blocks.saturating_sub(chunk.blocks());
        era.blocks = era.blocks.saturating_add(chunk.blocks());
        era.bytes = era.bytes.saturating_add(disk_bytes);
    }

    fn blocks(&self) -> u64 {
        self.eras.iter().map(|era| era.blocks).sum()
    }

    fn disk_bytes(&self) -> u64 {
        self.eras.iter().map(|era| era.bytes).sum()
    }

    /// Logs one progress line. The speeds are those of the last minute. `bytes_left` and
    /// `secs_left` are estimated from the bytes per block of each era and the speed on the
    /// wire, and are absent while an era with blocks left has not been sampled.
    /// `decode_cpu_percent` is the processor time spent decoding answers to find their
    /// cursors, in percent of one core: near 100 times the number of cores, the processor is
    /// the limit, not the line.
    fn log(&mut self, in_flight: usize, free_bytes: Option<u64>) {
        let blocks = self.blocks();
        let wire_bytes = self.meters.wire_bytes.load(Ordering::Relaxed);
        let wire_bytes_per_sec = self.wire_rate.per_sec(wire_bytes);
        let decode_micros = self.meters.decode_nanos.load(Ordering::Relaxed) / 1000;
        let [legacy, bedrock] = &mut self.eras;
        let bytes_left = legacy
            .left_bytes()
            .zip(bedrock.left_bytes())
            .map(|(legacy, bedrock)| legacy.saturating_add(bedrock));
        info!(
            chunks = self.chunks,
            of = self.total_chunks,
            blocks,
            blocks_per_sec = self.blocks_rate.per_sec(blocks),
            bytes_left,
            secs_left = bytes_left.and_then(|bytes| bytes.checked_div(wire_bytes_per_sec)),
            in_flight,
            wire_bytes_per_sec,
            decode_cpu_percent = self.decode_rate.per_sec(decode_micros) / 10_000,
            disk_bytes = self.disk_bytes(),
            free_bytes,
            "downloading"
        );
        if free_bytes.is_some_and(|free| free < LOW_SPACE_BYTES) {
            warn!(free_bytes, "the state directory's disk is running low");
        }
    }

    /// Logs the summary and returns the number of chunks still missing.
    fn summary(&self) -> usize {
        let secs = self.started.elapsed().as_secs().max(1);
        let missing = self.total_chunks.saturating_sub(self.chunks);
        info!(
            chunks = self.chunks,
            missing,
            blocks = self.blocks(),
            wire_bytes = self.meters.wire_bytes.load(Ordering::Relaxed),
            disk_bytes = self.disk_bytes(),
            disk_bytes_per_block = self.disk_bytes().checked_div(self.blocks()),
            blocks_per_sec = self.blocks() / secs,
            secs,
            "download ended"
        );
        missing
    }
}

/// Fetches one chunk into the file at `path`, starting over when an attempt fails for a
/// reason that may pass. Returns the size of the file written.
async fn fetch_chunk(
    source: &HyperSync,
    meters: &Meters,
    chunk: Chunk,
    path: PathBuf,
) -> Result<u64, DownloadError> {
    let mut backoff = BACKOFF_BASE;
    let mut attempt = 1;
    loop {
        match attempt_chunk(source, meters, chunk, &path).await {
            Err(err) if err.may_pass() && attempt < MAX_ATTEMPTS => {
                // Up to half of the wait is random, so parallel requests do not retry together.
                let wait = backoff.mul_f64(1.0 - fastrand::f64() / 2.0);
                let (from, to) = (chunk.from, chunk.to);
                if attempt > 1 {
                    warn!(from, to, attempt, %err, ?wait, "chunk failed again, retrying");
                } else {
                    debug!(from, to, %err, ?wait, "chunk failed, retrying");
                }
                sleep(wait).await;
                backoff = backoff.saturating_mul(2).min(BACKOFF_CAP);
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// One attempt at a chunk: every answer is passed to a writer as it arrives, and the file is
/// given its final name only if all of them arrived. The file starts with one byte naming
/// the content encoding, which every answer of the chunk must then have.
async fn attempt_chunk(
    source: &HyperSync,
    meters: &Meters,
    chunk: Chunk,
    path: &Path,
) -> Result<u64, DownloadError> {
    let (pieces_tx, pieces_rx) = mpsc::channel(WRITE_QUEUE_PIECES);
    let (complete_tx, complete_rx) = oneshot::channel();
    let writer = {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || write_chunk(&path, pieces_rx, complete_rx))
    };

    let fetched = async {
        let mut chunk_encoding = None;
        let mut cursor = chunk.from;
        while cursor < chunk.to {
            let accept = |encoding: Encoding| match chunk_encoding.replace(encoding) {
                None => Ok(Some(Bytes::copy_from_slice(&[encoding as u8]))),
                Some(first) if first == encoding => Ok(None),
                Some(first) => Err(SourceError::Malformed(format!(
                    "the service changed content encoding within a chunk, {first:?} to \
                     {encoding:?}"
                ))),
            };
            let answer = source
                .fetch_into(cursor, chunk.to, &pieces_tx, meters, accept)
                .await?;
            if answer.next_block <= cursor {
                return Err(DownloadError::NoProgress {
                    from: cursor,
                    to: chunk.to,
                });
            }
            cursor = answer.next_block;
        }
        Ok(())
    }
    .await;

    // The writer ends when the queue closes; it keeps the file only if told it is complete.
    drop(pieces_tx);
    if fetched.is_ok() {
        // A writer that already failed is not listening; its error is returned below.
        let _told = complete_tx.send(());
    } else {
        drop(complete_tx);
    }
    let written = writer.await?;
    match fetched {
        Ok(()) => Ok(written?),
        // The writer failing is why the source found nobody taking the answer.
        Err(DownloadError::Source(SourceError::Unwanted)) => Err(written
            .err()
            .map_or(SourceError::Unwanted.into(), DownloadError::Io)),
        Err(err) => Err(err),
    }
}

/// Writes the pieces of a chunk to its file as they arrive. The file gets its final name only
/// if `complete` is signalled after the last piece. Returns the file's size. Blocking.
fn write_chunk(
    path: &Path,
    mut pieces: mpsc::Receiver<Bytes>,
    complete: oneshot::Receiver<()>,
) -> io::Result<u64> {
    let mut written = 0_u64;
    write_atomic(path, |file| {
        let mut out = BufWriter::new(file);
        while let Some(piece) = pieces.blocking_recv() {
            out.write_all(&piece)?;
            written = written.saturating_add(u64::try_from(piece.len()).unwrap_or(u64::MAX));
        }
        complete.blocking_recv().map_err(|_abandoned| {
            io::Error::new(io::ErrorKind::Interrupted, "the chunk was not completed")
        })?;
        out.flush()
    })?;
    Ok(written)
}
