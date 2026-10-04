//! The `download` step: fetches every chunk of the range that is not on disk yet.
//!
//! Chunks are fetched with a fixed number of requests in flight. A chunk may take several
//! requests (an answer may cover less than was asked); it is written, compressed, only once
//! every block of it has arrived. Nothing is parsed beyond the cursor and nothing is verified:
//! the request window is spent on the transfer.
//!
//! A failed request is retried a few times with capped, jittered backoff. When the service
//! refuses for rate limits or the token, or retries run out, the step stops with a summary of
//! what is missing; it never spins.

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::source::{Source, SourceError};
use crate::state::{Chunk, Plan, State, write_atomic};

/// Attempts per request before the step gives up.
const MAX_ATTEMPTS: u32 = 6;
/// Wait before the second attempt; doubled for each further one.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
/// Longest wait between two attempts.
const BACKOFF_CAP: Duration = Duration::from_secs(20);
/// How often progress is logged.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
/// Compression level of the chunks on disk. Low, so writing keeps up with the transfer; the
/// answers are hex text and compress well at any level.
const COMPRESSION_LEVEL: i32 = 1;

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

/// Downloads the missing chunks of `plan` from `source`, `requests` at a time, until all are
/// on disk, `cancel` fires, or the service stops answering.
///
/// # Errors
///
/// Returns an error if the range is not complete when the step ends: it was cancelled, or
/// the service refused or kept failing. Run it again to continue.
pub(crate) async fn run<S: Source>(
    source: S,
    state: &State,
    plan: &Plan,
    requests: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let missing = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || {
            plan.chunks()
                .filter(|chunk| !state.raw_path(*chunk).exists())
                .collect::<Vec<_>>()
        })
        .await?
    };
    let total = missing.len();
    let total_blocks: u64 = missing.iter().map(|chunk| chunk.blocks()).sum();
    info!(
        chunks = total,
        blocks = total_blocks,
        requests,
        "download starting"
    );

    let source = Arc::new(source);
    let mut queue = missing.into_iter();
    let mut tasks = JoinSet::new();
    let mut progress = interval(PROGRESS_INTERVAL);
    progress.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let started = Instant::now();
    let (mut chunks_done, mut blocks_done) = (0_usize, 0_u64);
    // Bytes of the answers once decompressed, and of the chunk files written.
    let (mut answer_bytes, mut disk_bytes) = (0_u64, 0_u64);
    let mut stopped = None;

    loop {
        while tasks.len() < requests
            && let Some(chunk) = queue.next()
        {
            let (source, path) = (Arc::clone(&source), state.raw_path(chunk));
            tasks.spawn(async move { (chunk, fetch_chunk(&*source, chunk, path).await) });
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                stopped = Some("stopped by signal".to_owned());
                break;
            }
            finished = tasks.join_next() => match finished {
                Some(Ok((chunk, Ok((answers, written))))) => {
                    chunks_done = chunks_done.saturating_add(1);
                    blocks_done = blocks_done.saturating_add(chunk.blocks());
                    answer_bytes = answer_bytes.saturating_add(answers);
                    disk_bytes = disk_bytes.saturating_add(written);
                }
                Some(Ok((chunk, Err(err)))) => {
                    stopped = Some(format!("blocks {}..{}: {err}", chunk.from, chunk.to));
                    break;
                }
                Some(Err(err)) => {
                    stopped = Some(format!("download task failed: {err}"));
                    break;
                }
                None => break,
            },
            _ = progress.tick() => {
                let secs = started.elapsed().as_secs().max(1);
                let blocks_per_sec = blocks_done / secs;
                let left_blocks = total_blocks.saturating_sub(blocks_done);
                info!(
                    chunks = chunks_done,
                    of = total,
                    blocks_per_sec,
                    decompressed_bytes_per_sec = answer_bytes / secs,
                    disk_bytes_per_sec = disk_bytes / secs,
                    disk_bytes,
                    secs_left = left_blocks.checked_div(blocks_per_sec),
                    "downloading"
                );
            }
        }
    }
    // A chunk being written is finished or leaves only a temporary file; requests in flight
    // are dropped.
    tasks.shutdown().await;

    let secs = started.elapsed().as_secs().max(1);
    info!(
        chunks = chunks_done,
        missing = total.saturating_sub(chunks_done),
        blocks = blocks_done,
        decompressed_bytes = answer_bytes,
        disk_bytes,
        disk_bytes_per_block = disk_bytes.checked_div(blocks_done),
        blocks_per_sec = blocks_done / secs,
        secs,
        "download ended"
    );
    match stopped {
        None => Ok(()),
        Some(reason) => Err(eyre::eyre!(
            "download incomplete, {} of {total} chunks missing: {reason}; run it again to continue",
            total.saturating_sub(chunks_done)
        )),
    }
}

/// Fetches one chunk and writes it to `path`. Returns the size of the answers after
/// decompression (the bytes on the wire are not visible behind the HTTP client) and the size
/// of the file written.
async fn fetch_chunk<S: Source>(
    source: &S,
    chunk: Chunk,
    path: PathBuf,
) -> Result<(u64, u64), DownloadError> {
    let mut pages = Vec::new();
    let mut cursor = chunk.from;
    while cursor < chunk.to {
        let page = fetch_page(source, cursor, chunk.to).await?;
        if page.next_block <= cursor {
            return Err(DownloadError::NoProgress {
                from: cursor,
                to: chunk.to,
            });
        }
        cursor = page.next_block;
        pages.push(page.body);
    }
    let received = pages.iter().map(Bytes::len).sum::<usize>();
    let written = tokio::task::spawn_blocking(move || {
        write_atomic(&path, |file| {
            let mut out = zstd::stream::Encoder::new(file, COMPRESSION_LEVEL)?;
            for page in &pages {
                out.write_all(page)?;
            }
            out.finish()?;
            Ok(())
        })?;
        Ok::<_, io::Error>(std::fs::metadata(&path)?.len())
    })
    .await??;
    Ok((u64::try_from(received).unwrap_or(u64::MAX), written))
}

/// Fetches one page, retrying what may succeed on another attempt.
async fn fetch_page<S: Source>(
    source: &S,
    from: u64,
    to: u64,
) -> Result<crate::source::Page, SourceError> {
    let mut backoff = BACKOFF_BASE;
    let mut attempt = 1;
    loop {
        match source.fetch(from, to).await {
            Ok(page) => return Ok(page),
            Err(err) if err.is_retryable() && attempt < MAX_ATTEMPTS => {
                // Up to half of the wait is random, so parallel requests do not retry together.
                let wait = backoff.mul_f64(1.0 - fastrand::f64() / 2.0);
                if attempt > 1 {
                    warn!(from, to, attempt, %err, ?wait, "request failed again, retrying");
                } else {
                    debug!(from, to, %err, ?wait, "request failed, retrying");
                }
                sleep(wait).await;
                backoff = backoff.saturating_mul(2).min(BACKOFF_CAP);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}
