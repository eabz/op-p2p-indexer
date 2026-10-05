//! The `download` step: fetches every chunk of the range that is not on disk yet.
//!
//! Chunks are fetched with a fixed number of requests in flight. Each answer is written to
//! the chunk's temporary file as it arrives and as it travelled, in the service's content
//! encoding, so memory stays small whatever a chunk holds and nothing is compressed here.
//! The file gets its final name only once every block of the chunk has arrived (a chunk may
//! take several requests: an answer may cover less than was asked) and its rows are checked
//! for the fields their forks have ([`fill::lacking`], what the fill's scan lists). The
//! service's servers do not all answer alike: on Base some leave whole columns out of an answer
//! (`mix_hash`, deposits' `source_hash`, `mint`, `deposit_nonce`) that the same query asked
//! again has. A chunk whose rows lack a field is asked for again, up to [`COMPLETE_ANSWERS`]
//! times, and the most complete answer is kept; the fill takes what it still lacks. Nothing
//! is verified here.
//!
//! `--refetch-incomplete` also reads every chunk on disk not sealed yet, with its fill
//! ([`scan`]), and asks again for those whose rows still lack a field or cannot be read: a new
//! answer replaces the chunk (and its fill, which belonged to the old one) only if it lacks
//! less, every row counted. The scan's list is kept in `refetch.json` and shortened as chunks
//! are done, so a run stopped by the service's rate limit (HTTP 429, which a couple of short
//! waits do not outlast) goes on from where it was without scanning again.
//!
//! A chunk that fails for a reason that may pass (the service busy or limiting, a broken
//! connection) is fetched again from its start a few times, with capped, jittered backoff.
//! When a chunk fails for good, or the disk is nearly full, no new chunk is started, the
//! requests in flight finish and are written, and the step ends with a summary of what is
//! missing. It never spins.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use eyre::WrapErr;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::backoff::Backoff;
use crate::cli::ScanMode;
use crate::fill;
use crate::progress::{self, Rate};
use crate::scan;
use crate::source::{Encoding, HyperSync, Meters, SourceError};
use crate::state::{
    Chunk, LOW_SPACE_BYTES, Lacking, MIN_SPACE_BYTES, Plan, State, covered, remove_if_exists,
};
use crate::verify::Forks;

/// Pieces of an answer waiting to be written, per chunk. A piece is what the HTTP client
/// hands over at once, tens of kilobytes; with the decoder that finds the cursor a request
/// in flight holds about a megabyte.
const WRITE_QUEUE_PIECES: usize = 16;
/// Open files a request in flight needs: its connection and its chunk file.
const FILES_PER_REQUEST: u64 = 2;
/// Open files the process needs besides: the standard streams, the lock, the directories
/// being listed, the L1 lookup's connections.
const FILES_BESIDES: u64 = 64;
/// Answers asked for a chunk while each lacks a field; then the most complete is kept.
const COMPLETE_ANSWERS: u32 = 8;
/// Answers in a row no more complete than the best, after which the best is kept: a field no
/// server has for the chunk (an upgrade deposit's mint, which it has none of).
const UNIMPROVED_ANSWERS: u32 = 3;
/// Attempts at an answer the service refuses for the request rate (HTTP 429), the first
/// included, before the run stops.
const RATE_LIMITED_ATTEMPTS: u32 = 3;
/// Wait before asking again for a chunk whose answer lacked a field.
const INCOMPLETE_WAIT: Duration = Duration::from_millis(250);

/// Why a chunk was not downloaded.
#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("the service answered blocks {from}..{to} without advancing")]
    NoProgress { from: u64, to: u64 },
    #[error("none of the {answers} answers for blocks {from}..{to} could be read as rows")]
    Unreadable { from: u64, to: u64, answers: u32 },
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
            Self::NoProgress { .. } | Self::Unreadable { .. } | Self::Task(_) => false,
        }
    }
}

/// A chunk to download.
#[derive(Debug, Clone, Copy)]
struct Job {
    chunk: Chunk,
    /// Whether it is a chunk on disk asked for again: an answer replaces it only if it lacks
    /// fewer fields, every row counted.
    on_disk: bool,
}

/// What `--refetch-incomplete` asks for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Refetch {
    /// Scan the chunks on disk again, even with a list kept from the last scan.
    pub(crate) rescan: bool,
    /// How the scan reads a chunk.
    pub(crate) scan: ScanMode,
    /// Requests in flight while chunks are asked for again.
    pub(crate) requests: usize,
}

/// Counts the fields an answer's rows lack, a few answers at a time: the reading is CPU-bound.
#[derive(Debug, Clone)]
struct Checker {
    forks: Forks,
    permits: Arc<Semaphore>,
}

impl Checker {
    fn new(plan: &Plan, threads: usize) -> Self {
        Self {
            forks: Forks::new(plan.chain),
            permits: Arc::new(Semaphore::new(threads)),
        }
    }

    /// How many fields the rows of the answer at `path` lack; `u64::MAX` if they cannot be
    /// read (an answer cut short or garbled is the least complete there is).
    async fn lacking(&self, path: &Path) -> u64 {
        let Ok(_permit) = self.permits.acquire().await else {
            return u64::MAX;
        };
        let (forks, path) = (self.forks, path.to_owned());
        let counted = tokio::task::spawn_blocking(move || fill::lacking(&forks, &path, None)).await;
        match counted {
            Ok(Ok((lacking, _))) => lacking,
            Ok(Err(err)) => {
                debug!(%err, "an answer that cannot be read");
                u64::MAX
            }
            Err(err) => {
                debug!(%err, "the check of an answer failed");
                u64::MAX
            }
        }
    }
}

/// Downloads the missing chunks of `plan` from `source`, `requests` at a time, until all are
/// on disk, `cancel` fires, a chunk fails for good, the service keeps limiting requests, or
/// the disk is nearly full; with `refetch`, also the chunks on disk whose rows, with their
/// fill, lack a field: those the last scan listed (`refetch.json`), else a new scan's, in
/// block order, the list kept as they are done. Answers are checked on `threads` threads.
///
/// # Errors
///
/// Returns an error if the range is not complete when the step ends. Run it again to
/// continue.
pub(crate) async fn run(
    source: &HyperSync,
    state: &State,
    plan: &Plan,
    mut requests: usize,
    threads: usize,
    refetch: Option<Refetch>,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let (jobs, mut refetching) = jobs(state, plan, refetch, threads, cancel).await?;
    if let Some(refetch) = refetch {
        requests = refetch.requests;
    }
    let checker = Checker::new(plan, threads);
    let refetch_total = refetching.len();
    // Whether chunks were done since the list was last written: it is, at the progress line.
    let mut unkept = false;
    let meters = Arc::new(Meters::default());
    let mut done = Progress::new(&jobs, plan.chain.bedrock_block, Arc::clone(&meters));
    info!(
        chunks = done.total_chunks,
        blocks = done.total_blocks,
        requests,
        "download starting"
    );

    let mut queue = jobs.into_iter();
    let mut tasks = JoinSet::new();
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Why no new chunk is started any more; the chunks in flight still finish.
    let mut stopped: Option<String> = None;
    let mut rate_limited = false;

    loop {
        while stopped.is_none()
            && tasks.len() < requests
            && let Some(job) = queue.next()
        {
            let (source, checker, state) = (source.clone(), checker.clone(), state.clone());
            let meters = Arc::clone(&meters);
            tasks.spawn(async move {
                let fetched = fetch_chunk(&source, &meters, &checker, &state, job).await;
                (job, fetched)
            });
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                stopped = Some("stopped by signal".to_owned());
                break;
            }
            finished = tasks.join_next() => match finished {
                Some(Ok((job, Ok(fetched)))) => {
                    done.chunk_done(job, fetched);
                    unkept |= refetching.remove(&job.chunk.from).is_some();
                }
                Some(Ok((Job { chunk, .. }, Err(err)))) => {
                    if matches!(err, DownloadError::Source(SourceError::RateLimited)) {
                        rate_limited = true;
                        stopped.get_or_insert_with(|| {
                            "HyperSync is rate limiting requests (HTTP 429)".to_owned()
                        });
                    } else {
                        warn!(from = chunk.from, to = chunk.to, %err, "chunk failed");
                        stopped.get_or_insert_with(|| {
                            format!("blocks {}..{}: {err}", chunk.from, chunk.to)
                        });
                    }
                }
                Some(Err(err)) => {
                    stopped.get_or_insert_with(|| format!("download task failed: {err}"));
                }
                None => break,
            },
            _ = tick.tick() => {
                let disk = state.clone();
                let free = tokio::task::spawn_blocking(move || disk.free_bytes()).await?;
                let free = free.wrap_err("failed to read the free disk space")?;
                done.log(tasks.len(), free);
                if unkept {
                    keep_refetch(state, plan, &refetching).await?;
                    unkept = false;
                }
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

    if refetch.is_some() {
        keep_refetch(state, plan, &refetching).await?;
    }
    let missing = done.summary(refetching.len());
    let left = (missing, done.total_chunks, refetching.len(), refetch_total);
    ended(stopped, rate_limited, left)
}

/// How the step ended: `left` is the chunks missing of all, and those still to ask for again
/// of the refetch's.
fn ended(
    stopped: Option<String>,
    rate_limited: bool,
    (missing, total, refetch_left, refetch_total): (usize, usize, usize, usize),
) -> eyre::Result<()> {
    match stopped {
        None => Ok(()),
        Some(reason) if rate_limited => Err(eyre::eyre!(
            "{reason}: {refetch_left} of {refetch_total} chunks to ask for again left, and \
             {missing} of {total} chunks in all; run `download` again later, which goes on from \
             the list kept in refetch.json"
        )),
        Some(reason) => Err(eyre::eyre!(
            "download incomplete, {missing} of {total} chunks missing: {reason}; run it again to \
             continue"
        )),
    }
}

/// The chunks to download: those not on disk, then with `refetch` those on disk to ask for
/// again (the list kept, by first block, as the map returned), in block order.
async fn jobs(
    state: &State,
    plan: &Plan,
    refetch: Option<Refetch>,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<(Vec<Job>, BTreeMap<u64, Lacking>)> {
    let missing = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || {
            let mut missing = Vec::new();
            // A chunk `verify` sealed and uploaded needs no download: its file is gone.
            let sealed = state.sealed_through()?;
            for chunk in plan.chunks() {
                if !state.raw_path(chunk).try_exists()? && !covered(sealed, chunk) {
                    // A fill belongs to the download it was fetched for: one left from an
                    // earlier download of the chunk goes before the new one is written.
                    remove_if_exists(&state.fill_path(chunk))?;
                    missing.push(Job {
                        chunk,
                        on_disk: false,
                    });
                }
            }
            io::Result::Ok(missing)
        })
        .await??
    };
    let mut jobs = missing;
    // The chunks on disk still to ask for again, by first block: kept in `refetch.json`.
    let mut refetching: BTreeMap<u64, Lacking> = BTreeMap::new();
    if let Some(refetch) = refetch {
        let listed = refetch_list(state, plan, refetch, threads, cancel).await?;
        let absent: std::collections::BTreeSet<u64> =
            jobs.iter().map(|job| job.chunk.from).collect();
        for lacking in listed {
            // A chunk whose file is gone is downloaded as missing.
            if !absent.contains(&lacking.from) {
                jobs.push(Job {
                    chunk: lacking.chunk(),
                    on_disk: true,
                });
                refetching.insert(lacking.from, lacking);
            }
        }
    }
    Ok((jobs, refetching))
}

/// The chunks to ask for again: the list the last scan kept, unless `refetch` asks for a new
/// scan or there is none for this plan; then a new scan's, which is kept.
async fn refetch_list(
    state: &State,
    plan: &Plan,
    refetch: Refetch,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<Vec<Lacking>> {
    if !refetch.rescan {
        let (state, plan) = (state.clone(), *plan);
        let kept = tokio::task::spawn_blocking(move || state.read_refetch(&plan))
            .await?
            .wrap_err("failed to read refetch.json")?;
        if let Some(listed) = kept {
            info!(
                chunks = listed.len(),
                "asking again for the chunks the last scan listed (refetch.json; --rescan scans \
                 again)"
            );
            return Ok(listed);
        }
    }
    let found = scan::incomplete(state, plan, refetch.scan, threads, cancel).await?;
    keep(state, plan, found.clone()).await?;
    Ok(found)
}

/// Writes the chunks still to ask for again to `refetch.json`.
async fn keep_refetch(
    state: &State,
    plan: &Plan,
    refetching: &BTreeMap<u64, Lacking>,
) -> eyre::Result<()> {
    keep(state, plan, refetching.values().copied().collect()).await
}

/// Writes `chunks` to `refetch.json`.
async fn keep(state: &State, plan: &Plan, chunks: Vec<Lacking>) -> eyre::Result<()> {
    let (state, plan) = (state.clone(), *plan);
    tokio::task::spawn_blocking(move || state.write_refetch(&plan, chunks))
        .await?
        .wrap_err("failed to write refetch.json")
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
    /// Chunks asked for more than once for an answer lacking a field, and those kept lacking
    /// one (the fill takes the rest).
    retried: usize,
    incomplete: usize,
    /// Chunks on disk asked for again whose answer replaced them, and those kept as they were.
    replaced: usize,
    unreplaced: usize,
}

impl Progress {
    fn new(jobs: &[Job], bedrock_block: u64, meters: Arc<Meters>) -> Self {
        let mut progress = Self {
            started: Instant::now(),
            total_chunks: jobs.len(),
            total_blocks: jobs.iter().map(|job| job.chunk.blocks()).sum(),
            chunks: 0,
            bedrock_block,
            eras: [Era::new(), Era::new()],
            meters,
            blocks_rate: Rate::new(),
            wire_rate: Rate::new(),
            decode_rate: Rate::new(),
            retried: 0,
            incomplete: 0,
            replaced: 0,
            unreplaced: 0,
        };
        for job in jobs {
            let era = progress.era(job.chunk);
            era.left_blocks = era.left_blocks.saturating_add(job.chunk.blocks());
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

    fn chunk_done(&mut self, job: Job, fetched: Fetched) {
        let chunk = job.chunk;
        let kept = fetched.bytes > 0;
        self.chunks = self.chunks.saturating_add(1);
        self.retried = self
            .retried
            .saturating_add(usize::from(fetched.answers > 1));
        self.incomplete = self
            .incomplete
            .saturating_add(usize::from(kept && fetched.lacking > 0));
        if job.on_disk {
            if kept {
                self.replaced = self.replaced.saturating_add(1);
            } else {
                self.unreplaced = self.unreplaced.saturating_add(1);
            }
        }
        if kept && fetched.lacking > 0 {
            debug!(
                from = chunk.from,
                to = chunk.to,
                lacking = fetched.lacking,
                "chunk kept lacking fields"
            );
        }
        let era = self.era(chunk);
        era.left_blocks = era.left_blocks.saturating_sub(chunk.blocks());
        era.blocks = era.blocks.saturating_add(chunk.blocks());
        era.bytes = era.bytes.saturating_add(fetched.bytes);
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

    /// Logs the summary, with the chunks on disk still to ask for again (`refetch_left`), and
    /// returns the number of chunks still missing.
    fn summary(&self, refetch_left: usize) -> usize {
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
            retried_chunks = self.retried,
            incomplete_chunks = self.incomplete,
            replaced_chunks = self.replaced,
            unreplaced_chunks = self.unreplaced,
            refetch_left,
            "download ended"
        );
        missing
    }
}

/// What fetching one chunk gave.
#[derive(Debug, Clone, Copy)]
struct Fetched {
    /// Size of the answer kept; zero if none was.
    bytes: u64,
    /// Fields the chunk's rows lack, as kept.
    lacking: u64,
    /// Answers asked for.
    answers: u32,
}

/// Fetches `job`'s chunk, asking again while its answer lacks a field (up to
/// [`COMPLETE_ANSWERS`], or [`UNIMPROVED_ANSWERS`] in a row no better). The chunk's file
/// always holds the most complete answer so far: an answer replaces it, and its fill, which
/// belonged to the one before, only if it lacks fewer fields.
async fn fetch_chunk(
    source: &HyperSync,
    meters: &Meters,
    checker: &Checker,
    state: &State,
    job: Job,
) -> Result<Fetched, DownloadError> {
    let path = state.raw_path(job.chunk);
    // A killed run's is removed when the directory is opened.
    let answer = path.with_extension("answer.tmp");
    // A chunk on disk: what its answer lacks, every row counted, which a new one must beat.
    let lacking = if job.on_disk {
        checker.lacking(&path).await
    } else {
        u64::MAX
    };
    let mut fetched = Fetched {
        bytes: 0,
        lacking,
        answers: 0,
    };
    let mut unimproved = 0_u32;
    while fetched.answers < COMPLETE_ANSWERS && unimproved < UNIMPROVED_ANSWERS {
        if fetched.answers > 0 {
            sleep(INCOMPLETE_WAIT).await;
        }
        let bytes = fetch_answer(source, meters, job.chunk, &answer).await?;
        fetched.answers = fetched.answers.saturating_add(1);
        let lacking = checker.lacking(&answer).await;
        // An answer that cannot be read (`u64::MAX`) is never kept.
        if lacking < fetched.lacking {
            let (answer, path, fill) = (answer.clone(), path.clone(), state.fill_path(job.chunk));
            tokio::task::spawn_blocking(move || {
                // The fill first: a crash between leaves the old answer without one, which
                // the fill makes again.
                remove_if_exists(&fill)?;
                fs::rename(&answer, &path)
            })
            .await??;
            (fetched.bytes, fetched.lacking) = (bytes, lacking);
            unimproved = 0;
            if lacking == 0 {
                break;
            }
        } else {
            tokio::fs::remove_file(&answer).await?;
            unimproved = unimproved.saturating_add(1);
        }
    }
    // A new chunk with no answer that could be read is not on disk: the run says so.
    if !job.on_disk && fetched.bytes == 0 {
        return Err(DownloadError::Unreadable {
            from: job.chunk.from,
            to: job.chunk.to,
            answers: fetched.answers,
        });
    }
    Ok(fetched)
}

/// Fetches one answer for `chunk` into the file at `path`, starting over when an attempt
/// fails for a reason that may pass. A refusal for the request rate is tried again
/// [`RATE_LIMITED_ATTEMPTS`] times only: the service's limit lasts longer than a run should
/// wait, so the run stops and says so. Returns the size of the file written.
async fn fetch_answer(
    source: &HyperSync,
    meters: &Meters,
    chunk: Chunk,
    path: &Path,
) -> Result<u64, DownloadError> {
    let (mut backoff, mut limited) = (Backoff::new(), Backoff::rate_limited());
    loop {
        match attempt_chunk(source, meters, chunk, path).await {
            Err(err @ DownloadError::Source(SourceError::RateLimited)) => {
                let Some(wait) = limited
                    .next()
                    .filter(|_| limited.attempt() <= RATE_LIMITED_ATTEMPTS)
                else {
                    return Err(err);
                };
                warn!(
                    from = chunk.from,
                    to = chunk.to,
                    ?wait,
                    "the service is limiting requests: waiting"
                );
                sleep(wait).await;
            }
            Err(err) if err.may_pass() => {
                let attempt = backoff.attempt();
                let Some(wait) = backoff.next() else {
                    return Err(err);
                };
                let (from, to) = (chunk.from, chunk.to);
                if attempt > 1 {
                    warn!(from, to, attempt, %err, ?wait, "chunk failed again, retrying");
                } else {
                    debug!(from, to, %err, ?wait, "chunk failed, retrying");
                }
                sleep(wait).await;
            }
            result => return result,
        }
    }
}

/// One attempt at a chunk: every answer is passed to a writer as it arrives, and the file at
/// `path` is kept only if all of them arrived. The file starts with one byte naming the
/// content encoding, which every answer of the chunk must then have.
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

/// Writes the pieces of a chunk to the file at `path` as they arrive, synced, and keeps it
/// only if `complete` is signalled after the last piece. Returns the file's size. Blocking.
fn write_chunk(
    path: &Path,
    mut pieces: mpsc::Receiver<Bytes>,
    complete: oneshot::Receiver<()>,
) -> io::Result<u64> {
    let mut written = 0_u64;
    let result = File::create(path).and_then(|file| {
        let mut out = BufWriter::new(file);
        while let Some(piece) = pieces.blocking_recv() {
            out.write_all(&piece)?;
            written = written.saturating_add(u64::try_from(piece.len()).unwrap_or(u64::MAX));
        }
        complete.blocking_recv().map_err(|_abandoned| {
            io::Error::new(io::ErrorKind::Interrupted, "the chunk was not completed")
        })?;
        out.into_inner()
            .map_err(io::IntoInnerError::into_error)?
            .sync_all()
    });
    if let Err(err) = result {
        // Best effort: a leftover is removed when the directory is next opened.
        let _removed = fs::remove_file(path);
        return Err(err);
    }
    Ok(written)
}
