//! The `verify` step: rebuilds every downloaded block and checks it, offline.
//!
//! Each downloaded chunk is rebuilt and checked on its own (`block`), several at once, and
//! written as the bytes that passed (see `chunk`). Once every chunk is verified, the chunks are
//! linked to each other by their parent hashes and the last block is checked against the
//! anchor (a trusted hash, or the claim of a dispute game on L1: see `game`). The whole range
//! is then proven by the chain of parent hashes, and recorded as accepted: `load` loads
//! nothing without that record.
//!
//! What is proven for every block: its header hashes to its block hash and links to its
//! parent up to the anchor; its transactions root is the root over the stored transaction
//! encodings; its receipts root is the root over the stored receipts. That is what a peer
//! checks when these bytes are served to it.
//!
//! What is not proven here: senders. No signature is checked; the sender recorded with each
//! block is the one the service reports, and `load` recovers and checks it before archiving it.
//! A transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client before
//! Bedrock) has no signer and gets the zero address. They are counted.

mod block;
mod lists;
mod receipt;
mod transaction;

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, io};

use alloy_consensus::Header;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::chunk::{self, ChunkFile, Link};
use crate::progress::{self, Rate};
use crate::rows::RowsError;
use crate::state::{Anchor, Chunk, LOW_SPACE_BYTES, MIN_SPACE_BYTES, Plan, State, VerifiedRange};

/// Threads reading chunk links per verify thread: the work is waiting for the disk to open
/// files, not computing.
const LINK_READERS_PER_THREAD: usize = 2;
/// How often the linking pass looks whether its readers are done.
const LINK_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Compressed size of the chunks verified at once, whatever the number of threads; one chunk
/// is always allowed. A chunk takes about twenty times its compressed size while it is
/// verified (its text, then its rows), so this bounds `verify` to roughly 5 GB.
const IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;

/// A rule a block failed.
#[derive(Debug, thiserror::Error)]
enum Check {
    #[error("the block is not in the downloaded chunk")]
    Missing,
    #[error("transaction indexes are not 0, 1, 2, ...: found {found} at position {position}")]
    TransactionIndex { position: u64, found: u64 },
    #[error("transaction {index} has type {kind}, which is not known")]
    UnsupportedType { index: u64, kind: u8 },
    #[error("transaction {index} lacks the field `{field}`")]
    MissingField { index: u64, field: &'static str },
    #[error("transaction {index}: the field `{field}` is not in the expected form: {reason}")]
    Field {
        index: u64,
        field: &'static str,
        reason: String,
    },
    #[error("transaction {index} has an invalid signature value v")]
    SignatureV { index: u64 },
    #[error("the header has ommers hash {0}; blocks with ommers are not rebuilt")]
    Ommers(B256),
    #[error(
        "the rebuilt header hashes to {computed}, the block's hash is {reported}: a header \
         field, a transaction, a receipt or a log is not what the chain has"
    )]
    HeaderHash { computed: B256, reported: B256 },
    #[error("parent hash is {parent}, the block before has hash {previous}")]
    ParentLink { parent: B256, previous: B256 },
    #[error("the header lacks `mix_hash`, which a block from Bedrock on must have")]
    MissingMixHash,
}

/// Why a chunk was not verified.
#[derive(Debug, thiserror::Error)]
enum ChunkError {
    #[error("failed to write the verified chunk: {0}")]
    Io(io::Error),
    #[error("blocks {from}..{to}: {source}")]
    Rows {
        from: u64,
        to: u64,
        source: RowsError,
    },
    #[error("block {number}: {check}")]
    Block { number: u64, check: Check },
}

/// The fork activations of an OP Stack chain that change an encoding rebuilt here: Bedrock by
/// block number, the others in Unix seconds.
#[derive(Debug, Clone, Copy)]
struct Forks {
    /// Bedrock: the first block in the current format. Before it a header's `mix_hash` is
    /// zero, so a row without it can be rebuilt.
    bedrock_block: u64,
    /// Regolith: the L1-attributes deposit stops being a system transaction.
    regolith: u64,
    /// Canyon: the deposit nonce and receipt version become part of the hashed receipt.
    canyon: u64,
    /// Isthmus: the header carries the hash of an empty requests list.
    isthmus: u64,
}

/// What verified chunks held.
#[derive(Debug, Clone, Copy, Default)]
struct Stats {
    blocks: u64,
    transactions: u64,
    /// Transactions signed with all zeros.
    zero_signatures: u64,
    /// Pre-Bedrock blocks whose row lacked `mix_hash`, rebuilt with zero and proven by the
    /// header hash.
    rebuilt_header_fields: u64,
    /// Size of the verified chunk files written.
    disk_bytes: u64,
}

/// What a run has to do, as the state directory shows it at start.
#[derive(Debug)]
struct Todo {
    /// Chunks downloaded and not verified yet, with the size of their file.
    chunks: Vec<(Chunk, u64)>,
    /// Blocks in them.
    blocks: u64,
    /// Bytes of their downloaded files.
    raw_bytes: u64,
    already_verified: usize,
    not_downloaded: usize,
    /// Verified files that were damaged (cut short, or not a chunk) and were removed, to be
    /// verified again from their download.
    damaged: usize,
    /// Free space on the state directory's disk, where the system tells.
    free_bytes: Option<u64>,
}

/// Verifies every downloaded chunk of `plan` that is not verified yet, `threads` at a time,
/// then links the chunks up to the anchor and, if the whole range holds, records it as
/// accepted. The record of an earlier run is removed first, so it exists only while the range
/// on disk is one `verify` accepted.
///
/// With `from_block`, only the chunks from that block on are verified and nothing is linked
/// or accepted: a check of one part of the chain.
///
/// # Errors
///
/// Returns an error naming the block and the check if a chunk fails, and an error if the
/// range is not verified completely when the step ends (chunks not downloaded, cancelled, the
/// disk nearly full, or a broken link).
pub(crate) async fn run(
    state: &State,
    plan: &Plan,
    threads: usize,
    from_block: Option<u64>,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let todo = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || todo(&state, &plan, from_block)).await??
    };
    announce(&todo, threads, from_block)?;
    let forks = Forks {
        bedrock_block: plan.chain.bedrock_block,
        regolith: plan.chain.regolith_time,
        canyon: plan.chain.canyon_time,
        isthmus: plan.chain.isthmus_time,
    };

    let mut progress = Progress::new(&todo);
    let mut queue = todo.chunks.into_iter().peekable();
    let mut tasks = JoinSet::new();
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut failure = None;
    // Compressed bytes of the chunks being verified, which bounds the memory they take.
    let mut in_flight_bytes = 0_u64;
    loop {
        while failure.is_none()
            && !cancel.is_cancelled()
            && tasks.len() < threads
            && let Some((chunk, bytes)) = queue.next_if(|(_, bytes)| {
                tasks.is_empty() || in_flight_bytes.saturating_add(*bytes) <= IN_FLIGHT_BYTES
            })
        {
            in_flight_bytes = in_flight_bytes.saturating_add(bytes);
            let (raw, verified) = (state.raw_path(chunk), state.verified_path(chunk));
            tasks.spawn_blocking(move || {
                let result = block::verify_chunk(&forks, chunk, &raw, &verified);
                (chunk, bytes, result)
            });
        }
        // Chunks being verified run to their end: blocking work cannot be interrupted.
        tokio::select! {
            finished = tasks.join_next() => match finished {
                Some(Ok((_, bytes, Ok(verified)))) => {
                    in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
                    progress.chunk_done(verified, bytes);
                }
                Some(Ok((chunk, bytes, Err(err)))) => {
                    in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
                    let file = state.raw_path(chunk);
                    // The first failure is the one reported; the chunks already running
                    // usually fail for the same reason.
                    if failure.is_none() {
                        error!(%err, file = %file.display(), "chunk failed verification");
                    }
                    failure.get_or_insert_with(|| {
                        format!(
                            "{err} (chunk {}; delete it and download again if the data is wrong)",
                            file.display()
                        )
                    });
                }
                Some(Err(err)) => {
                    failure.get_or_insert_with(|| format!("verify task failed: {err}"));
                }
                None => break,
            },
            _ = tick.tick() => {
                let disk = state.clone();
                let free = tokio::task::spawn_blocking(move || disk.free_bytes()).await??;
                progress.log(tasks.len(), free);
                if free.is_some_and(|free| free < MIN_SPACE_BYTES) {
                    failure.get_or_insert_with(|| {
                        format!(
                            "less than {} GiB free on the state directory's disk",
                            MIN_SPACE_BYTES >> 30
                        )
                    });
                }
            }
        }
    }
    progress.summary();
    if let Some(failure) = failure {
        eyre::bail!("verification failed: {failure}");
    }
    // A stopped run does not go on to link the range: that is the next run's work.
    eyre::ensure!(
        !cancel.is_cancelled(),
        "verify was stopped before every chunk was verified: run `verify` again to continue"
    );
    if let Some(from_block) = from_block {
        info!(
            from_block,
            "the chunks from that block on verify; the range is NOT accepted: run `verify` \
             without --from-block before `load`"
        );
        return Ok(());
    }

    let (state, plan, cancel) = (state.clone(), *plan, cancel.clone());
    tokio::task::spawn_blocking(move || accept(&state, &plan, threads, &cancel)).await?
}

/// Logs what the run will do, and refuses it if the verified chunks clearly cannot fit on
/// the disk.
fn announce(todo: &Todo, threads: usize, from_block: Option<u64>) -> eyre::Result<()> {
    info!(
        chunks = todo.chunks.len(),
        blocks = todo.blocks,
        raw_bytes = todo.raw_bytes,
        already_verified = todo.already_verified,
        not_downloaded = todo.not_downloaded,
        damaged_removed = todo.damaged,
        free_bytes = todo.free_bytes,
        from_block,
        threads,
        "verify starting"
    );
    // A verified chunk is about as large as its downloaded one; half of that is the least
    // that could do.
    eyre::ensure!(
        todo.free_bytes
            .is_none_or(|free| free >= todo.raw_bytes / 2),
        "the verified chunks will not fit: {} bytes of downloaded chunks to verify, {} bytes \
         free on the state directory's disk",
        todo.raw_bytes,
        todo.free_bytes.unwrap_or_default()
    );
    Ok(())
}

/// Removes the record of an earlier run and lists what this one has to verify. Blocking.
fn todo(state: &State, plan: &Plan, from_block: Option<u64>) -> io::Result<Todo> {
    // Whatever an earlier run accepted is not accepted again until this one ends well.
    state.clear_verified()?;
    let mut todo = Todo {
        chunks: Vec::new(),
        blocks: 0,
        raw_bytes: 0,
        already_verified: 0,
        not_downloaded: 0,
        damaged: 0,
        free_bytes: state.free_bytes()?,
    };
    let from_block = from_block.unwrap_or_default();
    for chunk in plan.chunks().filter(|chunk| chunk.to > from_block) {
        let verified = state.verified_path(chunk);
        match chunk::check(&verified)? {
            ChunkFile::Present => {
                todo.already_verified = todo.already_verified.saturating_add(1);
                continue;
            }
            ChunkFile::Missing => {}
            // Not verified: it goes, and the chunk is verified again from its download (or
            // downloaded again, if that is gone too).
            ChunkFile::Damaged => {
                fs::remove_file(&verified)?;
                warn!(
                    file = %verified.display(),
                    "a verified chunk is damaged (cut short, or not a chunk); removed it, to \
                     verify it again"
                );
                todo.damaged = todo.damaged.saturating_add(1);
            }
        }
        match state.raw_path(chunk).metadata() {
            Ok(file) => {
                todo.blocks = todo.blocks.saturating_add(chunk.blocks());
                todo.raw_bytes = todo.raw_bytes.saturating_add(file.len());
                todo.chunks.push((chunk, file.len()));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                todo.not_downloaded = todo.not_downloaded.saturating_add(1);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(todo)
}

/// What the run has verified so far, for the progress lines and the summary.
#[derive(Debug)]
struct Progress {
    started: Instant,
    total_chunks: usize,
    /// Bytes of the downloaded chunks to verify, and of those verified so far: the work
    /// that scales, which the time left is estimated from.
    total_raw_bytes: u64,
    raw_bytes: u64,
    chunks: usize,
    done: Stats,
    blocks_rate: Rate,
    transactions_rate: Rate,
    raw_rate: Rate,
}

impl Progress {
    fn new(todo: &Todo) -> Self {
        Self {
            started: Instant::now(),
            total_chunks: todo.chunks.len(),
            total_raw_bytes: todo.raw_bytes,
            raw_bytes: 0,
            chunks: 0,
            done: Stats::default(),
            blocks_rate: Rate::new(),
            transactions_rate: Rate::new(),
            raw_rate: Rate::new(),
        }
    }

    const fn chunk_done(&mut self, chunk: Stats, raw_bytes: u64) {
        self.chunks = self.chunks.saturating_add(1);
        self.raw_bytes = self.raw_bytes.saturating_add(raw_bytes);
        self.done.blocks = self.done.blocks.saturating_add(chunk.blocks);
        self.done.transactions = self.done.transactions.saturating_add(chunk.transactions);
        self.done.zero_signatures = self
            .done
            .zero_signatures
            .saturating_add(chunk.zero_signatures);
        self.done.rebuilt_header_fields = self
            .done
            .rebuilt_header_fields
            .saturating_add(chunk.rebuilt_header_fields);
        self.done.disk_bytes = self.done.disk_bytes.saturating_add(chunk.disk_bytes);
    }

    /// Logs one progress line. The time left is the downloaded bytes still to verify over
    /// those verified per second in the last minute: bytes, unlike blocks, cost about the
    /// same everywhere in the chain.
    fn log(&mut self, busy_threads: usize, free_bytes: Option<u64>) {
        let raw_bytes_per_sec = self.raw_rate.per_sec(self.raw_bytes);
        info!(
            chunks = self.chunks,
            of = self.total_chunks,
            blocks = self.done.blocks,
            transactions = self.done.transactions,
            blocks_per_sec = self.blocks_rate.per_sec(self.done.blocks),
            transactions_per_sec = self.transactions_rate.per_sec(self.done.transactions),
            raw_bytes_per_sec,
            secs_left = self
                .total_raw_bytes
                .saturating_sub(self.raw_bytes)
                .checked_div(raw_bytes_per_sec),
            busy_threads,
            disk_bytes = self.done.disk_bytes,
            free_bytes,
            "verifying"
        );
        if free_bytes.is_some_and(|free| free < LOW_SPACE_BYTES) {
            warn!(free_bytes, "the state directory's disk is running low");
        }
    }

    fn summary(&self) {
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            chunks = self.chunks,
            missing = self.total_chunks.saturating_sub(self.chunks),
            blocks = self.done.blocks,
            transactions = self.done.transactions,
            zero_signature_transactions = self.done.zero_signatures,
            rebuilt_header_fields = self.done.rebuilt_header_fields,
            disk_bytes = self.done.disk_bytes,
            blocks_per_sec = self.done.blocks / secs,
            secs,
            "chunks verified in this run"
        );
    }
}

/// How far the range is verified, as the linking pass found it.
#[derive(Debug)]
struct Linked {
    /// Chunks with a verified file.
    verified: usize,
    /// Chunks of the plan.
    total: usize,
    /// The first broken link, if any.
    broken: Option<String>,
    /// Hash of the range's last block, when its chunk is verified.
    last_hash: Option<B256>,
}

impl fmt::Display for Linked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {} chunks verified", self.verified, self.total)?;
        if let Some(broken) = &self.broken {
            write!(f, "; {broken}")?;
        }
        Ok(())
    }
}

/// Links the verified chunks and checks the top against the anchor; if the whole range holds,
/// records it as accepted, which `load` requires. Blocking.
fn accept(
    state: &State,
    plan: &Plan,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let Some(linked) = link_chunks(state, plan, threads, cancel)? else {
        eyre::bail!("verify was stopped while linking the chunks: run `verify` again");
    };
    let (None, true, Some(last_hash)) = (
        &linked.broken,
        linked.verified == linked.total,
        linked.last_hash,
    ) else {
        eyre::bail!("range not verified: {linked}");
    };
    state.write_verified(&VerifiedRange {
        first: plan.first,
        last: plan.last,
        last_hash,
        anchor: plan.anchor,
        verified_at_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_secs()),
    })?;
    info!(
        chunks = linked.total,
        first = plan.first,
        last = plan.last,
        anchor = %plan.anchor,
        "range verified up to the anchor"
    );
    Ok(())
}

/// Checks that each verified chunk continues the one before and that the last block of the
/// range matches the anchor. Returns `None` if `cancel` fired before it was done. Blocking.
///
/// It reads the first 64 bytes of every chunk's file, on several threads: opening hundreds
/// of thousands of files is what takes the time, and most of it is waiting for the disk. The
/// chain itself is then checked in order, in memory.
fn link_chunks(
    state: &State,
    plan: &Plan,
    threads: usize,
    cancel: &CancellationToken,
) -> io::Result<Option<Linked>> {
    let chunks: Vec<Chunk> = plan.chunks().collect();
    let total = chunks.len();
    let readers = threads.saturating_mul(LINK_READERS_PER_THREAD).max(1);
    info!(chunks = total, readers, "linking the verified chunks");
    let Some(links) = read_links(state, &chunks, readers, cancel)? else {
        return Ok(None);
    };

    let mut linked = Linked {
        verified: 0,
        total,
        broken: None,
        last_hash: None,
    };
    let mut previous: Option<Link> = None;
    for (chunk, link) in chunks.iter().zip(links) {
        // A chunk that is not verified yet.
        let Some(link) = link else {
            previous = None;
            continue;
        };
        linked.verified = linked.verified.saturating_add(1);
        if let Some(previous) = previous
            && previous.last_hash != link.first_parent
            && linked.broken.is_none()
        {
            linked.broken = Some(format!(
                "block {}: parent hash is {}, the block before has hash {}",
                chunk.from, link.first_parent, previous.last_hash
            ));
        }
        if chunk.to > plan.last {
            linked.last_hash = Some(link.last_hash);
            if linked.broken.is_none() {
                linked.broken = check_top(state, plan, *chunk, link)?;
            }
        }
        previous = Some(link);
    }
    Ok(Some(linked))
}

/// Reads how each of `chunks` attaches to its neighbours, in the order of `chunks`, on
/// `readers` threads; `None` for a chunk without a verified file. The outer `None` means
/// `cancel` fired first.
fn read_links(
    state: &State,
    chunks: &[Chunk],
    readers: usize,
    cancel: &CancellationToken,
) -> io::Result<Option<Vec<Option<Link>>>> {
    let done = AtomicUsize::new(0);
    let share = chunks.len().div_ceil(readers).max(1);
    let read = |part: &[Chunk]| -> io::Result<Vec<Option<Link>>> {
        let mut links = Vec::with_capacity(part.len());
        for chunk in part {
            if cancel.is_cancelled() {
                break;
            }
            links.push(match chunk::read_link(&state.verified_path(*chunk)) {
                Ok(link) => Some(link),
                Err(err) if err.kind() == io::ErrorKind::NotFound => None,
                Err(err) => return Err(err),
            });
            done.fetch_add(1, Ordering::Relaxed);
        }
        Ok(links)
    };
    let parts = std::thread::scope(|scope| {
        let readers: Vec<_> = chunks
            .chunks(share)
            .map(|part| scope.spawn(|| read(part)))
            .collect();
        // This thread reports while the others read.
        let mut logged = Instant::now();
        while readers.iter().any(|reader| !reader.is_finished()) {
            std::thread::park_timeout(LINK_POLL_INTERVAL);
            if logged.elapsed() >= progress::INTERVAL {
                info!(
                    chunks = done.load(Ordering::Relaxed),
                    of = chunks.len(),
                    "linking"
                );
                logged = Instant::now();
            }
        }
        readers
            .into_iter()
            .map(|reader| {
                reader
                    .join()
                    .unwrap_or_else(|_panic| Err(io::Error::other("a link reader panicked")))
            })
            .collect::<io::Result<Vec<_>>>()
    })?;
    if cancel.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(parts.into_iter().flatten().collect()))
}

/// Checks the last block of the range, in the verified `chunk`, against the plan's anchor.
/// Returns what is wrong, if anything. Blocking.
fn check_top(state: &State, plan: &Plan, chunk: Chunk, link: Link) -> io::Result<Option<String>> {
    let game = match plan.anchor {
        Anchor::Hash(anchor) => {
            return Ok((link.last_hash != anchor).then(|| {
                format!(
                    "block {}: hash is {}, the anchor is {anchor}",
                    plan.last, link.last_hash
                )
            }));
        }
        Anchor::Game(game) => game,
    };
    // The game's claim covers the state root and the withdrawals root of the last header.
    let (_, blocks) = chunk::read(&state.verified_path(chunk))?;
    let header = blocks
        .last()
        .map(|block| Header::decode(&mut &block.encoded.header[..]))
        .transpose()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        .ok_or(io::ErrorKind::UnexpectedEof)?;
    Ok(game
        .check(
            link.last_hash,
            (header.timestamp, plan.chain.isthmus_time),
            header.state_root,
            header.withdrawals_root,
        )
        .err()
        .map(|err| err.to_string()))
}
