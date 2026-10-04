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
//! What is not proven: senders. No signature is checked. The sender recorded for the optional
//! database rows is the one the service reports; a transaction signed with all zeros (an
//! L1-to-L2 message of OP Mainnet's client before Bedrock) has none and gets the zero
//! address. They are counted.

mod block;
mod receipt;
mod transaction;

use std::fmt;
use std::io;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use alloy_consensus::Header;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::chunk::{self, Link};
use crate::progress::{self, Rate};
use crate::rows::RowsError;
use crate::state::{Anchor, Chunk, Plan, State, VerifiedRange};

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

/// The fork activations of an OP Stack chain that change an encoding rebuilt here, in Unix
/// seconds.
#[derive(Debug, Clone, Copy)]
struct Forks {
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
    already_verified: usize,
    not_downloaded: usize,
}

/// Verifies every downloaded chunk of `plan` that is not verified yet, `threads` at a time,
/// then links the chunks up to the anchor and, if the whole range holds, records it as
/// accepted. The record of an earlier run is removed first, so it exists only while the range
/// on disk is one `verify` accepted.
///
/// # Errors
///
/// Returns an error naming the block and the check if a chunk fails, and an error if the
/// range is not verified completely when the step ends (chunks not downloaded, cancelled, or
/// a broken link).
pub(crate) async fn run(
    state: &State,
    plan: &Plan,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let todo = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || todo(&state, &plan)).await??
    };
    info!(
        chunks = todo.chunks.len(),
        blocks = todo.blocks,
        already_verified = todo.already_verified,
        not_downloaded = todo.not_downloaded,
        threads,
        "verify starting"
    );
    let forks = Forks {
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
                    progress.chunk_done(verified);
                }
                Some(Ok((chunk, bytes, Err(err)))) => {
                    in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
                    let file = state.raw_path(chunk);
                    error!(%err, file = %file.display(), "chunk failed verification");
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
            _ = tick.tick() => progress.log(tasks.len()),
        }
    }
    progress.summary();
    if let Some(failure) = failure {
        eyre::bail!("verification failed: {failure}");
    }

    let (state, plan) = (state.clone(), *plan);
    tokio::task::spawn_blocking(move || accept(&state, &plan)).await?
}

/// Removes the record of an earlier run and lists what this one has to verify. Blocking.
fn todo(state: &State, plan: &Plan) -> io::Result<Todo> {
    // Whatever an earlier run accepted is not accepted again until this one ends well.
    state.clear_verified()?;
    let mut todo = Todo {
        chunks: Vec::new(),
        blocks: 0,
        already_verified: 0,
        not_downloaded: 0,
    };
    for chunk in plan.chunks() {
        if state.verified_path(chunk).exists() {
            todo.already_verified = todo.already_verified.saturating_add(1);
            continue;
        }
        match state.raw_path(chunk).metadata() {
            Ok(file) => {
                todo.blocks = todo.blocks.saturating_add(chunk.blocks());
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
    total_blocks: u64,
    chunks: usize,
    done: Stats,
    blocks_rate: Rate,
    transactions_rate: Rate,
}

impl Progress {
    fn new(todo: &Todo) -> Self {
        Self {
            started: Instant::now(),
            total_chunks: todo.chunks.len(),
            total_blocks: todo.blocks,
            chunks: 0,
            done: Stats::default(),
            blocks_rate: Rate::new(),
            transactions_rate: Rate::new(),
        }
    }

    const fn chunk_done(&mut self, chunk: Stats) {
        self.chunks = self.chunks.saturating_add(1);
        self.done.blocks = self.done.blocks.saturating_add(chunk.blocks);
        self.done.transactions = self.done.transactions.saturating_add(chunk.transactions);
        self.done.zero_signatures = self
            .done
            .zero_signatures
            .saturating_add(chunk.zero_signatures);
        self.done.disk_bytes = self.done.disk_bytes.saturating_add(chunk.disk_bytes);
    }

    fn log(&mut self, busy_threads: usize) {
        let blocks_per_sec = self.blocks_rate.per_sec(self.done.blocks);
        info!(
            chunks = self.chunks,
            of = self.total_chunks,
            blocks = self.done.blocks,
            transactions = self.done.transactions,
            blocks_per_sec,
            transactions_per_sec = self.transactions_rate.per_sec(self.done.transactions),
            secs_left = self
                .total_blocks
                .saturating_sub(self.done.blocks)
                .checked_div(blocks_per_sec),
            busy_threads,
            disk_bytes = self.done.disk_bytes,
            "verifying"
        );
    }

    fn summary(&self) {
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            chunks = self.chunks,
            missing = self.total_chunks.saturating_sub(self.chunks),
            blocks = self.done.blocks,
            transactions = self.done.transactions,
            zero_signature_transactions = self.done.zero_signatures,
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
fn accept(state: &State, plan: &Plan) -> eyre::Result<()> {
    let linked = link_chunks(state, plan)?;
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
/// range matches the anchor. Reads two hashes per chunk. Blocking.
fn link_chunks(state: &State, plan: &Plan) -> io::Result<Linked> {
    let total = plan.chunks().count();
    info!(chunks = total, "linking the verified chunks");
    let mut linked = Linked {
        verified: 0,
        total,
        broken: None,
        last_hash: None,
    };
    let mut previous: Option<Link> = None;
    let mut logged = Instant::now();
    for (position, chunk) in plan.chunks().enumerate() {
        if logged.elapsed() >= progress::INTERVAL {
            info!(chunks = position, of = total, "linking");
            logged = Instant::now();
        }
        let link = match chunk::read_link(&state.verified_path(chunk)) {
            Ok(link) => link,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                previous = None;
                continue;
            }
            Err(err) => return Err(err),
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
                linked.broken = check_top(state, plan, chunk, link)?;
            }
        }
        previous = Some(link);
    }
    Ok(linked)
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
            header.timestamp,
            header.state_root,
            header.withdrawals_root,
        )
        .err()
        .map(|err| err.to_string()))
}
