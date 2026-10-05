//! `load`: checks the senders of the verified range and appends it to the block archive the
//! node serves from, its committed store.
//!
//! ```text
//! <state>/verified/<chunk>.blk ─▶ senders recovered and checked ─▶ FjallArchive::bulk_append
//! ```
//!
//! **Senders.** The sender of every signed transaction is recovered from its signature and
//! must equal the one in the verified chunk (the service's); a deposit's must equal the `from`
//! its encoding carries, which the block hash covers. A legacy transaction signed with all
//! zeros has no signer: its recorded sender (the zero address) is kept, unproven, and counted.
//! The first difference stops `load` before its block is appended. The recovery runs in the
//! threads that prepare the blocks, next to the writes; it is most of the load's CPU (about
//! 37 µs per transaction with decoding, on one core of an M1 Pro, with libsecp256k1).
//!
//! Nothing but the archive is touched. Only a range `verify` accepted whole is loaded.
//!
//! What the archive holds is asked of the archive, never recorded beside it: `load` starts
//! after the archive's last block, so a stopped run or a new archive directory cannot make it
//! skip blocks. Before it writes, it checks that the archive starts at the first block of the
//! range and that its last block is the verified one at that height; an archive of another
//! range or chain is refused. (Earlier builds kept marker files in `<state>/loaded/`; they are
//! not read.)
//!
//! The archive is written in block order by bulk appends (`FjallArchive::bulk_append`):
//! chunks are read, decompressed and their blocks prepared (hash checked, values compressed)
//! on blocking threads, one chunk per core, and collected into appends of about a gigabyte,
//! one written while the next is prepared. Each append writes new files and syncs them, so a
//! stop or a crash leaves the archive holding a contiguous prefix of the range.
//!
//! `load` succeeds only if it reaches the end of the range: stopping on a signal is reported
//! as a failure that says what to run next.
//!
//! The archive gets the bytes `verify` checked, unchanged, with the senders `load` checked;
//! nothing is encoded again. Does not download or verify anything, and trusts a verified
//! chunk's file.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Instant;

use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::B256;
use clap::Args;
use eyre::{WrapErr, ensure, eyre};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_primitives::{
    ChainIdentity, EncodedBlock, decode_transaction, is_zero_signature, split_body,
};
use op_indexer_storage::archive_store::{FjallArchive, PreparedBlock};
use op_indexer_storage::{ArchiveStore, StorageError};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::chunk::{self, VerifiedBlock};
use crate::progress::{self, Rate};
use crate::state::{Chunk, MIN_SPACE_BYTES, Plan, State, free_bytes};

/// Fewest and most chunks read, decompressed and prepared at once ahead of the archive's
/// writer: one per core within these bounds. Preparing (recovering the senders, hashing the
/// header, compressing the values) is the CPU of a load, mostly the recovery; a chunk after
/// Bedrock is a few megabytes in memory.
const READ_AHEAD: (usize, usize) = (4, 32);
/// Bytes of block encodings collected before they are appended to the archive in one bulk
/// append. Each writes new table and blob files and syncs them, so it must be large; one is
/// written while the next is prepared, and a prepared one takes about half this in memory.
const APPEND_BYTES: u64 = 1024 * 1024 * 1024;
/// Settings of `load`: it checks the senders and fills the block archive the node serves from.
#[derive(Debug, Clone, Args)]
pub(crate) struct LoadArgs {
    /// Directory of the indexer's block archive: `archive` inside its data directory
    /// (default `data-<chain>/archive`, the node's default for the plan's chain: `data-op` or
    /// `data-unichain`). The indexer must not be running on it. It must be empty or hold the
    /// start of this range.
    #[arg(long, env = "OP_INDEXER_IMPORT_ARCHIVE_DIR")]
    pub(crate) archive_dir: Option<PathBuf>,
}

/// The node's default archive for the plan's chain, `data-<chain>/archive`. Earlier builds of
/// the node defaulted to `data/archive`: while that exists and the new one does not, loading
/// into a new archive the node would not find by itself is refused. Blocking.
fn default_archive_dir(plan: &Plan) -> eyre::Result<PathBuf> {
    let dir = PathBuf::from(plan.chain.default_data_dir()).join("archive");
    let old = Path::new("data/archive");
    eyre::ensure!(
        dir.try_exists()? || !old.try_exists()?,
        "{} holds an archive from an earlier build, whose default data directory was `data`: \
         move data to {} (or give --archive-dir {})",
        old.display(),
        plan.chain.default_data_dir(),
        old.display()
    );
    Ok(dir)
}

/// Loads the verified range of `plan` into the archive, to the end of the range.
///
/// # Errors
///
/// Returns an error if `verify` has not accepted the range, or `cancel` fires before the end
/// of the range. Also if the archive cannot be opened, is open in another process, does not
/// start at the first block of the range or holds another chain; if a chunk's file cannot be
/// read or does not hold its blocks; and if the archive refuses or fails a write.
pub(crate) async fn run(
    args: &LoadArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    // Stopped before it began, for example during the step before it in `run`.
    ensure!(!cancel.is_cancelled(), "stopped before `load` began");
    // Only a range `verify` accepted whole (every chunk, every link, the anchor) is loaded.
    let accepted = state
        .read_verified()?
        .filter(|range| range.covers(plan))
        .ok_or_else(|| {
            eyre!(
                "blocks {} to {} are not verified: run `verify`, which must accept the whole range",
                plan.first,
                plan.last
            )
        })?;
    // Startup-only blocking I/O, before any chunk is read.
    let identity = ChainIdentity {
        chain_id: plan.chain.chain_id,
        genesis_hash: plan.chain.genesis_hash,
    };
    let archive_dir = match &args.archive_dir {
        Some(dir) => dir.clone(),
        None => default_archive_dir(plan)?,
    };
    let archive = FjallArchive::open(&archive_dir, identity).map_err(|err| {
        if err.is_archive_locked() {
            eyre!(
                "the block archive in {} is open in another process: stop the indexer, or the \
                 other import, that is using it and run `load` again",
                archive_dir.display()
            )
        } else {
            eyre::Report::new(err).wrap_err("failed to open the block archive")
        }
    })?;

    let held_to = archive_tip(&archive, &archive_dir, state, plan).await?;
    let stopped_at = fill_archive(&archive, &archive_dir, held_to, state, plan, cancel).await?;
    if stopped_at.is_none() {
        check_top(&archive, &archive_dir, plan, accepted.last_hash).await?;
    }
    match stopped_at {
        None => Ok(()),
        Some(block) => Err(eyre!(
            "load was stopped at block {block}, before the end of the range ({}): run `load` \
             again to continue",
            plan.last
        )),
    }
}

/// Appends the range to the archive from the block after `held_to` on. Returns the first
/// block not appended if `cancel` stopped it, `None` when the archive holds the whole range.
///
/// Chunks are read and prepared for the archive ([`PreparedBlock`]) on blocking threads, one
/// per core, and collected into bulk appends of about [`APPEND_BYTES`]; one append is written
/// while the next is collected. A failed append is not retried: it can leave files the next
/// open of the archive removes, and running `load` again resumes after the archive's tip.
async fn fill_archive(
    archive: &FjallArchive,
    archive_dir: &Path,
    held_to: Option<u64>,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<Option<u64>> {
    let next = held_to.map_or(plan.first, |held_to| held_to.saturating_add(1));
    let to_append = plan.last.saturating_add(1).saturating_sub(next);
    // Chunks that end at or below the archive's last block are held whole. Of one that
    // straddles it, only the blocks above are taken.
    let to_load: Vec<Chunk> = plan.chunks().skip_while(|chunk| chunk.to <= next).collect();
    let sizes = file_sizes(state, &to_load).await?;
    let file_bytes = sizes.iter().sum();
    info!(
        archive_holds_to = held_to,
        first = plan.first,
        last = plan.last,
        blocks_to_append = to_append,
        chunk_bytes = file_bytes,
        "loading the block archive"
    );
    // The archive takes more than the verified chunks it is loaded from (its values are
    // compressed with snappy, not zstd): a disk with less free is refused before anything is
    // written. A whole chain is hundreds of gigabytes to terabytes (Base: 2 to 3.5 TB).
    let free = archive_free_bytes(archive_dir).await?;
    ensure!(
        free.is_none_or(|free| free >= file_bytes),
        "the archive will not fit: {file_bytes} bytes of verified chunks to load, which the \
         archive takes more than, and {} bytes free on the disk of {}",
        free.unwrap_or_default(),
        archive_dir.display()
    );
    let mut progress = Progress::new(to_append, file_bytes);
    let read_ahead = std::thread::available_parallelism()
        .map_or(READ_AHEAD.0, std::num::NonZero::get)
        .clamp(READ_AHEAD.0, READ_AHEAD.1);
    let mut chunks = to_load.into_iter().zip(sizes);
    let mut reads: VecDeque<JoinHandle<eyre::Result<Collected>>> = VecDeque::new();
    let mut pending = Collected::default();
    // The bulk append being written, with what it holds.
    let mut writing: Option<(JoinHandle<Result<(), StorageError>>, Amount)> = None;
    let mut stopped = false;
    loop {
        // Once stopped, nothing more is read; what is being read is still appended.
        stopped = stopped || cancel.is_cancelled();
        while !stopped
            && reads.len() < read_ahead
            && let Some((chunk, file_bytes)) = chunks.next()
        {
            let path = state.verified_path(chunk);
            reads.push_back(spawn_blocking(move || {
                prepare(&path, chunk, next, file_bytes)
            }));
        }
        if let Some(read) = reads.pop_front() {
            pending.extend(read.await.wrap_err("reading a chunk panicked")??);
        }
        if pending.amount.rlp_bytes < APPEND_BYTES && !reads.is_empty() {
            continue;
        }
        // The previous append must be in before the next is written: each extends the last.
        if let Some((write, written)) = writing.take() {
            write
                .await
                .wrap_err("an archive append panicked")?
                .wrap_err("archive bulk_append")?;
            progress.appended(written);
        }
        // Nothing more is appended on a disk nearly full (the append before is in); a later
        // run resumes after the archive's last block.
        let free = archive_free_bytes(archive_dir).await?;
        ensure!(
            free.is_none_or(|free| free >= MIN_SPACE_BYTES),
            "less than {} GiB free on the disk of {}: free some and run `load` again, which \
             resumes after the archive's last block",
            MIN_SPACE_BYTES >> 30,
            archive_dir.display()
        );
        if !pending.blocks.is_empty() {
            let Collected { blocks, amount } = std::mem::take(&mut pending);
            let archive = archive.clone();
            let write = tokio::spawn(async move { archive.bulk_append(blocks).await });
            writing = Some((write, amount));
        }
        // With nothing more to collect, the next turn waits for the last append.
        if reads.is_empty() && writing.is_none() {
            break;
        }
    }
    let range = archive.range().await;
    let range = range.wrap_err("failed to read the block archive")?;
    progress.summary(range.map(|(first, last)| (first.number, last.number)));
    let appended_to = next.saturating_add(progress.done.blocks);
    Ok((appended_to <= plan.last).then_some(appended_to))
}

/// Free space on the disk of the archive at `dir`, where the system tells.
async fn archive_free_bytes(dir: &Path) -> eyre::Result<Option<u64>> {
    let dir = dir.to_owned();
    let free = spawn_blocking(move || free_bytes(&dir)).await?;
    free.wrap_err("failed to read the free disk space")
}

/// How much was read or appended.
#[derive(Debug, Default, Clone, Copy)]
struct Amount {
    blocks: u64,
    /// Bytes of the blocks' encodings (RLP), as verified.
    rlp_bytes: u64,
    /// Bytes of the verified chunk files they were read from.
    file_bytes: u64,
    /// Transactions whose sender was recovered from the signature and matched.
    recovered: u64,
    /// Legacy transactions signed with all zeros, whose recorded sender is kept unproven.
    zero_signatures: u64,
}

impl Amount {
    fn add(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.rlp_bytes = self.rlp_bytes.saturating_add(other.rlp_bytes);
        self.file_bytes = self.file_bytes.saturating_add(other.file_bytes);
        self.recovered = self.recovered.saturating_add(other.recovered);
        self.zero_signatures = self.zero_signatures.saturating_add(other.zero_signatures);
    }
}

/// Blocks prepared for the archive and not appended yet.
#[derive(Default)]
struct Collected {
    blocks: Vec<PreparedBlock>,
    amount: Amount,
}

impl Collected {
    fn extend(&mut self, other: Self) {
        self.blocks.extend(other.blocks);
        self.amount.add(other.amount);
    }
}

/// Reads the verified chunk at `path`, `file_bytes` long, checks the senders of its blocks
/// from `next` on ([`check_senders`]) and prepares those blocks for the archive. Blocking:
/// decompression, one signature recovery per transaction, a hash and compression per block.
fn prepare(path: &Path, chunk: Chunk, next: u64, file_bytes: u64) -> eyre::Result<Collected> {
    // `next` is the same for every chunk of the run: it falls inside the first one only.
    let first = next.max(chunk.from);
    let held = usize::try_from(first.saturating_sub(chunk.from))
        .wrap_err("a chunk has too many blocks")?;
    let mut collected = Collected {
        amount: Amount {
            file_bytes,
            ..Amount::default()
        },
        ..Collected::default()
    };
    for (number, block) in (first..).zip(read(path, chunk)?.into_iter().skip(held)) {
        // Checks one sender per transaction, which `check_senders` relies on.
        let prepared = PreparedBlock::new(&block)
            .wrap_err_with(|| format!("{} does not hold archivable blocks", path.display()))?;
        let senders = check_senders(number, &block)?;
        collected.amount.add(Amount {
            blocks: 1,
            rlp_bytes: u64::try_from(size(&block.encoded)).unwrap_or(u64::MAX),
            ..senders
        });
        collected.blocks.push(prepared);
    }
    Ok(collected)
}

/// Checks the sender recorded for every transaction of `block` (number `number`, already
/// checked to have one sender per transaction) in its verified chunk, which is the one the
/// archive service reported:
///
/// - a signed transaction: the sender is recovered from its signature and must equal it;
/// - a deposit: it must equal the `from` in the deposit's encoding, which the block hash
///   covers;
/// - a legacy transaction signed with all zeros (an L1-to-L2 message before Bedrock) has no
///   signer: the recorded sender is kept and counted, unproven.
///
/// Returns what was checked, as an [`Amount`] with only the sender counts set.
///
/// # Errors
///
/// Returns an error naming the block, the transaction's index and both addresses for the
/// first sender that differs, or if a transaction does not decode or has no recoverable
/// sender.
fn check_senders(number: u64, block: &VerifiedBlock) -> eyre::Result<Amount> {
    let body = split_body(&block.encoded.body)
        .ok_or_else(|| eyre!("block {number}: the verified body does not decode"))?;
    let mut checked = Amount::default();
    for (index, (leaf, recorded)) in body.transactions.iter().zip(&block.senders).enumerate() {
        let transaction = decode_transaction(leaf)
            .map_err(|err| eyre!("block {number}: transaction {index} does not decode: {err}"))?;
        if is_zero_signature(&transaction) {
            checked.zero_signatures = checked.zero_signatures.saturating_add(1);
            continue;
        }
        if !matches!(transaction, OpTxEnvelope::Deposit(_)) {
            checked.recovered = checked.recovered.saturating_add(1);
        }
        // A deposit's sender is the `from` in its encoding.
        let proven = transaction.recover_signer().map_err(|err| {
            eyre!("block {number}: transaction {index} has no recoverable sender: {err}")
        })?;
        ensure!(
            proven == *recorded,
            "block {number}: transaction {index} was sent by {proven}, but its verified chunk \
             records {recorded}: the archive service reported a wrong sender. Nothing from this \
             block on is loaded"
        );
    }
    Ok(checked)
}

/// Bytes of the verified file of each of `chunks`, which progress is measured in.
async fn file_sizes(state: &State, chunks: &[Chunk]) -> eyre::Result<Vec<u64>> {
    let paths: Vec<PathBuf> = chunks
        .iter()
        .map(|chunk| state.verified_path(*chunk))
        .collect();
    spawn_blocking(move || {
        paths
            .iter()
            .map(|path| {
                Ok(std::fs::metadata(path)
                    .wrap_err_with(|| format!("failed to read verified chunk {}", path.display()))?
                    .len())
            })
            .collect()
    })
    .await
    .wrap_err("measuring the chunks panicked")?
}

/// What the archive pass has done, for its progress lines. The time left is reckoned from
/// the verified files' bytes, not from blocks: a block after Bedrock is tens of times larger
/// than one before.
struct Progress {
    started: Instant,
    logged: Instant,
    rate: Rate,
    /// Blocks this run has to append.
    total_blocks: u64,
    /// Bytes of the verified files this run reads.
    total_file_bytes: u64,
    /// What is in the archive.
    done: Amount,
}

impl Progress {
    fn new(total_blocks: u64, total_file_bytes: u64) -> Self {
        let now = Instant::now();
        Self {
            started: now,
            logged: now,
            rate: Rate::new(),
            total_blocks,
            total_file_bytes,
            done: Amount::default(),
        }
    }

    /// Records an append, and logs a progress line when one is due.
    fn appended(&mut self, appended: Amount) {
        self.done.add(appended);
        if self.logged.elapsed() < progress::INTERVAL {
            return;
        }
        self.logged = Instant::now();
        let file_bytes_per_sec = self.rate.per_sec(self.done.file_bytes);
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            blocks = self.done.blocks,
            of = self.total_blocks,
            blocks_per_sec = self.done.blocks / secs,
            mb_per_sec = self.done.rlp_bytes / secs / 1_000_000,
            secs_left = self
                .total_file_bytes
                .saturating_sub(self.done.file_bytes)
                .checked_div(file_bytes_per_sec),
            bytes = self.done.rlp_bytes,
            "loading"
        );
    }

    fn summary(&self, archive: Option<(u64, u64)>) {
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            blocks = self.done.blocks,
            of = self.total_blocks,
            bytes = self.done.rlp_bytes,
            blocks_per_sec = self.done.blocks / secs,
            mb_per_sec = self.done.rlp_bytes / secs / 1_000_000,
            secs,
            archive_first = archive.map(|(first, _)| first),
            archive_last = archive.map(|(_, last)| last),
            senders_recovered = self.done.recovered,
            zero_signature_transactions = self.done.zero_signatures,
            "{}",
            if self.done.blocks == self.total_blocks {
                "load finished: the range is in the block archive"
            } else {
                "load stopped before the end of the range"
            }
        );
    }
}

/// Bytes of a block's three encodings.
fn size(block: &EncodedBlock) -> usize {
    let receipts = block.receipts.as_ref().map_or(0, |receipts| receipts.len());
    block
        .header
        .len()
        .saturating_add(block.body.len())
        .saturating_add(receipts)
}

/// Returns the archive's last block, after checking that the archive is the one this range
/// is loaded into: it starts at the first block of the range, and its last block (or, if it
/// reaches past the range, the last block of the range) is the verified one at that height.
/// `None` for an empty archive.
async fn archive_tip(
    archive: &FjallArchive,
    directory: &Path,
    state: &State,
    plan: &Plan,
) -> eyre::Result<Option<u64>> {
    let range = archive.range().await;
    let Some((first, tip)) = range.wrap_err("failed to read the block archive")? else {
        return Ok(None);
    };
    ensure!(
        first.number == plan.first,
        "the block archive in {} starts at block {}, but the range starts at block {}: use the \
         archive this range is loaded into, or an empty directory",
        directory.display(),
        first.number,
        plan.first
    );
    // The block both must agree on: the archive's tip, or the top of the range below it.
    let number = tip.number.min(plan.last);
    let chunk = plan
        .chunks()
        .find(|chunk| (chunk.from..chunk.to).contains(&number))
        .ok_or_else(|| eyre!("block {number} is outside the range"))?;
    let path = state.verified_path(chunk);
    let position = usize::try_from(number.saturating_sub(chunk.from))
        .wrap_err("a chunk has too many blocks")?;
    let hash = spawn_blocking(move || read(&path, chunk))
        .await
        .wrap_err("reading a chunk panicked")??
        .get(position)
        .map(|block| block.encoded.hash)
        .ok_or_else(|| eyre!("block {number} is missing from its verified chunk"))?;
    let held = if number == tip.number {
        tip.hash == hash
    } else {
        // The archive reaches past the range: look the range's last block up in it.
        let held = archive.number_of(hash).await;
        held.wrap_err("failed to read the block archive")? == Some(number)
    };
    ensure!(
        held,
        "the block archive in {} holds another chain: its block {number} is not the verified \
         block {hash}. Use the archive this range is loaded into, or an empty directory",
        directory.display()
    );
    Ok(Some(tip.number))
}

/// Checks that the archive's block at the top of the range is the one `verify` accepted
/// (`verified.json`), which binds what was loaded to what was verified.
async fn check_top(
    archive: &FjallArchive,
    directory: &Path,
    plan: &Plan,
    accepted: B256,
) -> eyre::Result<()> {
    let range = archive.range().await;
    let tip = range.wrap_err("failed to read the block archive")?;
    let held = match tip {
        Some((_, tip)) if tip.number == plan.last => tip.hash == accepted,
        // The archive reaches past the range: look the accepted block up in it.
        Some(_) => {
            let number = archive.number_of(accepted).await;
            number.wrap_err("failed to read the block archive")? == Some(plan.last)
        }
        None => false,
    };
    ensure!(
        held,
        "the block archive in {} does not hold the block {accepted} that `verify` accepted at \
         the top of the range ({}): the archive or the verified chunks have changed since \
         `verify`. Load into an empty directory after running `verify` again",
        directory.display(),
        plan.last
    );
    Ok(())
}

/// Reads the verified chunk at `path`: the blocks of `chunk`, in block order. Blocking.
fn read(path: &Path, chunk: Chunk) -> eyre::Result<Vec<VerifiedBlock>> {
    // Its errors name the file.
    let (_link, blocks) = chunk::read(path)?;
    ensure!(
        u64::try_from(blocks.len()).ok() == Some(chunk.blocks()),
        "{} holds {} blocks, not the {} of its chunk",
        path.display(),
        blocks.len(),
        chunk.blocks()
    );
    Ok(blocks)
}
