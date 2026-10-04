//! `load`: appends the verified range to the local block archive the node serves from, and,
//! only when asked, writes it to ClickHouse too.
//!
//! ```text
//! <state>/verified/<chunk>.blk ─▶ the verified bytes ─▶ FjallArchive::bulk_append (fjall)
//!            with --clickhouse-url ─▶ typed blocks ─▶ CommittedStore::insert (ClickHouse)
//!                                 ─▶ <state>/loaded/<chunk>.clickhouse
//! ```
//!
//! By default nothing but the archive is touched: no database is needed, contacted or
//! migrated. Only a range `verify` accepted whole is loaded.
//!
//! What the archive holds is asked of the archive, never recorded beside it: `load` starts
//! after the archive's last block, so a stopped run or a new archive directory cannot make it
//! skip blocks. Before it writes, it checks that the archive starts at the first block of the
//! range and that its last block is the verified one at that height; an archive of another
//! range or chain is refused. ClickHouse has no such range to ask, so it keeps one marker per
//! chunk, written once it holds the chunk; it can be loaded on a later run.
//!
//! The archive is written in block order by bulk appends (`FjallArchive::bulk_append`):
//! chunks are read, decompressed and their blocks prepared (hash checked, values compressed)
//! on blocking threads, one chunk per core, and collected into appends of about a gigabyte,
//! one written while the next is prepared. Each append writes new files and syncs them, so a
//! stop or a crash leaves the archive holding a contiguous prefix of the range.
//!
//! `load` succeeds only if it reaches the end of the range: stopping on a signal is reported
//! as a failure that says what to run next. Both writes are idempotent, so repeating a chunk
//! is harmless.
//!
//! The archive gets the bytes `verify` checked, unchanged. The typed blocks for ClickHouse
//! are decoded from those same bytes; nothing is encoded again. Does not download or verify
//! anything, and trusts a verified chunk's file.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Args;
use eyre::{WrapErr, ensure, eyre};
use op_indexer_primitives::{BlockSource, DecodedBlock, EncodedBlock, decode_block};
use op_indexer_storage::archive_store::{FjallArchive, PreparedBlock};
use op_indexer_storage::committed_store::{BulkRows, ClickHouseStore};
use op_indexer_storage::{
    ArchiveStore, ClickHouseConfig, RetryError, Severity, StorageError, Store,
};
use tokio::task::{JoinHandle, JoinSet, spawn_blocking};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::chunk::{self, VerifiedBlock};
use crate::cli::Secret;
use crate::progress::{self, Rate};
use crate::state::{Chunk, Plan, State, write_atomic};

/// Fewest and most chunks read, decompressed and prepared at once ahead of the archive's
/// writer: one per core within these bounds. Preparing (hashing the header, compressing the
/// values) is most of the CPU of a load; a chunk after Bedrock is a few megabytes in memory.
const READ_AHEAD: (usize, usize) = (4, 32);
/// Bytes of block encodings collected before they are appended to the archive in one bulk
/// append. Each writes new table and blob files and syncs them, so it must be large; one is
/// written while the next is prepared, and a prepared one takes about half this in memory.
const APPEND_BYTES: usize = 1024 * 1024 * 1024;
/// Rows (of every table together) per bulk insert into ClickHouse: large enough that the
/// server writes few, large parts and a round trip is small next to the data, small enough
/// that a batch takes a few hundred megabytes in memory at most.
const BULK_ROWS: usize = 500_000;
/// Batches written at once by default: each waits on the network and the server, not on this
/// machine, so a few overlap well; a remote service benefits most.
const DEFAULT_INSERTS: u64 = 4;
/// How long a store call is retried before `load` gives up. A store that is down for longer
/// needs an operator; `load` continues where it stopped when it is run again.
const RETRY_BUDGET: Duration = Duration::from_mins(5);

/// Settings of `load`. By default it fills the local block archive the node serves from and
/// needs no database; ClickHouse is loaded only when `--clickhouse-url` is given.
#[derive(Debug, Clone, Args)]
pub(crate) struct LoadArgs {
    /// Directory of the indexer's block archive: `archive` inside its data directory. The
    /// indexer must not be running on it. It must be empty or hold the start of this range,
    /// and the indexer must then keep every block (`OP_INDEXER_ARCHIVE_RETENTION_BLOCKS=all`).
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_ARCHIVE_DIR",
        default_value = "data/archive"
    )]
    pub(crate) archive_dir: PathBuf,
    /// Also write the blocks to ClickHouse, at this HTTP interface (for example
    /// `http://127.0.0.1:8123`). Optional: without it no database is contacted. Its
    /// migrations are applied if missing. Can be given on a later run: the archive is then
    /// left as it is and only ClickHouse is written.
    #[arg(long, env = "OP_INDEXER_IMPORT_CLICKHOUSE_URL")]
    pub(crate) clickhouse_url: Option<String>,
    /// ClickHouse database. Only used with `--clickhouse-url`.
    #[arg(
        long,
        env = "OP_INDEXER_CLICKHOUSE_DATABASE",
        default_value = "op_indexer"
    )]
    pub(crate) clickhouse_database: String,
    /// ClickHouse user. Only used with `--clickhouse-url`.
    #[arg(long, env = "OP_INDEXER_CLICKHOUSE_USER", default_value = "indexer")]
    pub(crate) clickhouse_user: String,
    /// ClickHouse password. Only used with `--clickhouse-url`. A flag is visible in the
    /// process list; the environment variable is not. It is never logged.
    #[arg(long, env = "OP_INDEXER_CLICKHOUSE_PASSWORD", hide_env_values = true)]
    pub(crate) clickhouse_password: Option<Secret>,
    /// ClickHouse inserts in flight at once, each a batch of about half a million rows on its
    /// own connection. Only used with `--clickhouse-url`. More helps a remote service (4 to 8
    /// for ClickHouse Cloud); a local server is busy with 2.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_CLICKHOUSE_INSERTS",
        default_value_t = DEFAULT_INSERTS,
        value_parser = clap::value_parser!(u64).range(1..=64)
    )]
    pub(crate) clickhouse_inserts: u64,
}

/// Loads the verified range of `plan` into the archive, then into ClickHouse when it is asked
/// for, to the end of the range.
///
/// # Errors
///
/// Returns an error if `verify` has not accepted the range, or `cancel` fires before the end
/// of the range. Also if the archive cannot be opened, is open in another process, does not
/// start at the first block of the range or holds another chain; if a chunk's file cannot be
/// read or does not hold its blocks; if a store keeps failing; and, when ClickHouse is asked
/// for, if it cannot be reached or migrated or refuses a chunk.
pub(crate) async fn run(
    args: &LoadArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    // Stopped before it began, for example during the step before it in `run`.
    ensure!(!cancel.is_cancelled(), "stopped before `load` began");
    // Only a range `verify` accepted whole (every chunk, every link, the anchor) is loaded.
    let accepted = state.read_verified()?;
    ensure!(
        accepted.is_some_and(|range| range.covers(plan)),
        "blocks {} to {} are not verified: run `verify`, which must accept the whole range",
        plan.first,
        plan.last
    );
    let committed = match &args.clickhouse_url {
        Some(url) => Some(clickhouse(args, url, plan).await?),
        None => None,
    };
    // Startup-only blocking I/O, before any chunk is read.
    let archive = FjallArchive::open(&args.archive_dir).map_err(|err| {
        if err.is_archive_locked() {
            eyre!(
                "the block archive in {} is open in another process: stop the indexer, or the \
                 other import, that is using it and run `load` again",
                args.archive_dir.display()
            )
        } else {
            eyre::Report::new(err).wrap_err("failed to open the block archive")
        }
    })?;

    let held_to = archive_tip(&archive, &args.archive_dir, state, plan).await?;
    let stopped_at = fill_archive(&archive, held_to, state, plan, cancel).await?;
    let stopped_at = match (stopped_at, &committed) {
        (None, Some(committed)) => {
            let inserts = usize::try_from(args.clickhouse_inserts).unwrap_or(1);
            fill_clickhouse(committed, state, plan, inserts, cancel).await?
        }
        (stopped_at, _) => stopped_at,
    };
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
        if pending.amount.rlp_bytes < APPEND_BYTES as u64 && !reads.is_empty() {
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

/// How much was read or appended.
#[derive(Debug, Default, Clone, Copy)]
struct Amount {
    blocks: u64,
    /// Bytes of the blocks' encodings (RLP), as verified.
    rlp_bytes: u64,
    /// Bytes of the verified chunk files they were read from.
    file_bytes: u64,
}

impl Amount {
    fn add(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.rlp_bytes = self.rlp_bytes.saturating_add(other.rlp_bytes);
        self.file_bytes = self.file_bytes.saturating_add(other.file_bytes);
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

/// Reads the verified chunk at `path`, `file_bytes` long, and prepares its blocks from
/// `next` on for the archive. Blocking: decompression, a hash and compression per block.
fn prepare(path: &Path, chunk: Chunk, next: u64, file_bytes: u64) -> eyre::Result<Collected> {
    let held =
        usize::try_from(next.saturating_sub(chunk.from)).wrap_err("a chunk has too many blocks")?;
    let mut collected = Collected {
        amount: Amount {
            file_bytes,
            ..Amount::default()
        },
        ..Collected::default()
    };
    for block in read(path, chunk)?.into_iter().skip(held) {
        let rlp_bytes = u64::try_from(size(&block.encoded)).unwrap_or(u64::MAX);
        collected.amount.add(Amount {
            blocks: 1,
            rlp_bytes,
            file_bytes: 0,
        });
        let block = PreparedBlock::new(&block.encoded)
            .wrap_err_with(|| format!("{} does not hold archivable blocks", path.display()))?;
        collected.blocks.push(block);
    }
    Ok(collected)
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

/// Writes every chunk of the range that has no ClickHouse marker yet, and marks it. Returns
/// the first block of the earliest chunk not written when `cancel` stopped it, `None` when
/// ClickHouse holds the whole range.
///
/// Chunks are read and turned into rows on blocking threads, joined into batches of about
/// [`BULK_ROWS`] rows, and written by up to `inserts` batches at once, each in one synchronous
/// insert per table (`ClickHouseStore::bulk_insert`, child tables before `blocks`). A chunk's
/// marker is written only once its batch is in every table, so a stop leaves no marker for a
/// chunk ClickHouse does not fully hold; a chunk written twice is harmless, the tables keep one
/// row per position. Memory is bounded by the batches in flight and the chunks being read.
async fn fill_clickhouse(
    committed: &ClickHouseStore,
    state: &State,
    plan: &Plan,
    inserts: usize,
    cancel: &CancellationToken,
) -> eyre::Result<Option<u64>> {
    let mut queue = unloaded_chunks(state, plan).await?.into_iter();
    let readers = std::thread::available_parallelism().map_or(1, usize::from);
    info!(
        first = plan.first,
        last = plan.last,
        chunks = queue.len(),
        inserts,
        readers,
        "loading ClickHouse"
    );
    let mut tally = Tally::new();
    let mut reading: JoinSet<eyre::Result<(Chunk, BulkRows)>> = JoinSet::new();
    let mut writing: JoinSet<eyre::Result<Result<Batch, u64>>> = JoinSet::new();
    let mut batch = Batch::default();
    // The first block of a chunk whose insert was abandoned on a stop.
    let mut abandoned: Option<u64> = None;
    loop {
        let stopping = cancel.is_cancelled();
        // Read ahead until the batch being built is full: the batches in flight, that one and
        // the chunks being read are what the pass holds in memory.
        while !stopping
            && reading.len() < readers
            && batch.rows.rows() < BULK_ROWS
            && let Some(chunk) = queue.next()
        {
            let (committed, path) = (committed.clone(), state.verified_path(chunk));
            reading.spawn_blocking(move || {
                let blocks = read(&path, chunk)?
                    .into_iter()
                    .map(decode)
                    .collect::<eyre::Result<Vec<DecodedBlock>>>()
                    .wrap_err_with(|| format!("{} does not decode", path.display()))?;
                Ok((chunk, committed.bulk_rows(&blocks)?))
            });
        }
        let due = batch.rows.rows() >= BULK_ROWS || reading.is_empty() || stopping;
        if due && !batch.chunks.is_empty() && writing.len() < inserts {
            let (batch, committed, cancel) = (
                std::mem::take(&mut batch),
                committed.clone(),
                cancel.clone(),
            );
            writing.spawn(async move {
                let insert = retry(&cancel, Store::Committed, "ClickHouse insert", || {
                    committed.bulk_insert(&batch.rows)
                });
                // On a stop, the first block of the batch, whose chunks stay unmarked.
                let first = batch.chunks.first().map_or(u64::MAX, |chunk| chunk.from);
                Ok(insert.await?.map(|()| batch).ok_or(first))
            });
            continue;
        }
        if reading.is_empty() && writing.is_empty() {
            break;
        }
        tokio::select! {
            Some(read) = reading.join_next() => {
                let (chunk, rows) = read.wrap_err("reading a chunk panicked")??;
                batch.chunks.push(chunk);
                batch.rows.append(rows);
            }
            Some(written) = writing.join_next() => {
                match written.wrap_err("an insert panicked")?? {
                    Ok(written) => {
                        mark_loaded(state, &written.chunks).await?;
                        tally.add(&written.rows, writing.len());
                    }
                    // Cancelled while retrying: those chunks stay unmarked.
                    Err(first) => abandoned = Some(abandoned.map_or(first, |a| a.min(first))),
                }
            }
        }
    }
    tally.summary();
    // A stop leaves unmarked chunks, which the next run writes; nothing is skipped. The run
    // is complete only if no chunk is left unread and no insert was abandoned.
    let unread = queue.as_slice().first().map(|chunk| chunk.from);
    Ok(match (unread, abandoned) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (first, None) | (None, first) => first,
    })
}

/// Chunks whose rows are being joined into one bulk insert, and those rows.
#[derive(Debug, Default)]
struct Batch {
    chunks: Vec<Chunk>,
    rows: BulkRows,
}

/// What the ClickHouse pass has written, for its progress lines.
struct Tally {
    started: Instant,
    logged: Instant,
    rate: Rate,
    blocks: u64,
    transactions: u64,
}

impl Tally {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            logged: Instant::now(),
            rate: Rate::new(),
            blocks: 0,
            transactions: 0,
        }
    }

    /// Records a written batch and logs a progress line when one is due.
    fn add(&mut self, rows: &BulkRows, in_flight: usize) {
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        self.blocks = self.blocks.saturating_add(count(rows.blocks()));
        self.transactions = self.transactions.saturating_add(count(rows.transactions()));
        if self.logged.elapsed() >= progress::INTERVAL {
            self.logged = Instant::now();
            info!(
                blocks = self.blocks,
                transactions = self.transactions,
                transactions_per_sec = self.rate.per_sec(self.transactions),
                in_flight,
                "loading ClickHouse"
            );
        }
    }

    fn summary(&self) {
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            blocks = self.blocks,
            transactions = self.transactions,
            transactions_per_sec = self.transactions / secs,
            secs,
            "ClickHouse pass ended"
        );
    }
}

/// The chunks of the plan that have no ClickHouse marker, in block order.
async fn unloaded_chunks(state: &State, plan: &Plan) -> eyre::Result<Vec<Chunk>> {
    let (state, plan) = (state.clone(), *plan);
    spawn_blocking(move || -> eyre::Result<Vec<Chunk>> {
        let mut todo = Vec::new();
        for chunk in plan.chunks() {
            if !exists(&state.clickhouse_loaded_path(chunk))? {
                todo.push(chunk);
            }
        }
        Ok(todo)
    })
    .await
    .wrap_err("listing the chunks panicked")?
}

/// Writes the markers that ClickHouse holds `chunks`.
async fn mark_loaded(state: &State, chunks: &[Chunk]) -> eyre::Result<()> {
    let markers: Vec<PathBuf> = chunks
        .iter()
        .map(|chunk| state.clickhouse_loaded_path(*chunk))
        .collect();
    spawn_blocking(move || {
        markers
            .iter()
            .try_for_each(|marker| write_atomic(marker, |_file| Ok(())))
    })
    .await
    .wrap_err("writing markers panicked")?
    .wrap_err("failed to mark a chunk as loaded")
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

/// Connects to ClickHouse at `url` and applies its migrations.
async fn clickhouse(args: &LoadArgs, url: &str, plan: &Plan) -> eyre::Result<ClickHouseStore> {
    let config = ClickHouseConfig {
        url: url.to_owned(),
        database: args.clickhouse_database.clone(),
        user: args.clickhouse_user.clone(),
        password: args
            .clickhouse_password
            .as_ref()
            .map(|password| password.expose().to_owned()),
    };
    let committed = ClickHouseStore::new(&config, plan.chain.chain_id);
    committed
        .ping()
        .await
        .wrap_err("failed to reach ClickHouse")?;
    committed
        .migrate()
        .await
        .wrap_err("failed to migrate ClickHouse")?;
    Ok(committed)
}

/// Reads the verified chunk at `path`: the blocks of `chunk`, in block order. Blocking.
fn read(path: &Path, chunk: Chunk) -> eyre::Result<Vec<VerifiedBlock>> {
    let (_link, blocks) = chunk::read(path)
        .wrap_err_with(|| format!("failed to read verified chunk {}", path.display()))?;
    ensure!(
        u64::try_from(blocks.len()).ok() == Some(chunk.blocks()),
        "{} holds {} blocks, not the {} of its chunk",
        path.display(),
        blocks.len(),
        chunk.blocks()
    );
    Ok(blocks)
}

/// Decodes one verified block from its consensus encoding into the typed form ClickHouse's
/// rows are built from.
fn decode(block: VerifiedBlock) -> eyre::Result<DecodedBlock> {
    let (typed, receipts) = decode_block(&block.encoded)?;
    ensure!(
        typed.body.transactions.len() == block.senders.len(),
        "block {}: {} transactions and {} senders",
        typed.header.number,
        typed.body.transactions.len(),
        block.senders.len()
    );
    Ok(DecodedBlock {
        block: typed,
        hash: block.encoded.hash,
        senders: block.senders,
        receipts,
        source: BlockSource::Import,
    })
}

fn exists(path: &Path) -> eyre::Result<bool> {
    path.try_exists()
        .wrap_err_with(|| format!("failed to look for {}", path.display()))
}

/// Runs a call to `store` through storage's retry helper, for at most [`RETRY_BUDGET`].
/// `None` if `cancel` fired while it was waiting to retry.
///
/// # Errors
///
/// Returns the first error that is not transient, and the last transient one once the call
/// has been failing for [`RETRY_BUDGET`].
async fn retry<T, F, Fut>(
    cancel: &CancellationToken,
    store: Store,
    operation: &'static str,
    call: F,
) -> eyre::Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let result = op_indexer_storage::retry(cancel, store, operation, Some(RETRY_BUDGET), call);
    match result.await {
        Ok(value) => Ok(Some(value)),
        Err(RetryError::Cancelled) => Ok(None),
        Err(RetryError::Storage(err)) if err.severity() == Severity::Transient => Err(err)
            .wrap_err_with(|| {
                format!(
                    "{operation} kept failing for {} minutes; run `load` again once the store \
                     is reachable",
                    RETRY_BUDGET.as_secs() / 60
                )
            }),
        Err(RetryError::Storage(err)) => Err(err).wrap_err(operation),
    }
}
