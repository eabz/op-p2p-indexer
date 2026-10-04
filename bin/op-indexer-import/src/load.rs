//! `load`: appends the verified range to the local block archive the node serves from, and,
//! only when asked, writes it to ClickHouse too.
//!
//! ```text
//! <state>/verified/<chunk>.blk ─▶ the verified bytes ─▶ ArchiveStore::append_batch (fjall)
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
//! The archive is written by one writer, in block order. Chunks are read and decompressed
//! ahead of it on blocking threads, and consecutive chunks go into one append until it is
//! large enough: a commit is a synced write, and one per small chunk would be most of the
//! time.
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
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::committed_store::ClickHouseStore;
use op_indexer_storage::{
    ArchiveStore, ClickHouseConfig, CommittedStore, RetryError, Severity, StorageError, Store,
};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::chunk::{self, VerifiedBlock};
use crate::cli::Secret;
use crate::progress::{self, Rate};
use crate::state::{Chunk, Plan, State, write_atomic};

/// Chunks read and decompressed ahead of the archive's writer. A chunk after Bedrock can be
/// tens of megabytes decompressed, so this is what bounds memory.
const READ_AHEAD: usize = 4;
/// Bytes of blocks collected before they are appended to the archive in one call. The archive
/// commits at most this much at once, so more would not save a synced write.
const APPEND_BYTES: usize = 16 * 1024 * 1024;
/// Blocks per insert into ClickHouse.
const INSERT_BLOCKS: usize = 1000;
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
        (None, Some(committed)) => fill_clickhouse(committed, state, plan, cancel).await?,
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
async fn fill_archive(
    archive: &FjallArchive,
    held_to: Option<u64>,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<Option<u64>> {
    let next = held_to.map_or(plan.first, |held_to| held_to.saturating_add(1));
    let to_append = plan.last.saturating_add(1).saturating_sub(next);
    info!(
        archive_holds_to = held_to,
        first = plan.first,
        last = plan.last,
        blocks_to_append = to_append,
        "loading the block archive"
    );
    let mut progress = Progress::new(to_append);
    // Chunks that end at or below the archive's last block are held whole. Of one that
    // straddles it, only the blocks above are taken.
    let mut chunks = plan.chunks().skip_while(|chunk| chunk.to <= next);
    let mut reads: VecDeque<JoinHandle<eyre::Result<Vec<EncodedBlock>>>> = VecDeque::new();
    let mut pending: Vec<EncodedBlock> = Vec::new();
    let (mut pending_bytes, mut appended_to) = (0_usize, next);
    let mut stopped = false;
    loop {
        while reads.len() < READ_AHEAD
            && let Some(chunk) = chunks.next()
        {
            let path = state.verified_path(chunk);
            reads.push_back(spawn_blocking(move || {
                let held = usize::try_from(next.saturating_sub(chunk.from))
                    .wrap_err("a chunk has too many blocks")?;
                Ok(read(&path, chunk)?
                    .into_iter()
                    .skip(held)
                    .map(|block| block.encoded)
                    .collect())
            }));
        }
        let read = reads.pop_front();
        if let Some(read) = read {
            let blocks = read.await.wrap_err("reading a chunk panicked")??;
            pending_bytes = pending_bytes.saturating_add(blocks.iter().map(size).sum());
            pending.extend(blocks);
        }
        // Everything read is appended before stopping, so no read is wasted.
        stopped = stopped || cancel.is_cancelled();
        if pending_bytes < APPEND_BYTES && !reads.is_empty() && !stopped {
            continue;
        }
        if !pending.is_empty() {
            let (blocks, bytes) = (pending.len(), pending_bytes);
            // The clone is of reference-counted buffers; the bytes are not copied.
            let append = retry(cancel, Store::Archive, "archive append_batch", || {
                archive.append_batch(pending.clone())
            });
            if append.await?.is_none() {
                return Ok(Some(appended_to));
            }
            appended_to = appended_to.saturating_add(u64::try_from(blocks).unwrap_or(u64::MAX));
            progress.appended(blocks, bytes, appended_to.saturating_sub(1));
            pending.clear();
            pending_bytes = 0;
        }
        if stopped || reads.is_empty() {
            break;
        }
    }
    let range = archive.range().await;
    let range = range.wrap_err("failed to read the block archive")?;
    progress.summary(range.map(|(first, last)| (first.number, last.number)));
    Ok(stopped
        .then_some(appended_to)
        .filter(|next| *next <= plan.last))
}

/// Writes every chunk of the range that has no ClickHouse marker yet, in block order, and
/// marks it. Returns the first block of the chunk `cancel` stopped it at, `None` when
/// ClickHouse holds the whole range.
async fn fill_clickhouse(
    committed: &ClickHouseStore,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<Option<u64>> {
    info!(first = plan.first, last = plan.last, "loading ClickHouse");
    let (mut loaded, mut logged) = (0_u64, Instant::now());
    for chunk in plan.chunks() {
        let marker = state.clickhouse_loaded_path(chunk);
        if exists(&marker)? {
            continue;
        }
        if cancel.is_cancelled() {
            return Ok(Some(chunk.from));
        }
        let path = state.verified_path(chunk);
        // One signature of work per block is already done by `verify`; decoding is still CPU
        // work, so it runs off the runtime.
        let blocks = spawn_blocking(move || {
            read(&path, chunk)?
                .into_iter()
                .map(decode)
                .collect::<eyre::Result<Vec<DecodedBlock>>>()
                .wrap_err_with(|| format!("{} does not decode", path.display()))
        })
        .await
        .wrap_err("reading a chunk panicked")??;
        for batch in blocks.chunks(INSERT_BLOCKS) {
            let insert = retry(cancel, Store::Committed, "ClickHouse insert", || {
                committed.insert(batch)
            });
            if insert.await?.is_none() {
                return Ok(Some(chunk.from));
            }
        }
        spawn_blocking(move || write_atomic(&marker, |_file| Ok(())))
            .await
            .wrap_err("writing a marker panicked")?
            .wrap_err("failed to mark a chunk as loaded")?;
        loaded = loaded.saturating_add(chunk.blocks());
        if logged.elapsed() >= progress::INTERVAL {
            logged = Instant::now();
            info!(
                blocks = loaded,
                up_to = chunk.to.saturating_sub(1),
                "loading ClickHouse"
            );
        }
    }
    info!(blocks = loaded, "ClickHouse holds the range");
    Ok(None)
}

/// What the archive pass has done, for its progress lines.
struct Progress {
    started: Instant,
    logged: Instant,
    rate: Rate,
    /// Blocks this run has to append.
    total: u64,
    blocks: u64,
    bytes: u64,
}

impl Progress {
    fn new(total: u64) -> Self {
        let now = Instant::now();
        Self {
            started: now,
            logged: now,
            rate: Rate::new(),
            total,
            blocks: 0,
            bytes: 0,
        }
    }

    /// Records an append that brought the archive's last block to `tip`, and logs a progress
    /// line when one is due.
    fn appended(&mut self, blocks: usize, bytes: usize, tip: u64) {
        self.blocks = self
            .blocks
            .saturating_add(u64::try_from(blocks).unwrap_or(u64::MAX));
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        if self.logged.elapsed() < progress::INTERVAL {
            return;
        }
        self.logged = Instant::now();
        let blocks_per_sec = self.rate.per_sec(self.blocks);
        info!(
            blocks = self.blocks,
            of = self.total,
            archive_tip = tip,
            blocks_per_sec,
            secs_left = self
                .total
                .saturating_sub(self.blocks)
                .checked_div(blocks_per_sec),
            bytes = self.bytes,
            "loading"
        );
    }

    fn summary(&self, archive: Option<(u64, u64)>) {
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            blocks = self.blocks,
            of = self.total,
            bytes = self.bytes,
            blocks_per_sec = self.blocks / secs,
            secs,
            archive_first = archive.map(|(first, _)| first),
            archive_last = archive.map(|(_, last)| last),
            "{}",
            if self.blocks == self.total {
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
