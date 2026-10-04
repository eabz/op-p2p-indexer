//! `load`: appends verified chunks to the local block archive the node serves from, and,
//! only when asked, writes them to ClickHouse too.
//!
//! ```text
//! <state>/verified/<chunk>.blk ─▶ the verified bytes ─▶ ArchiveStore::append_batch (fjall)
//!                                 ─▶ <state>/loaded/<chunk>.archive
//!            with --clickhouse-url ─▶ typed blocks ─▶ CommittedStore::insert (ClickHouse)
//!                                 ─▶ <state>/loaded/<chunk>.clickhouse
//! ```
//!
//! By default nothing but the archive is touched: no database is needed, contacted or
//! migrated. Each target has its own marker per chunk, written once that target holds the
//! chunk, so ClickHouse can be loaded later from the same verified chunks without redoing
//! the archive, and a run that was stopped repeats at most one chunk per target; both writes
//! are idempotent.
//!
//! Chunks are loaded in block order and only while every chunk before them is loaded: the
//! archive holds one contiguous range. It stops at the first chunk that is not verified yet.
//!
//! The archive gets the bytes `verify` checked, unchanged. The typed blocks for ClickHouse
//! are decoded from those same bytes; nothing is encoded again. Does not download or verify
//! anything, and trusts a verified chunk's file.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use alloy_consensus::{BlockBody, Header};
use alloy_primitives::{Address, ChainId};
use clap::Args;
use eyre::{WrapErr, ensure, eyre};
use op_alloy_consensus::{OpBlock, OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_primitives::{BlockSource, DecodedBlock, EncodedBlock, decode_transaction};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::committed_store::ClickHouseStore;
use op_indexer_storage::{ArchiveStore, ClickHouseConfig, CommittedStore, Severity, StorageError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chunk;
use crate::cli::ApiToken;
use crate::state::{Chunk, Plan, State, Target, write_atomic};

/// Blocks per insert into ClickHouse. An insert is a few requests whatever its
/// size, so it is as large as memory comfortably allows.
const INSERT_BLOCKS: usize = 1000;
/// Wait before the first retry of a store call that failed with a transient error.
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Longest wait between retries.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// How often progress is logged.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

/// Settings of `load`. By default it fills the local block archive the node serves from and
/// needs no database; ClickHouse is loaded only when `--clickhouse-url` is given.
#[derive(Debug, Clone, Args)]
pub(crate) struct LoadArgs {
    /// Directory of the indexer's block archive: `archive` inside its data directory. The
    /// indexer must not be running on it. It must be empty or end at the block before the
    /// range, and the indexer must then keep every block
    /// (`OP_INDEXER_ARCHIVE_RETENTION_BLOCKS=all`).
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_ARCHIVE_DIR",
        default_value = "data/archive"
    )]
    pub(crate) archive_dir: PathBuf,
    /// Also write the blocks to ClickHouse, at this HTTP interface (for example
    /// `http://127.0.0.1:8123`). Optional: without it no database is contacted. Its
    /// migrations are applied if missing. Can be given on a later run: chunks already in the
    /// archive are then only written to ClickHouse.
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
    pub(crate) clickhouse_password: Option<ApiToken>,
    /// L2 chain id the ClickHouse rows are stored under. Only used with `--clickhouse-url`.
    #[arg(long, env = "OP_INDEXER_CHAIN_ID", default_value_t = 10)]
    pub(crate) chain_id: ChainId,
}

/// Loads every verified chunk of `plan` into the targets that do not hold it yet, in block
/// order, until a chunk is not verified, the range is done, or `cancel` fires.
///
/// # Errors
///
/// Returns an error if the archive cannot be opened, a chunk's file cannot be read or does
/// not hold its blocks, or the archive does not end where a chunk starts; and, when
/// ClickHouse is asked for, if it cannot be reached or migrated or refuses a chunk.
pub(crate) async fn run(
    args: &LoadArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let committed = match &args.clickhouse_url {
        Some(url) => Some(clickhouse(args, url).await?),
        None => None,
    };
    // Startup-only blocking I/O, before any chunk is read.
    let archive =
        FjallArchive::open(&args.archive_dir).wrap_err("failed to open the block archive")?;

    let (mut loaded, mut skipped) = (0_u64, 0_u64);
    let mut last_progress = Instant::now();
    for chunk in plan.chunks() {
        if cancel.is_cancelled() {
            break;
        }
        let archive_marker = state.loaded_path(chunk, Target::Archive);
        let clickhouse_marker = state.loaded_path(chunk, Target::ClickHouse);
        // What this chunk still has to be written to.
        let to_archive = (!exists(&archive_marker)?).then_some((&archive, archive_marker));
        let to_clickhouse = match &committed {
            Some(committed) if !exists(&clickhouse_marker)? => Some((committed, clickhouse_marker)),
            Some(_) | None => None,
        };
        if to_archive.is_none() && to_clickhouse.is_none() {
            skipped = skipped.saturating_add(1);
            continue;
        }
        let path = state.verified_path(chunk);
        if !exists(&path)? {
            info!(
                from = chunk.from,
                "stopping at the first chunk that is not verified"
            );
            break;
        }
        if !load(path, chunk, to_archive, to_clickhouse, cancel).await? {
            break;
        }
        loaded = loaded.saturating_add(1);
        if last_progress.elapsed() >= PROGRESS_INTERVAL {
            last_progress = Instant::now();
            info!(chunks = loaded, up_to = chunk.to, "loading");
        }
    }
    info!(
        loaded,
        already_loaded = skipped,
        clickhouse = committed.is_some(),
        "load finished: chunks appended to the block archive"
    );
    Ok(())
}

/// Connects to ClickHouse at `url` and applies its migrations.
async fn clickhouse(args: &LoadArgs, url: &str) -> eyre::Result<ClickHouseStore> {
    let config = ClickHouseConfig {
        url: url.to_owned(),
        database: args.clickhouse_database.clone(),
        user: args.clickhouse_user.clone(),
        password: args
            .clickhouse_password
            .as_ref()
            .map(|password| password.expose().to_owned()),
    };
    let committed = ClickHouseStore::new(&config, args.chain_id);
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

/// Loads the chunk in the file `path` into each target given, and writes that target's
/// marker. Returns `false` if cancellation stopped it; what has no marker yet is loaded
/// again by the next run.
async fn load(
    path: PathBuf,
    chunk: Chunk,
    archive: Option<(&FjallArchive, PathBuf)>,
    committed: Option<(&ClickHouseStore, PathBuf)>,
    cancel: &CancellationToken,
) -> eyre::Result<bool> {
    let typed = committed.is_some();
    let (blocks, encoded) = tokio::task::spawn_blocking(move || read(&path, chunk, typed))
        .await
        .wrap_err("reading a chunk panicked")??;

    if let Some((archive, marker)) = archive {
        // The clone is of reference-counted buffers; the bytes are not copied.
        let append = retry(cancel, "archive append_batch", || {
            archive.append_batch(encoded.clone())
        });
        if append.await?.is_none() {
            return Ok(false);
        }
        mark(marker).await?;
    }
    if let Some((committed, marker)) = committed {
        for batch in blocks.chunks(INSERT_BLOCKS) {
            let insert = retry(cancel, "ClickHouse insert", || committed.insert(batch));
            if insert.await?.is_none() {
                return Ok(false);
            }
        }
        mark(marker).await?;
    }
    Ok(true)
}

/// Writes a chunk's marker for one target.
async fn mark(marker: PathBuf) -> eyre::Result<()> {
    tokio::task::spawn_blocking(move || write_atomic(&marker, |_file| Ok(())))
        .await
        .wrap_err("writing a marker panicked")?
        .wrap_err("failed to mark a chunk as loaded")
}

/// Reads a verified chunk: its blocks' bytes for the archive, in block order, and, when
/// `typed`, the same blocks decoded for ClickHouse (empty otherwise). Blocking.
fn read(
    path: &Path,
    chunk: Chunk,
    typed: bool,
) -> eyre::Result<(Vec<DecodedBlock>, Vec<EncodedBlock>)> {
    let (_link, verified) = chunk::read(path)
        .wrap_err_with(|| format!("failed to read verified chunk {}", path.display()))?;
    ensure!(
        u64::try_from(verified.len()).ok() == Some(chunk.blocks()),
        "{} holds {} blocks, not the {} of its chunk",
        path.display(),
        verified.len(),
        chunk.blocks()
    );
    let mut blocks = Vec::new();
    let mut encoded = Vec::with_capacity(verified.len());
    for (number, block) in (chunk.from..).zip(verified) {
        if typed {
            let decoded = decode(&block.encoded, block.senders).wrap_err_with(|| {
                format!("block {number} in {} does not decode", path.display())
            })?;
            ensure!(
                decoded.block.header.number == number,
                "{} holds block {} where block {number} belongs",
                path.display(),
                decoded.block.header.number
            );
            blocks.push(decoded);
        }
        encoded.push(block.encoded);
    }
    Ok((blocks, encoded))
}

/// Decodes one verified block from its consensus encoding into the typed form ClickHouse's
/// rows are built from.
fn decode(encoded: &EncodedBlock, senders: Vec<Address>) -> eyre::Result<DecodedBlock> {
    let header = alloy_rlp::decode_exact::<Header>(&encoded.header).wrap_err("header")?;
    let (transactions, has_withdrawals) = decode_body(&encoded.body).wrap_err("body")?;
    ensure!(
        transactions.len() == senders.len(),
        "{} transactions and {} senders",
        transactions.len(),
        senders.len()
    );
    let receipts = encoded
        .receipts
        .as_ref()
        .ok_or_else(|| eyre!("a verified block has no receipts"))?;
    let receipts =
        alloy_rlp::decode_exact::<Vec<OpReceiptEnvelope>>(receipts).wrap_err("receipts")?;
    Ok(DecodedBlock {
        block: OpBlock {
            header,
            body: BlockBody {
                transactions,
                // Verified blocks have none: `verify` checked the ommers hash.
                ommers: Vec::new(),
                withdrawals: has_withdrawals.then(Default::default),
            },
        },
        hash: encoded.hash,
        senders,
        receipts: Some(receipts),
        source: BlockSource::Import,
    })
}

/// Decodes the transactions of a block body, `[transactions, ommers]` or
/// `[transactions, ommers, withdrawals]`, and says whether it has a withdrawals list.
///
/// Each transaction goes through [`decode_transaction`], which keeps the hash of the bytes
/// it was given: alloy's own body decoding cannot read a legacy transaction signed with all
/// zeros.
fn decode_body(body: &[u8]) -> eyre::Result<(Vec<OpTxEnvelope>, bool)> {
    let mut fields = list_payload(body)?.0;
    let (mut entries, after_transactions) = list_payload(fields)?;
    fields = after_transactions;
    let (_ommers, after_ommers) = list_payload(fields)?;

    let mut transactions = Vec::new();
    while !entries.is_empty() {
        let start = entries;
        let header = alloy_rlp::Header::decode(&mut entries)?;
        let (payload, rest) = entries
            .split_at_checked(header.payload_length)
            .ok_or_else(|| eyre!("a transaction is cut short"))?;
        entries = rest;
        // In a body a typed transaction is wrapped in an RLP string; a legacy one is a list
        // and is taken with its list header.
        let leaf = if header.list {
            start
                .get(..start.len().saturating_sub(rest.len()))
                .ok_or_else(|| eyre!("a transaction is cut short"))?
        } else {
            payload
        };
        transactions.push(decode_transaction(leaf)?);
    }
    Ok((transactions, !after_ommers.is_empty()))
}

/// Splits `buf` at its first RLP item, which must be a list: the list's payload, and what
/// follows the list.
fn list_payload(buf: &[u8]) -> eyre::Result<(&[u8], &[u8])> {
    let mut rest = buf;
    let header = alloy_rlp::Header::decode(&mut rest)?;
    ensure!(header.list, "expected an RLP list");
    rest.split_at_checked(header.payload_length)
        .ok_or_else(|| eyre!("an RLP list is cut short"))
}

fn exists(path: &Path) -> eyre::Result<bool> {
    path.try_exists()
        .wrap_err_with(|| format!("failed to look for {}", path.display()))
}

/// Runs a store call and repeats it while it fails with a transient error, waiting between
/// attempts with exponential backoff, shortened at random by up to half. `None` if `cancel`
/// fired while waiting.
///
/// # Errors
///
/// Returns the first error that is not transient.
async fn retry<T, F, Fut>(
    cancel: &CancellationToken,
    operation: &'static str,
    mut call: F,
) -> eyre::Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let err = match call().await {
            Ok(value) => return Ok(Some(value)),
            Err(err) if err.severity() == Severity::Transient => err,
            Err(err) => return Err(err).wrap_err(operation),
        };
        let half = backoff / 2;
        let delay = half + half.mul_f64(fastrand::f64());
        warn!(operation, ?delay, ?err, "store call failed, retrying");
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(None),
            () = sleep(delay) => {}
        }
        backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
    }
}
