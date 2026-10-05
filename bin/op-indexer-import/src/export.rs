//! `export`: converts the verified range into sealed chunks and uploads them to object
//! storage, with their manifest and the hash index (`docs/serving.md` §3). One-time per chain.
//!
//! ```text
//! <state>/verified/<chunk>.blk ─▶ senders checked ─▶ ChunkRecord ─▶ ChunkWriter ─▶ R2
//!                                 (every core)        (every core)   (in order)    chunk, then
//!                                                                                  manifest
//! ```
//!
//! Only a range `verify` accepted whole is exported. Every sender is proven before its block
//! is sealed, because the verified chunks record the archive service's:
//!
//! - a signed transaction: the sender is recovered from its signature and must equal it;
//! - a deposit: it must equal the `from` in the deposit's encoding, which the block hash covers;
//! - a legacy transaction signed with all zeros (an L1-to-L2 message before Bedrock) has no
//!   signer: the recorded sender is kept and counted, unproven.
//!
//! Resumable: the manifest says where the last run stopped, and one listing of the bucket says
//! which chunks after it are already uploaded (their names carry their root, so they are
//! skipped). The hash index's first generation is written at the end. Credentials come from
//! the environment and are never logged.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Instant;

use alloy_consensus::transaction::SignerRecoverable as _;
use eyre::{WrapErr, ensure, eyre};
use futures_util::{StreamExt as _, TryStreamExt as _};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_chunks::{
    ChunkEntry, ChunkRecord, ChunkStore, ChunkWriter, IndexBuilder, Manifest, R2Config,
    ReadOptions, SealedChunk,
};
use op_indexer_primitives::{decode_transaction, is_zero_signature, split_body};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::chunk::{self, VerifiedBlock};
use crate::cli::ExportArgs;
use crate::progress::{self, Rate};
use crate::state::{Chunk, Plan, State};

/// The exporter id this tool writes into the manifest.
const EXPORTER: &str = "import-export";
/// Chunks listed per manifest segment.
const CHUNKS_PER_SEGMENT: usize = 16;
/// Chunk indexes read at once when a resumed run rebuilds the hash index's input.
const INDEX_READS: usize = 32;

/// Exports the verified range of `plan`.
///
/// # Errors
///
/// Returns an error if `verify` has not accepted the range, the credentials or the target are
/// missing, a verified chunk cannot be read or carries a wrong sender, a block cannot be
/// sealed, the store fails, or `cancel` fires (run again to continue).
pub(crate) async fn run(
    args: &ExportArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    ensure!(!cancel.is_cancelled(), "stopped before `export` began");
    state
        .read_verified()?
        .filter(|range| range.covers(plan))
        .ok_or_else(|| {
            eyre!(
                "blocks {} to {} are not verified: run `verify`, which must accept the whole range",
                plan.first,
                plan.last
            )
        })?;
    let store = open_store(args, plan)?;
    let mut manifest = Manifest::load(&store)
        .await
        .wrap_err("failed to read the manifest")?;
    let done = manifest.last().is_some_and(|entry| entry.last >= plan.last);
    if done
        && manifest
            .index_generation()
            .is_some_and(|generation| generation.through_block >= plan.last)
    {
        info!(chunks = manifest.entries().len(), "export already complete");
        return Ok(());
    }
    let next = manifest
        .last()
        .map_or(plan.first, |entry| entry.last.saturating_add(1));
    let mut index = IndexBuilder::new(&state.root().join("export-index"))
        .wrap_err("failed to start the hash index")?;
    // A resumed run rebuilds the index's input from the chunks it exported before.
    let mut listed = futures_util::stream::iter(manifest.entries().to_vec())
        .map(|entry| {
            let store = store.clone();
            async move { store.index(&entry).await }
        })
        .buffered(INDEX_READS);
    while let Some(chunk_index) = listed.try_next().await? {
        for (hash, number) in chunk_index.blocks() {
            index.push(hash, number)?;
        }
    }
    drop(listed);
    if !done {
        let parts = Parts {
            args,
            state,
            plan,
            store: &store,
        };
        export_blocks(&parts, &mut manifest, &mut index, next, cancel).await?;
    }
    let generation = manifest
        .index_generation()
        .map_or(0, |generation| generation.generation.saturating_add(1));
    info!(generation, "writing the hash index");
    // The builder holds every exported block, so the generation needs no base.
    let written = index
        .finish(&store, None, generation, plan.last)
        .await
        .wrap_err("failed to write the hash index")?;
    manifest
        .append_generation(&store, written, EXPORTER)
        .await
        .wrap_err("failed to record the hash index in the manifest")?;
    info!(
        generation = written.generation,
        entries = written.entries,
        chunks = manifest.entries().len(),
        "export complete"
    );
    Ok(())
}

/// The store `args` name: a local directory, or R2 from the environment's credentials, in the
/// chain's bucket (`<chain>-snapshot` unless given) under the prefix.
fn open_store(args: &ExportArgs, plan: &Plan) -> eyre::Result<ChunkStore> {
    if let Some(dir) = &args.to_dir {
        return ChunkStore::local(dir, &args.r2_prefix, plan.chain, ReadOptions::default())
            .wrap_err_with(|| format!("failed to use {}", dir.display()));
    }
    let missing = |name: &str| eyre!("{name} is not set (or pass --to-dir)");
    let config = R2Config {
        account_id: args
            .r2_account_id
            .clone()
            .ok_or_else(|| missing("OP_INDEXER_R2_ACCOUNT_ID"))?,
        bucket: args
            .r2_bucket
            .clone()
            .unwrap_or_else(|| format!("{}-snapshot", plan.chain.name)),
        prefix: args.r2_prefix.clone(),
        access_key_id: args
            .r2_access_key_id
            .as_ref()
            .ok_or_else(|| missing("OP_INDEXER_R2_ACCESS_KEY_ID"))?
            .expose()
            .to_owned(),
        secret_access_key: args
            .r2_secret_access_key
            .as_ref()
            .ok_or_else(|| missing("OP_INDEXER_R2_SECRET_ACCESS_KEY"))?
            .expose()
            .to_owned(),
        endpoint: args.r2_endpoint.clone(),
    };
    info!(
        bucket = config.bucket,
        prefix = config.prefix,
        "exporting to R2"
    );
    ChunkStore::r2(&config, plan.chain, ReadOptions::default())
        .wrap_err("failed to set up the R2 client")
}

/// What every step of a run reads.
struct Parts<'a> {
    args: &'a ExportArgs,
    state: &'a State,
    plan: &'a Plan,
    store: &'a ChunkStore,
}

/// Senders checked, as [`check_senders`] counts them.
#[derive(Debug, Default, Clone, Copy)]
struct Senders {
    /// Transactions whose sender was recovered from the signature and matched.
    recovered: u64,
    /// Legacy transactions signed with all zeros, whose recorded sender is kept unproven.
    zero_signatures: u64,
}

impl Senders {
    fn add(&mut self, other: Self) {
        self.recovered = self.recovered.saturating_add(other.recovered);
        self.zero_signatures = self.zero_signatures.saturating_add(other.zero_signatures);
    }
}

/// What the export did, for its progress lines.
#[derive(Debug, Default)]
struct Totals {
    blocks: u64,
    chunks: u64,
    skipped: u64,
    read_bytes: u64,
    uploaded_bytes: u64,
    senders: Senders,
}

/// The export's progress lines, every [`progress::INTERVAL`], with speeds over the last
/// minute and the time left from the verified bytes still to read.
struct Progress {
    blocks: u64,
    total_bytes: u64,
    started: Instant,
    logged: Instant,
    blocks_rate: Rate,
    read_rate: Rate,
    upload_rate: Rate,
}

impl Progress {
    fn new(blocks: u64, total_bytes: u64) -> Self {
        Self {
            blocks,
            total_bytes,
            started: Instant::now(),
            logged: Instant::now(),
            blocks_rate: Rate::new(),
            read_rate: Rate::new(),
            upload_rate: Rate::new(),
        }
    }

    fn log(&mut self, totals: &Totals, uploads_in_flight: usize) {
        if self.logged.elapsed() < progress::INTERVAL {
            return;
        }
        self.logged = Instant::now();
        let read_per_sec = self.read_rate.per_sec(totals.read_bytes);
        info!(
            blocks = totals.blocks,
            of = self.blocks,
            chunks = totals.chunks,
            skipped = totals.skipped,
            blocks_per_sec = self.blocks_rate.per_sec(totals.blocks),
            read_mb_per_sec = read_per_sec / 1_000_000,
            upload_mb_per_sec = self.upload_rate.per_sec(totals.uploaded_bytes) / 1_000_000,
            uploads_in_flight,
            senders_recovered = totals.senders.recovered,
            secs_left = self.total_bytes.saturating_sub(totals.read_bytes) / read_per_sec.max(1),
            "exporting"
        );
    }
}

/// Uploads in flight, oldest first, with the entries the manifest lists once they finish.
type Uploads = VecDeque<(ChunkEntry, JoinHandle<eyre::Result<()>>)>;

/// Seals and uploads the blocks from `next` to the end of the range.
async fn export_blocks(
    parts: &Parts<'_>,
    manifest: &mut Manifest,
    index: &mut IndexBuilder,
    next: u64,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let Parts {
        args,
        state,
        plan,
        store,
    } = *parts;
    let chunks: Vec<Chunk> = plan.chunks().filter(|chunk| chunk.to > next).collect();
    let paths: Vec<PathBuf> = chunks
        .iter()
        .map(|chunk| state.verified_path(*chunk))
        .collect();
    let total_bytes = tokio::task::spawn_blocking(move || {
        paths
            .iter()
            .map(|path| std::fs::metadata(path).map(|meta| meta.len()))
            .sum::<std::io::Result<u64>>()
    })
    .await?
    .wrap_err("failed to measure the verified chunks")?;
    let stored = store
        .stored_chunks()
        .await
        .wrap_err("failed to list the chunks already uploaded")?;
    let threads = crate::threads(args.threads);
    info!(
        first = next,
        last = plan.last,
        verified_bytes = total_bytes,
        already_uploaded = stored.len(),
        threads,
        "exporting blocks"
    );
    let mut prepared = futures_util::stream::iter(chunks)
        .map(|chunk| {
            let path = state.verified_path(chunk);
            tokio::task::spawn_blocking(move || prepare(&path, chunk, next))
        })
        .buffered(threads);
    let mut writer = ChunkWriter::new(plan.chain, next);
    let (mut uploads, mut listed): (Uploads, Vec<ChunkEntry>) = (VecDeque::new(), Vec::new());
    let mut totals = Totals::default();
    let mut progress = Progress::new(
        plan.last.saturating_add(1).saturating_sub(next),
        total_bytes,
    );
    while let Some(batch) = prepared.try_next().await? {
        let batch = batch?;
        totals.senders.add(batch.senders);
        totals.read_bytes = totals.read_bytes.saturating_add(batch.file_bytes);
        // Sealing compresses and hashes: off the async scheduler.
        let sealed = tokio::task::block_in_place(|| {
            seal(&mut writer, index, batch.records, plan, &mut totals)
        })?;
        for chunk in sealed {
            totals.chunks = totals.chunks.saturating_add(1);
            if stored.contains(&chunk.entry.key()) {
                totals.skipped = totals.skipped.saturating_add(1);
                settle(&mut uploads, 0, &mut listed).await?;
                listed.push(chunk.entry);
            } else {
                totals.uploaded_bytes = totals.uploaded_bytes.saturating_add(chunk.entry.size);
                settle(&mut uploads, args.uploads.max(1) - 1, &mut listed).await?;
                uploads.push_back(upload(store, chunk));
            }
            if listed.len() >= CHUNKS_PER_SEGMENT {
                manifest
                    .append(store, std::mem::take(&mut listed), EXPORTER)
                    .await?;
            }
        }
        progress.log(&totals, uploads.len());
        if cancel.is_cancelled() {
            break;
        }
    }
    // The chunks in flight are uploaded and listed before stopping or finishing.
    settle(&mut uploads, 0, &mut listed).await?;
    if !listed.is_empty() {
        manifest.append(store, listed, EXPORTER).await?;
    }
    ensure!(
        !cancel.is_cancelled(),
        "stopped at block {}; run `export` again to continue",
        manifest
            .last()
            .map_or(next, |entry| entry.last.saturating_add(1))
    );
    let secs = progress.started.elapsed().as_secs().max(1);
    info!(
        blocks = totals.blocks,
        chunks = totals.chunks,
        skipped = totals.skipped,
        uploaded_bytes = totals.uploaded_bytes,
        secs,
        blocks_per_sec = totals.blocks / secs,
        senders_recovered = totals.senders.recovered,
        zero_signature_transactions = totals.senders.zero_signatures,
        "all blocks exported"
    );
    Ok(())
}

/// Adds `records` to the chunk being written and to the index; returns the chunks they
/// completed. A chunk ends where D4 ends it, or at the range's last block. Blocking.
fn seal(
    writer: &mut ChunkWriter,
    index: &mut IndexBuilder,
    records: Vec<ChunkRecord>,
    plan: &Plan,
    totals: &mut Totals,
) -> eyre::Result<Vec<SealedChunk>> {
    let mut sealed = Vec::new();
    for record in records {
        let (number, hash) = (record.number(), record.hash());
        index.push(hash, number)?;
        totals.blocks = totals.blocks.saturating_add(1);
        if writer.push(record)? || number == plan.last {
            let next = ChunkWriter::new(plan.chain, number.saturating_add(1));
            sealed.push(std::mem::replace(writer, next).finish()?);
        }
    }
    Ok(sealed)
}

/// Waits for the oldest uploads until at most `keep` are in flight, moving their entries to
/// `listed` in chunk order.
async fn settle(
    uploads: &mut Uploads,
    keep: usize,
    listed: &mut Vec<ChunkEntry>,
) -> eyre::Result<()> {
    while uploads.len() > keep {
        if let Some((entry, handle)) = uploads.pop_front() {
            handle.await??;
            listed.push(entry);
        }
    }
    Ok(())
}

/// Starts uploading `sealed`.
fn upload(store: &ChunkStore, sealed: SealedChunk) -> (ChunkEntry, JoinHandle<eyre::Result<()>>) {
    let store = store.clone();
    let entry = sealed.entry;
    let handle = tokio::spawn(async move {
        store.put_chunk(&sealed).await.wrap_err_with(|| {
            format!(
                "failed to upload chunk {}-{}",
                sealed.entry.first, sealed.entry.last
            )
        })
    });
    (entry, handle)
}

/// One verified chunk, prepared.
struct Prepared {
    records: Vec<ChunkRecord>,
    senders: Senders,
    file_bytes: u64,
}

/// Reads the verified chunk at `path`, checks the senders of its blocks from `next` on and
/// prepares their records. Blocking.
fn prepare(path: &Path, chunk: Chunk, next: u64) -> eyre::Result<Prepared> {
    let file_bytes = std::fs::metadata(path)
        .wrap_err_with(|| format!("failed to read verified chunk {}", path.display()))?
        .len();
    let mut prepared = Prepared {
        records: Vec::new(),
        senders: Senders::default(),
        file_bytes,
    };
    for (number, block) in (chunk.from..).zip(read(path, chunk)?) {
        if number < next {
            continue;
        }
        prepared.senders.add(check_senders(number, &block)?);
        prepared
            .records
            .push(ChunkRecord::new(&block).wrap_err_with(|| format!("block {number}"))?);
    }
    Ok(prepared)
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

/// Checks the sender recorded for every transaction of `block` (number `number`) in its
/// verified chunk, which is the one the archive service reported (module docs).
///
/// # Errors
///
/// Returns an error naming the block, the transaction's index and both addresses for the
/// first sender that differs, or if a transaction does not decode or has no recoverable
/// sender.
fn check_senders(number: u64, block: &VerifiedBlock) -> eyre::Result<Senders> {
    let body = split_body(&block.encoded.body)
        .ok_or_else(|| eyre!("block {number}: the verified body does not decode"))?;
    ensure!(
        body.transactions.len() == block.senders.len(),
        "block {number}: its verified chunk records {} senders for {} transactions",
        block.senders.len(),
        body.transactions.len()
    );
    let mut checked = Senders::default();
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
             block on is exported"
        );
    }
    Ok(checked)
}
