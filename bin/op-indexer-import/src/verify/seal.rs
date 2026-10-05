//! Sealing and uploading: the downloaded chunks, checked on every core (`block`) with their
//! senders, become [`ChunkRecord`]s, which a [`ChunkWriter`] seals in block order into the
//! chunks of `docs/serving.md` §1 (cut by size, block count and the Bedrock block, so the
//! same blocks always give the same chunks). Each chunk is uploaded, then recorded in
//! `<state>/sealed`, then the downloaded chunks it covers are deleted.
//!
//! Also the end of the step: the anchor check, read back from the store, and the hash index.

use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::time::Instant;

use alloy_consensus::Header;
use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use eyre::{WrapErr, ensure, eyre};
use futures_util::{StreamExt as _, TryStreamExt as _};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_chunks::{
    ChunkEntry, ChunkRecord, ChunkStore, ChunkWriter, IndexBuilder, Manifest, R2Config,
    ReadOptions, SealedChunk,
};
use op_indexer_primitives::{ArchivedBlock, decode_transaction, is_zero_signature, split_body};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{Check, ChunkError, Forks, IN_FLIGHT_BYTES, Stats, block};
use crate::cli::VerifyArgs;
use crate::progress::{self, Rate};
use crate::state::{Anchor, Chunk, Plan, State, covered};

/// The exporter id this tool writes into the manifest.
pub(super) const EXPORTER: &str = "import";
/// Chunks listed per manifest segment.
pub(super) const CHUNKS_PER_SEGMENT: usize = 16;
/// Chunk indexes read at once when a resumed run rebuilds the hash index's input.
const INDEX_READS: usize = 32;

/// The store `args` name: a local directory, or R2 from the environment's credentials, in the
/// chain's bucket (`<chain>-snapshot` unless given) under the prefix.
pub(super) fn open_store(args: &VerifyArgs, plan: &Plan) -> eyre::Result<ChunkStore> {
    if let Some(dir) = &args.to_dir {
        return ChunkStore::local(dir, &args.r2_prefix, plan.chain, ReadOptions::default())
            .wrap_err_with(|| format!("failed to use {}", dir.display()));
    }
    let mut config = R2Config::from_lookup(plan.chain, |name| match name {
        "OP_INDEXER_R2_ACCOUNT_ID" => args.r2_account_id.clone(),
        "OP_INDEXER_R2_BUCKET" => args.r2_bucket.clone(),
        "OP_INDEXER_R2_PREFIX" => Some(args.r2_prefix.clone()),
        "OP_INDEXER_R2_ACCESS_KEY_ID" => args
            .r2_access_key_id
            .as_ref()
            .map(|key| key.expose().to_owned()),
        "OP_INDEXER_R2_SECRET_ACCESS_KEY" => args
            .r2_secret_access_key
            .as_ref()
            .map(|key| key.expose().to_owned()),
        "OP_INDEXER_R2_ENDPOINT" => args.r2_endpoint.clone(),
        _ => None,
    })
    .wrap_err("R2 configuration is incomplete (or pass --to-dir)")?;
    config.prefix.clone_from(&args.r2_prefix);
    info!(
        bucket = config.bucket,
        prefix = config.prefix,
        "uploading to R2"
    );
    ChunkStore::r2(&config, plan.chain, ReadOptions::default())
        .wrap_err("failed to set up the R2 client")
}

/// Checks that the records of an earlier run, `sealed`, start at `start` and continue the
/// manifest's last chunk `listed` and each other.
pub(super) fn check_continues(
    listed: Option<&ChunkEntry>,
    start: u64,
    sealed: &[ChunkEntry],
) -> eyre::Result<()> {
    let (mut next, mut parent) = (start, listed.map(|entry| entry.last_hash));
    for entry in sealed {
        ensure!(
            entry.first == next,
            "the records in the state directory's sealed/ skip from block {next} to {}: delete \
             the records from {next} on and run `verify` again",
            entry.first
        );
        check_link(entry, parent)?;
        (next, parent) = (entry.last.saturating_add(1), Some(entry.last_hash));
    }
    Ok(())
}

/// Checks that `entry` names `parent`, the hash of the block before it, if known.
fn check_link(entry: &ChunkEntry, parent: Option<B256>) -> eyre::Result<()> {
    if let Some(parent) = parent {
        ensure!(
            entry.first_parent == parent,
            "block {}: parent hash is {}, the block before has hash {parent}; nothing is listed \
             in the manifest",
            entry.first,
            entry.first_parent
        );
    }
    Ok(())
}

/// Checks the range's last block, the last of the chunk `last`, against the plan's anchor; a
/// dispute game's claim needs the header, read back from the store.
pub(super) async fn check_anchor(
    store: &ChunkStore,
    plan: &Plan,
    last: &ChunkEntry,
) -> eyre::Result<()> {
    ensure!(
        last.last == plan.last,
        "the sealed chunks end at block {}, not at the range's last block {}",
        last.last,
        plan.last
    );
    let game = match plan.anchor {
        Anchor::Hash(anchor) => {
            ensure!(
                last.last_hash == anchor,
                "block {}: hash is {}, the anchor is {anchor}; nothing is listed in the manifest",
                plan.last,
                last.last_hash
            );
            return Ok(());
        }
        Anchor::Game(game) => game,
    };
    let index = store
        .index(last)
        .await
        .wrap_err("failed to read the last chunk back for the anchor")?;
    let block = store
        .block(last, &index, plan.last)
        .await
        .wrap_err("failed to read the last block back for the anchor")?
        .ok_or_else(|| eyre!("the last chunk does not hold block {}", plan.last))?;
    let header = Header::decode(&mut &block.encoded.header[..])
        .wrap_err("the last block's header does not decode")?;
    // The game's claim covers the state root and the withdrawals root of the last header.
    game.check(
        last.last_hash,
        (header.timestamp, plan.chain.isthmus_time()),
        header.state_root,
        header.withdrawals_root,
    )
    .map_err(|err| {
        eyre!(
            "block {}: {err}; nothing is listed in the manifest",
            plan.last
        )
    })
}

/// The sealing of a range, from the records of earlier runs on.
pub(super) struct Sealing<'a> {
    args: &'a VerifyArgs,
    state: &'a State,
    plan: &'a Plan,
    store: &'a ChunkStore,
    /// The chunks sealed after the manifest's last, uploaded and recorded, in block order.
    sealed: Vec<ChunkEntry>,
    /// Every block of the listed and sealed chunks, for the hash index.
    index: IndexBuilder,
    /// The first block not sealed yet, and the hash of the block before it, if known.
    next: u64,
    parent: Option<B256>,
}

impl<'a> Sealing<'a> {
    /// Starts after the manifest's chunks `listed` and the records `sealed` (checked with
    /// [`check_continues`]), reading the blocks of both for the hash index.
    pub(super) async fn new(
        args: &'a VerifyArgs,
        state: &'a State,
        plan: &'a Plan,
        store: &'a ChunkStore,
        listed: &[ChunkEntry],
        sealed: Vec<ChunkEntry>,
    ) -> eyre::Result<Self> {
        let mut index = IndexBuilder::new(&state.root().join("index-build"))
            .wrap_err("failed to start the hash index")?;
        let mut stored = futures_util::stream::iter(listed.iter().chain(&sealed).copied())
            .map(|entry| async move { store.index(&entry).await })
            .buffered(INDEX_READS);
        while let Some(chunk_index) = stored.try_next().await? {
            for (hash, number) in chunk_index.blocks() {
                index.push(hash, number)?;
            }
        }
        drop(stored);
        let last = sealed.last().or(listed.last());
        let next = last.map_or(plan.first, |entry| entry.last.saturating_add(1));
        let parent = last.map(|entry| entry.last_hash);
        Ok(Self {
            args,
            state,
            plan,
            store,
            sealed,
            index,
            next,
            parent,
        })
    }

    /// The chunks sealed after the manifest's last.
    pub(super) fn sealed(&self) -> &[ChunkEntry] {
        &self.sealed
    }

    /// Verifies, seals and uploads the blocks from the first not sealed yet to the range's
    /// end. The chunks uploading when it stops (done, failed or cancelled) are finished and
    /// recorded first.
    pub(super) async fn seal_rest(&mut self, cancel: &CancellationToken) -> eyre::Result<()> {
        if self.next > self.plan.last {
            return Ok(());
        }
        let (plan, next) = (*self.plan, self.next);
        let chunks = {
            let state = self.state.clone();
            let chunks: Vec<Chunk> = plan.chunks().filter(|chunk| chunk.to > next).collect();
            tokio::task::spawn_blocking(move || raw_sizes(&state, chunks)).await??
        };
        let stored = self
            .store
            .stored_chunks()
            .await
            .wrap_err("failed to list the chunks already uploaded")?;
        let raw_bytes = chunks.iter().map(|(_, bytes)| bytes).sum();
        info!(
            first = next,
            last = plan.last,
            chunks = chunks.len(),
            raw_bytes,
            already_uploaded = stored.len(),
            "verifying, sealing and uploading"
        );
        let mut run = Run {
            writer: ChunkWriter::new(plan.chain, next),
            read: VecDeque::new(),
            uploads: VecDeque::new(),
            keep: usize::try_from(self.args.uploads)
                .unwrap_or(1)
                .saturating_sub(1),
            progress: Progress::new(plan.last.saturating_add(1).saturating_sub(next), raw_bytes),
        };
        let sealed = self.seal_chunks(chunks, &stored, &mut run, cancel).await;
        // The chunks in flight are uploaded and recorded before stopping, failed or not.
        let settled = self.settle(&mut run.uploads, 0, &mut run.read).await;
        run.progress.summary();
        sealed?;
        settled?;
        ensure!(
            !cancel.is_cancelled(),
            "stopped at block {}: run `verify` again to go on",
            self.next
        );
        Ok(())
    }

    /// Checks `chunks` on `--threads` threads, at most [`IN_FLIGHT_BYTES`] of downloaded bytes
    /// at once (one chunk is always allowed), and seals and queues their blocks in block order,
    /// until all are done, one fails or `cancel` fires.
    async fn seal_chunks(
        &mut self,
        chunks: Vec<(Chunk, u64)>,
        stored: &HashSet<String>,
        run: &mut Run,
        cancel: &CancellationToken,
    ) -> eyre::Result<()> {
        let forks = Forks::new(self.plan.chain);
        let threads = crate::threads(self.args.threads);
        let next = self.next;
        let mut queue = chunks.into_iter().peekable();
        let mut checking: VecDeque<(Chunk, u64, JoinHandle<eyre::Result<Ready>>)> = VecDeque::new();
        let mut in_flight = 0_u64;
        loop {
            while !cancel.is_cancelled()
                && checking.len() < threads
                && let Some((chunk, bytes)) = queue.next_if(|(_, bytes)| {
                    checking.is_empty() || in_flight.saturating_add(*bytes) <= IN_FLIGHT_BYTES
                })
            {
                in_flight = in_flight.saturating_add(bytes);
                let (raw, fill) = (self.state.raw_path(chunk), self.state.fill_path(chunk));
                let handle =
                    tokio::task::spawn_blocking(move || prepare(&forks, chunk, &raw, &fill, next));
                checking.push_back((chunk, bytes, handle));
            }
            let Some((chunk, bytes, handle)) = checking.pop_front() else {
                return Ok(());
            };
            let ready = handle
                .await
                .map_err(|err| eyre!("a verify task failed: {err}"))??;
            in_flight = in_flight.saturating_sub(bytes);
            run.read.push_back(chunk);
            run.progress.read(bytes, ready.stats, ready.recovered);
            // Sealing compresses and hashes: off the async scheduler.
            let sealed = tokio::task::block_in_place(|| self.seal(&mut run.writer, ready.records))?;
            for chunk in sealed {
                check_link(&chunk.entry, self.parent)?;
                self.parent = Some(chunk.entry.last_hash);
                self.queue(chunk, stored, run).await?;
            }
            run.progress.log(run.uploads.len());
        }
    }

    /// Starts uploading `chunk`, unless the store holds it (`stored`), once at most
    /// `--uploads` minus one are in flight.
    async fn queue(
        &mut self,
        chunk: SealedChunk,
        stored: &HashSet<String>,
        run: &mut Run,
    ) -> eyre::Result<()> {
        run.progress.chunks = run.progress.chunks.saturating_add(1);
        let upload = if stored.contains(&chunk.entry.key()) {
            run.progress.skipped = run.progress.skipped.saturating_add(1);
            (chunk.entry, None)
        } else {
            run.progress.uploaded_bytes =
                run.progress.uploaded_bytes.saturating_add(chunk.entry.size);
            upload(self.store, chunk)
        };
        self.settle(&mut run.uploads, run.keep, &mut run.read)
            .await?;
        run.uploads.push_back(upload);
        Ok(())
    }

    /// Adds `records` to the chunk being written and to the index; returns the chunks they
    /// completed. A chunk ends where §1 ends it, or at the range's last block. Blocking.
    fn seal(
        &mut self,
        writer: &mut ChunkWriter,
        records: Vec<ChunkRecord>,
    ) -> eyre::Result<Vec<SealedChunk>> {
        let mut sealed = Vec::new();
        for record in records {
            let (number, hash) = (record.number(), record.hash());
            self.index.push(hash, number)?;
            if writer
                .push(record)
                .wrap_err_with(|| format!("block {number}"))?
                || number == self.plan.last
            {
                let next = ChunkWriter::new(self.plan.chain, number.saturating_add(1));
                sealed.push(std::mem::replace(writer, next).finish()?);
            }
        }
        Ok(sealed)
    }

    /// Waits for the oldest uploads until at most `keep` are in flight; records each as it
    /// finishes, in chunk order, and deletes the downloaded chunks of `read` the records now
    /// cover.
    async fn settle(
        &mut self,
        uploads: &mut Uploads,
        keep: usize,
        read: &mut VecDeque<Chunk>,
    ) -> eyre::Result<()> {
        while uploads.len() > keep {
            let Some((entry, handle)) = uploads.pop_front() else {
                break;
            };
            if let Some(handle) = handle {
                handle.await??;
            }
            let mut done = Vec::new();
            while let Some(chunk) = read.pop_front_if(|chunk| covered(Some(entry.last), *chunk)) {
                done.push(chunk);
            }
            let state = self.state;
            tokio::task::block_in_place(|| {
                state.write_sealed(&entry)?;
                done.into_iter()
                    .try_for_each(|chunk| state.remove_raw(chunk))
            })
            .wrap_err("failed to record a sealed chunk")?;
            self.next = entry.last.saturating_add(1);
            self.sealed.push(entry);
        }
        Ok(())
    }

    /// Writes the hash index of every listed block as the manifest's next generation.
    pub(super) async fn write_index(self, manifest: &mut Manifest) -> eyre::Result<()> {
        let generation = manifest
            .index_generation()
            .map_or(0, |generation| generation.generation.saturating_add(1));
        info!(generation, "writing the hash index");
        // The builder holds every listed block, so the generation needs no base.
        let written = self
            .index
            .finish(self.store, None, generation, self.plan.last)
            .await
            .wrap_err("failed to write the hash index")?;
        manifest
            .append_generation(self.store, written, EXPORTER)
            .await
            .wrap_err("failed to record the hash index in the manifest")?;
        info!(
            generation = written.generation,
            entries = written.entries,
            chunks = manifest.entries().len(),
            "verify complete: every chunk listed and indexed"
        );
        Ok(())
    }
}

/// What one run of [`Sealing::seal_rest`] keeps between chunks.
struct Run {
    writer: ChunkWriter,
    /// Downloaded chunks read, oldest first, deleted once a record covers them.
    read: VecDeque<Chunk>,
    uploads: Uploads,
    /// Uploads kept in flight while another is queued: `--uploads` minus one.
    keep: usize,
    progress: Progress,
}

/// Uploads in flight, oldest first, with the entry recorded once each finishes; `None` for a
/// chunk the store holds already.
type Uploads = VecDeque<(ChunkEntry, Option<JoinHandle<eyre::Result<()>>>)>;

/// Starts uploading `sealed`.
fn upload(
    store: &ChunkStore,
    sealed: SealedChunk,
) -> (ChunkEntry, Option<JoinHandle<eyre::Result<()>>>) {
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
    (entry, Some(handle))
}

/// The downloaded chunks `chunks`, which must all be on disk, with their sizes. Blocking.
fn raw_sizes(state: &State, chunks: Vec<Chunk>) -> eyre::Result<Vec<(Chunk, u64)>> {
    chunks
        .into_iter()
        .map(|chunk| {
            let path = state.raw_path(chunk);
            match path.metadata() {
                Ok(file) => Ok((chunk, file.len())),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(eyre!(
                    "blocks {} to {} are not downloaded ({}): run `download` first",
                    chunk.from,
                    chunk.to.saturating_sub(1),
                    path.display()
                )),
                Err(err) => Err(err).wrap_err_with(|| format!("failed to read {}", path.display())),
            }
        })
        .collect()
}

/// One downloaded chunk, checked and ready to seal.
struct Ready {
    records: Vec<ChunkRecord>,
    stats: Stats,
    /// Transactions whose sender was recovered from the signature and matched.
    recovered: u64,
}

/// Rebuilds and checks the downloaded chunk at `raw` (with its fill), checks the senders of
/// its blocks from `next` on and prepares their records. Blocking, CPU-bound.
fn prepare(forks: &Forks, chunk: Chunk, raw: &Path, fill: &Path, next: u64) -> eyre::Result<Ready> {
    let (stats, blocks) =
        block::verify_chunk(forks, chunk, raw, fill).map_err(|err| failed(&err, raw))?;
    let mut ready = Ready {
        records: Vec::with_capacity(blocks.len()),
        stats,
        recovered: 0,
    };
    for (number, block) in (chunk.from..).zip(&blocks) {
        if number < next {
            continue;
        }
        ready.recovered = ready
            .recovered
            .saturating_add(check_senders(number, block)?);
        ready
            .records
            .push(ChunkRecord::new(block).wrap_err_with(|| format!("block {number}"))?);
    }
    Ok(ready)
}

/// The error of a downloaded chunk at `raw` that failed, with what to do about it.
fn failed(err: &ChunkError, raw: &Path) -> eyre::Report {
    let hint = match err {
        // A row left without a field says itself what to run.
        ChunkError::Block {
            check: Check::Unfilled { .. } | Check::Hole | Check::MissingMixHash,
            ..
        } => "",
        // A field the rows carry wrong, or one the rebuild defaulted: what the chain's RPC
        // gives for the range may fix it.
        ChunkError::Block {
            check: Check::HeaderHash { .. },
            ..
        } => {
            "; run `download`, which skips the files present and fetches from the chain's RPC \
             what the rows lack (`--rpc-endpoint`); if that does not help, delete the chunk and \
             its fill and download it again"
        }
        ChunkError::Block { .. } | ChunkError::Fill(_) | ChunkError::Rows { .. } => {
            "; delete it, and its fill if there is one, and download again if the data is wrong"
        }
    };
    eyre!("{err} (chunk {}{hint})", raw.display())
}

/// Checks the sender of every transaction of `block` (number `number`), which is the one the
/// archive service reported; returns how many were recovered from a signature. A deposit's
/// must be the `from` in its encoding; a legacy transaction signed with all zeros has none
/// and keeps the recorded one.
///
/// # Errors
///
/// Returns an error naming the block, the transaction's index and both addresses for the
/// first sender that differs, or if a transaction does not decode or has no recoverable
/// sender.
fn check_senders(number: u64, block: &ArchivedBlock) -> eyre::Result<u64> {
    let body = split_body(&block.encoded.body)
        .ok_or_else(|| eyre!("block {number}: the rebuilt body does not decode"))?;
    ensure!(
        body.transactions.len() == block.senders.len(),
        "block {number}: {} senders for {} transactions",
        block.senders.len(),
        body.transactions.len()
    );
    let mut recovered = 0_u64;
    for (index, (leaf, reported)) in body.transactions.iter().zip(&block.senders).enumerate() {
        let transaction = decode_transaction(leaf)
            .map_err(|err| eyre!("block {number}: transaction {index} does not decode: {err}"))?;
        if is_zero_signature(&transaction) {
            continue;
        }
        if !matches!(transaction, OpTxEnvelope::Deposit(_)) {
            recovered = recovered.saturating_add(1);
        }
        let proven = transaction.recover_signer().map_err(|err| {
            eyre!("block {number}: transaction {index} has no recoverable sender: {err}")
        })?;
        ensure!(
            proven == *reported,
            "block {number}: transaction {index} was sent by {proven}, but the archive service \
             reports {reported}. Nothing from this block on is sealed"
        );
    }
    Ok(recovered)
}

/// The step's progress lines, every [`progress::INTERVAL`], with speeds over the last minute
/// and the time left from the downloaded bytes still to read.
struct Progress {
    total_blocks: u64,
    total_raw_bytes: u64,
    raw_bytes: u64,
    done: Stats,
    recovered: u64,
    chunks: u64,
    skipped: u64,
    uploaded_bytes: u64,
    started: Instant,
    logged: Instant,
    blocks_rate: Rate,
    raw_rate: Rate,
    upload_rate: Rate,
}

impl Progress {
    fn new(total_blocks: u64, total_raw_bytes: u64) -> Self {
        Self {
            total_blocks,
            total_raw_bytes,
            raw_bytes: 0,
            done: Stats::default(),
            recovered: 0,
            chunks: 0,
            skipped: 0,
            uploaded_bytes: 0,
            started: Instant::now(),
            logged: Instant::now(),
            blocks_rate: Rate::new(),
            raw_rate: Rate::new(),
            upload_rate: Rate::new(),
        }
    }

    fn read(&mut self, raw_bytes: u64, stats: Stats, recovered: u64) {
        self.raw_bytes = self.raw_bytes.saturating_add(raw_bytes);
        self.done.add(stats);
        self.recovered = self.recovered.saturating_add(recovered);
    }

    fn log(&mut self, uploads_in_flight: usize) {
        if self.logged.elapsed() < progress::INTERVAL {
            return;
        }
        self.logged = Instant::now();
        let raw_per_sec = self.raw_rate.per_sec(self.raw_bytes);
        info!(
            blocks = self.done.blocks,
            of = self.total_blocks,
            transactions = self.done.transactions,
            chunks = self.chunks,
            skipped = self.skipped,
            blocks_per_sec = self.blocks_rate.per_sec(self.done.blocks),
            raw_mb_per_sec = raw_per_sec / 1_000_000,
            upload_mb_per_sec = self.upload_rate.per_sec(self.uploaded_bytes) / 1_000_000,
            uploads_in_flight,
            secs_left = self
                .total_raw_bytes
                .saturating_sub(self.raw_bytes)
                .checked_div(raw_per_sec),
            "verifying"
        );
    }

    fn summary(&self) {
        let secs = self.started.elapsed().as_secs().max(1);
        info!(
            blocks = self.done.blocks,
            transactions = self.done.transactions,
            senders_recovered = self.recovered,
            zero_signature_transactions = self.done.zero_signatures,
            rebuilt_header_fields = self.done.rebuilt_header_fields,
            rpc_filled_transactions = self.done.rpc_filled_transactions,
            chunks = self.chunks,
            skipped = self.skipped,
            uploaded_bytes = self.uploaded_bytes,
            blocks_per_sec = self.done.blocks / secs,
            secs,
            "blocks verified and sealed in this run"
        );
    }
}
