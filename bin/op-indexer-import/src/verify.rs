//! The `verify` step: checks every downloaded block, seals the range into chunks and uploads
//! them to object storage, where servers read history from (`docs/serving.md` §1, §2).
//!
//! ```text
//! <state>/raw ─▶ rebuilt and checked ─▶ ChunkWriter ─▶ R2 (or --to-dir) ─▶ <state>/sealed record
//!                (every core: `block`,   (in order)    the chunk object    and the raw chunks
//!                 senders: `seal`)                                          it covers deleted
//! at the end: the last block against the anchor ─▶ manifest ─▶ hash index
//! ```
//!
//! What is proven for every block: its header hashes to its block hash and links to its
//! parent up to the anchor; its transactions root is the root over the stored transaction
//! encodings; its receipts root is the root over the stored receipts; the sender of every
//! signed transaction is the one recovered from its signature, and a deposit's is its `from`.
//! A transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client before
//! Bedrock) has no signer: the zero address is kept, unproven, and counted.
//!
//! Trust flows down from the anchor, so a chunk is uploaded as soon as it is sealed but listed
//! in the manifest, where servers find it, only once the whole range is sealed and its last
//! block matches the anchor. The links are checked as the chunks are sealed: inside a chunk by
//! the writer, between chunks by each one's first parent against the last hash before it, and
//! the first against the manifest's last chunk when the range continues a listed one. A chunk
//! uploaded but never listed is read by nobody; its name carries its root.
//!
//! Resumable: `<state>/sealed` records each uploaded chunk, in block order, and a run goes on
//! after the last record with a fresh writer, which cuts the same chunks an unbroken run would
//! (the writer starts afresh at every chunk). A downloaded chunk is deleted once the records
//! cover it whole, so the disk holds no copy of the range: besides the downloaded chunks left,
//! the hash index being built (16 bytes per block) and the chunks in memory.

mod block;
mod fields;
mod lists;
mod receipt;
mod seal;
mod transaction;

use alloy_primitives::B256;
use eyre::{WrapErr, ensure};
use op_indexer_chainspec::ChainSpec;
use op_indexer_chunks::{ChunkEntry, Manifest};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub(crate) use self::fields::{Missing, holes, missing};
pub(crate) use self::lists::encode_access_list;
use crate::cli::VerifyArgs;
use crate::rows::RowsError;
use crate::state::{Chunk, MIN_SPACE_BYTES, Plan, State, covered};

/// Compressed size of the downloaded chunks `verify` (and `fill`) reads at once, whatever the
/// number of threads; one chunk is always allowed. A chunk takes about twenty times its
/// compressed size while it is checked (its text, then its rows and blocks), so this bounds it
/// to roughly 5 GB.
pub(crate) const IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;

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
    #[error(
        "transaction {index} lacks the field `{field}`, which the archive service left out: run \
         `download`, which fetches it from the chain's RPC endpoint"
    )]
    Unfilled { index: u64, field: &'static str },
    #[error(
        "the download has no transactions for this block, which every block after Bedrock has \
         (the L1-attributes deposit): the archive service left them out; run `download`, which \
         fetches the block from the chain's RPC endpoint"
    )]
    Hole,
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
    #[error(
        "the header lacks `mix_hash`, which a block after the Bedrock block must have, and the \
         archive service left out: run `download`, which fetches the header's missing fields \
         from the chain's RPC endpoint"
    )]
    MissingMixHash,
}

/// Why a chunk was not verified.
#[derive(Debug, thiserror::Error)]
enum ChunkError {
    #[error("failed to read the chunk's fill: {0}")]
    Fill(std::io::Error),
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
pub(crate) struct Forks {
    /// Bedrock: the first block in the current format. Before it a header's `mix_hash` is
    /// zero, so a row without it can be rebuilt.
    bedrock_block: u64,
    /// Regolith: the L1-attributes deposit stops being a system transaction.
    regolith: u64,
    /// Canyon: the deposit nonce and receipt version become part of the hashed receipt.
    canyon: u64,
    /// Ecotone: the header carries the blob gas fields and the parent beacon block root.
    ecotone: u64,
    /// Isthmus: the header carries the hash of an empty requests list.
    isthmus: u64,
}

impl Forks {
    pub(crate) const fn new(chain: &ChainSpec) -> Self {
        Self {
            bedrock_block: chain.bedrock_block,
            regolith: chain.regolith_time(),
            canyon: chain.canyon_time(),
            ecotone: chain.ecotone_time(),
            isthmus: chain.isthmus_time(),
        }
    }
}

/// What the verified blocks held.
#[derive(Debug, Clone, Copy, Default)]
struct Stats {
    blocks: u64,
    transactions: u64,
    /// Transactions signed with all zeros.
    zero_signatures: u64,
    /// Pre-Bedrock blocks whose row lacked `mix_hash`, rebuilt with zero and proven by the
    /// header hash.
    rebuilt_header_fields: u64,
    /// Transactions with a field the service left out, taken from the chunk's fill (the
    /// chain's RPC) and proven by the header hash.
    rpc_filled_transactions: u64,
}

impl Stats {
    fn add(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.zero_signatures = self.zero_signatures.saturating_add(other.zero_signatures);
        self.rebuilt_header_fields = self
            .rebuilt_header_fields
            .saturating_add(other.rebuilt_header_fields);
        self.rpc_filled_transactions = self
            .rpc_filled_transactions
            .saturating_add(other.rpc_filled_transactions);
    }
}

/// Rebuilds the downloaded chunk at `raw` with its fill at `fill` and then `overlay` as
/// `verify` does, and says why it does not rebuild to its hashes, if it does not: `fill`'s
/// check of the header fields it rebuilt, before it writes them. Blocking, CPU-bound.
pub(crate) fn rebuild_error(
    forks: &Forks,
    chunk: Chunk,
    raw: &std::path::Path,
    fill: &std::path::Path,
    overlay: crate::fill::Fill,
) -> Option<String> {
    block::verify_chunk(forks, chunk, raw, fill, Some(overlay))
        .err()
        .map(|err| err.to_string())
}

/// Verifies, seals and uploads the blocks of `plan` the store does not list yet, then, once
/// the last block matches the anchor, lists the chunks in the manifest and writes the hash
/// index.
///
/// # Errors
///
/// Returns an error naming the block and the check if a block fails, and an error if a chunk
/// is not downloaded, the target or its credentials are missing, the store fails, the last
/// block does not match the anchor (nothing is listed), or `cancel` fires (run again to go on).
pub(crate) async fn run(
    args: &VerifyArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    ensure!(!cancel.is_cancelled(), "stopped before `verify` began");
    let store = seal::open_store(args, plan)?;
    let mut manifest = Manifest::load(&store)
        .await
        .wrap_err("failed to read the manifest")?;
    let listed_through = manifest.last().map(|entry| entry.last);
    if listed_through.is_some_and(|last| last >= plan.last)
        && manifest
            .index_generation()
            .is_some_and(|generation| generation.through_block >= plan.last)
    {
        info!(
            chunks = manifest.entries().len(),
            "the range is already sealed, listed and indexed"
        );
        return Ok(());
    }
    let start = listed_through.map_or(plan.first, |last| last.saturating_add(1));
    let (sealed, free_bytes) = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || prepare_state(&state, &plan, start)).await??
    };
    seal::check_continues(manifest.last(), start, &sealed)?;
    info!(
        first = start,
        last = plan.last,
        sealed_before = sealed.len(),
        free_bytes,
        "verify starting"
    );
    ensure!(
        free_bytes.is_none_or(|free| free >= MIN_SPACE_BYTES),
        "less than {} GiB free on the state directory's disk, which the hash index needs",
        MIN_SPACE_BYTES >> 30
    );

    let mut sealing =
        seal::Sealing::new(args, state, plan, &store, manifest.entries(), sealed).await?;
    sealing.seal_rest(cancel).await?;
    let sealed = sealing.sealed();
    if let Some(last) = sealed.last() {
        seal::check_anchor(&store, plan, last).await?;
        info!(
            last = plan.last,
            anchor = %plan.anchor,
            "the range is proven up to the anchor: listing its chunks"
        );
        for entries in sealed.chunks(seal::CHUNKS_PER_SEGMENT) {
            manifest
                .append(&store, entries.to_vec(), seal::EXPORTER)
                .await
                .wrap_err("failed to list the sealed chunks in the manifest")?;
        }
    }
    sealing.write_index(&mut manifest).await
}

/// Reads the records of the chunks sealed from `start` on, deletes the downloaded chunks they
/// cover (a run stopped between the two), and returns them with the free space. Blocking.
fn prepare_state(
    state: &State,
    plan: &Plan,
    start: u64,
) -> std::io::Result<(Vec<ChunkEntry>, Option<u64>)> {
    let mut sealed = state.read_sealed()?;
    let through = sealed.last().map(|entry| entry.last);
    for chunk in plan.chunks().filter(|chunk| covered(through, *chunk)) {
        state.remove_raw(chunk)?;
    }
    sealed.retain(|entry| entry.first >= start);
    Ok((sealed, state.free_bytes()?))
}
