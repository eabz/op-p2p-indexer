//! What the archive service leaves out of some rows, fetched from the chain's JSON-RPC
//! ([`Rpc`]) by `download` and kept next to the chunk, so `verify` stays offline.
//!
//! Known so far, on Unichain (OP Mainnet's whole chain needed nothing):
//!
//! - EIP-7702 transactions without their authorization list. The list is not optional, so such
//!   a row cannot be rebuilt from the download alone.
//! - Holes: blocks whose header row is there but none of their transaction and log rows (ten in
//!   a row at 55,142,810). Every block after the Bedrock block has at least the L1-attributes
//!   deposit, so a block without transactions is one the service left out; the whole block's
//!   transactions and receipts are fetched.
//!
//! After the chunks are downloaded, every chunk not verified yet is read, several at once
//! within `verify`'s memory bound ([`IN_FLIGHT_BYTES`]), and
//! checked for every field its rows lack ([`crate::verify::missing`]): all of them are logged
//! in one summary, so they can be dealt with together rather than one `verify` failure at a
//! time. What is missing is fetched, several calls per request ([`BATCH_CALLS`]), a few
//! requests at a time, and written to the chunk's fill (`<from>-<to>.fill.json` in `raw/`,
//! written atomically and durably), in the RPC's form. The downloaded chunk itself is kept as
//! received. `verify` merges the fill into the rows: a list into its transaction, a hole's
//! transactions and logs as rows of their own. A row or block still left out fails there with
//! a message to run `download`.
//!
//! The trust is unchanged: what the fill holds goes into the rebuilt block, and the block's
//! header hash proves it. A hole's senders are the RPC's `from`, which `load` recovers and
//! checks like every other.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use alloy_eips::BlockNumHash;
use alloy_eips::eip7702::SignedAuthorization;
use eyre::WrapErr;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chunk::{self, ChunkFile};
use crate::progress::{self, Rate};
use crate::rows::{self, LogRow, Rows, TransactionRow};
use crate::rpc::{BATCH_CALLS, FilledBlock, Rpc, Wanted};
use crate::state::{Chunk, Plan, State, read_json, write_json};
use crate::verify::{Forks, IN_FLIGHT_BYTES, Missing, encode_access_list, holes, missing};

/// Requests to the RPC endpoint in flight at once: it is a public endpoint, used politely.
const RPC_REQUESTS: usize = 4;

/// What a chunk's rows lack, fetched from the chain's RPC.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Fill {
    /// Transactions whose authorization list the service left out.
    #[serde(default)]
    transactions: Vec<FilledTransaction>,
    /// Blocks whose transactions and logs the service left out.
    #[serde(default)]
    blocks: Vec<FilledBlock>,
}

/// One transaction's authorization list, in the RPC's form.
#[derive(Debug, Serialize, Deserialize)]
struct FilledTransaction {
    block_number: u64,
    transaction_index: u64,
    authorization_list: Vec<SignedAuthorization>,
}

/// Puts what `fill` holds into the rows it is for. Returns how many transaction rows it
/// filled or added.
pub(crate) fn apply(rows: &mut Rows, fill: Fill) -> u64 {
    let mut filled = 0_u64;
    let mut added = false;
    for block in fill.blocks {
        // Only into a hole: rows the service sent are not doubled.
        if rows.block(block.number).is_none() || rows.has_transactions(block.number) {
            continue;
        }
        let (transactions, logs) = hole_rows(block);
        filled = filled.saturating_add(u64::try_from(transactions.len()).unwrap_or(u64::MAX));
        rows.transactions.extend(transactions);
        rows.logs.extend(logs);
        added = true;
    }
    if added {
        rows.sort();
    }
    for transaction in fill.transactions {
        let key = (transaction.block_number, transaction.transaction_index);
        let at = rows
            .transactions
            .binary_search_by_key(&key, |row| (row.block_number, row.transaction_index));
        if let Some(row) = at.ok().and_then(|at| rows.transactions.get_mut(at)) {
            row.filled_authorization_list = Some(transaction.authorization_list);
            filled = filled.saturating_add(1);
        }
    }
    filled
}

/// The rows of a block filled from the RPC, as the service would have sent them: the receipt's
/// fields with the transaction's, the access list in the service's layout, the authorization
/// list as filled.
fn hole_rows(block: FilledBlock) -> (Vec<TransactionRow>, Vec<LogRow>) {
    let number = block.number;
    let mut transactions = Vec::with_capacity(block.transactions.len());
    let mut logs = Vec::new();
    for (tx, receipt) in block.transactions.into_iter().zip(block.receipts) {
        let transaction_index = tx.transaction_index.to();
        logs.extend(receipt.logs.into_iter().map(|log| {
            let topic = |at: usize| log.topics.get(at).copied();
            LogRow {
                block_number: number,
                transaction_index,
                log_index: log.log_index.to(),
                address: log.address,
                data: log.data,
                topic0: topic(0),
                topic1: topic(1),
                topic2: topic(2),
                topic3: topic(3),
            }
        }));
        transactions.push(TransactionRow {
            block_number: number,
            transaction_index,
            from: Some(tx.from),
            to: tx.to,
            gas: tx.gas,
            gas_price: tx.gas_price,
            input: tx.input,
            value: tx.value,
            nonce: tx.nonce,
            v: tx.v,
            r: tx.r,
            s: tx.s,
            kind: Some(tx.kind.to()),
            status: receipt.status.map(|status| status.to()),
            root: receipt.root,
            cumulative_gas_used: receipt.cumulative_gas_used,
            chain_id: tx.chain_id,
            max_fee_per_gas: tx.max_fee_per_gas,
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas,
            y_parity: tx.y_parity,
            access_list: tx
                .access_list
                .as_ref()
                .map(|list| encode_access_list(list).into()),
            authorization_list: None,
            filled_authorization_list: tx.authorization_list,
            source_hash: tx.source_hash,
            mint: tx.mint,
            deposit_nonce: receipt.deposit_nonce,
            deposit_receipt_version: receipt.deposit_receipt_version,
        });
    }
    (transactions, logs)
}

/// Each field missing, how often, and the first block lacking it.
type Tally = BTreeMap<Missing, (u64, u64)>;

/// What the scan found in one chunk.
#[derive(Debug, Default)]
struct Scanned {
    missing: Tally,
    fetch: Fetch,
}

/// What one chunk needs from the RPC.
#[derive(Debug, Default)]
struct Fetch {
    /// The blocks with authorization lists to fetch.
    wanted: Vec<Wanted>,
    /// The blocks to fetch whole.
    holes: Vec<BlockNumHash>,
}

impl Fetch {
    fn is_empty(&self) -> bool {
        self.wanted.is_empty() && self.holes.is_empty()
    }
}

/// Adds `count` rows lacking `field`, the first in block `first`, to `tally`.
fn tally(tally: &mut Tally, field: Missing, count: u64, first: u64) {
    let entry = tally.entry(field).or_insert((0, first));
    entry.0 = entry.0.saturating_add(count);
    entry.1 = entry.1.min(first);
}

/// Lists every field the rows of the downloaded chunks not verified yet lack, logs them, and
/// fetches what can be fetched (authorization lists, holes) from `rpc` into the chunks' fills.
/// Up to `threads`
/// chunks are read at once, within [`IN_FLIGHT_BYTES`] of downloaded bytes.
///
/// # Errors
///
/// Returns an error if a chunk cannot be read, lists are missing and there is no endpoint, a
/// request fails for good, or a fill cannot be written.
pub(crate) async fn run(
    state: &State,
    plan: &Plan,
    rpc: Option<&Rpc>,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let chunks = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || to_scan(&state, &plan)).await??
    };
    let started = Instant::now();
    info!(
        chunks = chunks.len(),
        threads, "checking the downloaded rows for missing fields"
    );
    let forks = Forks::new(plan.chain);
    let total = chunks.len();
    let mut queue = chunks.into_iter().peekable();
    let mut tasks = JoinSet::new();
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rate = Rate::new();
    let mut scanned = 0_usize;
    let mut in_flight_bytes = 0_u64;
    let mut lacking = Tally::new();
    let mut to_fetch: Vec<(Chunk, Fetch)> = Vec::new();
    loop {
        while !cancel.is_cancelled()
            && tasks.len() < threads
            && let Some((chunk, bytes)) = queue.next_if(|(_, bytes)| {
                tasks.is_empty() || in_flight_bytes.saturating_add(*bytes) <= IN_FLIGHT_BYTES
            })
        {
            in_flight_bytes = in_flight_bytes.saturating_add(bytes);
            let (raw, fill) = (state.raw_path(chunk), state.fill_path(chunk));
            tasks.spawn_blocking(move || (chunk, bytes, scan(&forks, &raw, &fill)));
        }
        tokio::select! {
            finished = tasks.join_next() => match finished {
                Some(Ok((chunk, bytes, found))) => {
                    in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
                    let found = found.wrap_err_with(|| {
                        format!("blocks {}..{}: failed to read the chunk", chunk.from, chunk.to)
                    })?;
                    scanned = scanned.saturating_add(1);
                    for (field, (count, first)) in found.missing {
                        tally(&mut lacking, field, count, first);
                    }
                    if !found.fetch.is_empty() {
                        to_fetch.push((chunk, found.fetch));
                    }
                }
                Some(Err(err)) => return Err(err).wrap_err("a check task failed"),
                None => break,
            },
            _ = tick.tick() => {
                let per_sec = rate.per_sec(u64::try_from(scanned).unwrap_or(u64::MAX));
                let left = u64::try_from(total.saturating_sub(scanned)).unwrap_or(u64::MAX);
                info!(
                    chunks = scanned,
                    of = total,
                    chunks_per_sec = per_sec,
                    secs_left = left.checked_div(per_sec),
                    "checking"
                );
            }
        }
    }
    eyre::ensure!(
        !cancel.is_cancelled(),
        "stopped while checking the downloaded rows: run `download` again"
    );
    let lists: usize = to_fetch
        .iter()
        .flat_map(|(_, fetch)| &fetch.wanted)
        .map(|block| block.indexes.len())
        .sum();
    let holes: usize = to_fetch.iter().map(|(_, fetch)| fetch.holes.len()).sum();
    report(scanned, started, &lacking, lists, holes);
    if to_fetch.is_empty() {
        return Ok(());
    }
    let rpc = rpc.ok_or_else(|| {
        eyre::eyre!(
            "the archive service left out {lists} EIP-7702 authorization lists and {holes} \
             blocks' transactions, and no RPC endpoint is known for chain {}: give one with \
             --rpc-endpoint",
            plan.chain.chain_id
        )
    })?;
    fetch(state, rpc, to_fetch, cancel).await
}

/// Logs what the scan found: one line, then one warning per field missing.
fn report(chunks: usize, started: Instant, lacking: &Tally, lists: usize, holes: usize) {
    info!(
        chunks,
        secs = started.elapsed().as_secs(),
        fields_missing = lacking.len(),
        authorization_lists_to_fetch = lists,
        holes_to_fetch = holes,
        "downloaded rows checked"
    );
    for (field, (count, first_block)) in lacking {
        warn!(
            row = field.row,
            field = field.field,
            count,
            first_block,
            "downloaded rows lack this field"
        );
    }
}

/// The chunks downloaded and not verified yet, with the size of their download. Blocking.
fn to_scan(state: &State, plan: &Plan) -> io::Result<Vec<(Chunk, u64)>> {
    let mut chunks = Vec::new();
    for chunk in plan.chunks() {
        let bytes = match state.raw_path(chunk).metadata() {
            Ok(file) => file.len(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        if chunk::check(&state.verified_path(chunk))? != ChunkFile::Present {
            chunks.push((chunk, bytes));
        }
    }
    Ok(chunks)
}

/// Reads one downloaded chunk with its fill and lists what its rows lack. Blocking.
fn scan(forks: &Forks, raw: &Path, fill: &Path) -> eyre::Result<Scanned> {
    let mut rows = rows::read(raw)?;
    if let Some(fill) = read_json(fill)? {
        apply(&mut rows, fill);
    }
    let mut scanned = Scanned::default();
    missing(forks, &rows, |field, block| {
        tally(&mut scanned.missing, field, 1, block);
    });
    scanned.fetch.holes = holes(forks, &rows)
        .map(|block| BlockNumHash::new(block.number, block.hash))
        .collect();
    let mut lists: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for tx in rows
        .transactions
        .iter()
        .filter(|tx| tx.lacks_authorization_list())
    {
        lists
            .entry(tx.block_number)
            .or_default()
            .push(tx.transaction_index);
    }
    for (number, indexes) in lists {
        // A block the chunk lacks fails `verify` on its own; nothing to fetch for it.
        if let Some(block) = rows.block(number) {
            scanned.fetch.wanted.push(Wanted {
                number,
                hash: block.hash,
                indexes,
            });
        }
    }
    Ok(scanned)
}

/// Fetches what `to_fetch` lists, the chunks [`RPC_REQUESTS`] at a time (a chunk's calls
/// [`BATCH_CALLS`] per request), and adds it to each chunk's fill.
async fn fetch(
    state: &State,
    rpc: &Rpc,
    to_fetch: Vec<(Chunk, Fetch)>,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let started = Instant::now();
    info!(
        chunks = to_fetch.len(),
        endpoint = rpc.url(),
        requests = RPC_REQUESTS,
        "fetching what the archive service left out"
    );
    let mut queue = to_fetch.into_iter();
    let mut tasks = JoinSet::new();
    let mut filled = 0_u64;
    let mut failure = None;
    loop {
        while failure.is_none()
            && !cancel.is_cancelled()
            && tasks.len() < RPC_REQUESTS
            && let Some((chunk, blocks)) = queue.next()
        {
            let (rpc, path) = (rpc.clone(), state.fill_path(chunk));
            tasks.spawn(async move { (chunk, fill_chunk(&rpc, &blocks, path).await) });
        }
        tokio::select! {
            biased;
            // The requests in flight are dropped below, backoffs included; a fill being
            // written is atomic.
            () = cancel.cancelled() => break,
            finished = tasks.join_next() => match finished {
                Some(Ok((_, Ok(count)))) => filled = filled.saturating_add(count),
                Some(Ok((chunk, Err(err)))) => {
                    failure.get_or_insert_with(|| {
                        format!("blocks {}..{}: {err:#}", chunk.from, chunk.to)
                    });
                }
                Some(Err(err)) => {
                    failure.get_or_insert_with(|| format!("a fetch task failed: {err}"));
                }
                None => break,
            },
        }
    }
    tasks.shutdown().await;
    info!(
        rpc_filled_transactions = filled,
        secs = started.elapsed().as_secs(),
        "fetched what the archive service left out"
    );
    if let Some(failure) = failure {
        eyre::bail!(
            "fetching from {} failed: {failure}; run `download` again, or give another endpoint \
             with --rpc-endpoint",
            rpc.url()
        );
    }
    eyre::ensure!(
        !cancel.is_cancelled(),
        "stopped while fetching from the RPC endpoint: run `download` again"
    );
    Ok(())
}

/// Fetches what one chunk lacks and writes its fill with it added to what it held. Returns how
/// many transactions it filled or added.
async fn fill_chunk(rpc: &Rpc, fetch: &Fetch, path: PathBuf) -> eyre::Result<u64> {
    let mut lists = Vec::new();
    for batch in fetch.wanted.chunks(BATCH_CALLS) {
        let fetched = rpc.authorization_lists(batch).await?;
        let wanted = batch.iter().flat_map(|block| {
            block
                .indexes
                .iter()
                .map(move |&index| (block.number, index))
        });
        lists.extend(
            wanted
                .zip(fetched)
                .map(|((number, index), list)| FilledTransaction {
                    block_number: number,
                    transaction_index: index,
                    authorization_list: list,
                }),
        );
    }
    let mut blocks = Vec::new();
    // Two calls a block: the block and its receipts.
    for batch in fetch.holes.chunks(BATCH_CALLS / 2) {
        blocks.extend(rpc.whole_blocks(batch).await?);
    }
    let added: usize = blocks.iter().map(|block| block.transactions.len()).sum();
    let count = u64::try_from(lists.len().saturating_add(added)).unwrap_or(u64::MAX);
    tokio::task::spawn_blocking(move || {
        let mut fill = read_json::<Fill>(&path)?.unwrap_or_default();
        fill.transactions.extend(lists);
        fill.blocks.extend(blocks);
        write_json(&path, &fill)
    })
    .await??;
    Ok(count)
}
