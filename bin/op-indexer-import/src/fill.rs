//! What the archive service leaves out of some rows, fetched from the chain's JSON-RPC
//! ([`Rpc`]) by `download` and kept next to the chunk, so `verify` reads no RPC.
//!
//! Known so far:
//!
//! - EIP-7702 transactions without their authorization list (Unichain). The list is not
//!   optional, so such a row cannot be rebuilt from the download alone.
//! - Holes: blocks whose header row is there but none of their transaction and log rows (ten in
//!   a row at 55,142,810 on Unichain). Every block after the Bedrock block has at least the
//!   L1-attributes deposit, so a block without transactions is one the service left out; the
//!   whole block's transactions and receipts are fetched.
//! - Header fields: Base's rows lack `mix_hash` and `base_fee_per_gas` in large stretches before
//!   block 13.5 M (and may lack a later fork's fields there too). For a block whose header row
//!   lacks any field its forks have, the header is fetched (`eth_getBlockByNumber` without
//!   transactions) and its fields kept.
//!
//! After the chunks are downloaded, every chunk not sealed yet is read, several at once
//! within `verify`'s memory bound ([`IN_FLIGHT_BYTES`]), and checked for every field its rows
//! lack ([`crate::verify::missing`]): all of them are logged in one summary at the end. What a
//! chunk lacks is fetched as soon as the chunk is read, several calls per request (`--rpc-batch`),
//! `--rpc-requests` requests at a time, while the next chunks are read, and written to the
//! chunk's fill (`<from>-<to>.fill.json` in `raw/`, written atomically and durably), in the RPC's
//! form. The downloaded chunk itself is kept as received. A chunk whose fill already holds what
//! it lacks needs nothing, so a run stopped part way resumes where it was. `verify` merges the
//! fill into the rows: a list into its transaction, a hole's transactions and logs as rows of
//! their own, a header field into the header row where it lacks it. A row or block still left
//! out fails there with a message to run `download`.
//!
//! The trust is unchanged: what the fill holds goes into the rebuilt block, and the block's
//! header hash proves it. A hole's senders are the RPC's `from`, which `verify` recovers and
//! checks like every other.

use std::collections::{BTreeMap, VecDeque};
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

use crate::progress::{self, Rate};
use crate::rows::{self, LogRow, Rows, TransactionRow};
use crate::rpc::{FilledBlock, Rpc, RpcHeader, Wanted};
use crate::state::{Chunk, Plan, State, covered, read_json, write_json};
use crate::verify::{Forks, IN_FLIGHT_BYTES, Missing, encode_access_list, holes, missing};

/// What a chunk's rows lack, fetched from the chain's RPC.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Fill {
    /// Transactions whose authorization list the service left out.
    #[serde(default)]
    transactions: Vec<FilledTransaction>,
    /// Blocks whose transactions and logs the service left out.
    #[serde(default)]
    blocks: Vec<FilledBlock>,
    /// Header fields of blocks whose header row lacks some.
    #[serde(default)]
    headers: Vec<RpcHeader>,
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
    for header in fill.headers {
        // Only into a field the row lacks: what the service sent is kept.
        if let Some(row) = rows.block_mut(header.number.to()) {
            row.mix_hash = row.mix_hash.or(header.mix_hash);
            row.base_fee_per_gas = row.base_fee_per_gas.or(header.base_fee_per_gas);
            row.withdrawals_root = row.withdrawals_root.or(header.withdrawals_root);
            row.blob_gas_used = row.blob_gas_used.or(header.blob_gas_used);
            row.excess_blob_gas = row.excess_blob_gas.or(header.excess_blob_gas);
            row.parent_beacon_block_root = row
                .parent_beacon_block_root
                .or(header.parent_beacon_block_root);
        }
    }
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
    /// The blocks whose header fields to fetch.
    headers: Vec<BlockNumHash>,
}

impl Fetch {
    fn is_empty(&self) -> bool {
        self.blocks() == 0
    }

    /// Block reads from the RPC: a block may need two (its list and its header).
    fn blocks(&self) -> usize {
        self.wanted
            .len()
            .saturating_add(self.holes.len())
            .saturating_add(self.headers.len())
    }
}

/// Adds `count` rows lacking `field`, the first in block `first`, to `tally`.
fn tally(tally: &mut Tally, field: Missing, count: u64, first: u64) {
    let entry = tally.entry(field).or_insert((0, first));
    entry.0 = entry.0.saturating_add(count);
    entry.1 = entry.1.min(first);
}

/// Lists every field the rows of the downloaded chunks not sealed yet lack, and fetches what
/// can be fetched (authorization lists, holes, header fields) from `rpc` into the chunks'
/// fills, `requests` requests at a time, while the next chunks are read: up to `threads` at
/// once, within [`IN_FLIGHT_BYTES`] of downloaded bytes. Logs what is missing at the end.
///
/// # Errors
///
/// Returns an error if a chunk cannot be read, something is to fetch and there is no
/// endpoint, a request fails for good, a fill cannot be written, or `cancel` fires.
pub(crate) async fn run(
    state: &State,
    plan: &Plan,
    rpc: Option<&Rpc>,
    requests: usize,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let chunks = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || to_scan(&state, &plan)).await??
    };
    info!(
        chunks = chunks.len(),
        threads,
        endpoint = rpc.map(Rpc::url),
        rpc_batch = rpc.map(Rpc::batch_calls),
        rpc_requests = requests,
        "checking the downloaded rows for missing fields, and fetching them"
    );
    let forks = Forks::new(plan.chain);
    // Chunks read and waiting for their fetch: enough to keep every request busy, few enough
    // that reading, much faster, does not hold the range's needs in memory.
    let backlog = requests.saturating_mul(4);
    let mut work = Work::new(chunks.len(), rpc.is_some());
    let mut queue = chunks.into_iter().peekable();
    let (mut scans, mut fetches) = (JoinSet::new(), JoinSet::new());
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut in_flight_bytes = 0_u64;
    let mut aborted = false;
    loop {
        let go_on = work.failure.is_none() && !cancel.is_cancelled();
        while go_on
            && scans.len() < threads
            && work.to_fetch.len() < backlog
            && let Some((chunk, bytes)) = queue.next_if(|(_, bytes)| {
                scans.is_empty() || in_flight_bytes.saturating_add(*bytes) <= IN_FLIGHT_BYTES
            })
        {
            in_flight_bytes = in_flight_bytes.saturating_add(bytes);
            let (raw, fill) = (state.raw_path(chunk), state.fill_path(chunk));
            scans.spawn_blocking(move || (chunk, bytes, scan(&forks, &raw, &fill)));
        }
        if let Some(rpc) = rpc {
            while go_on
                && fetches.len() < requests
                && let Some((chunk, fetch)) = work.to_fetch.pop_front()
            {
                let (rpc, path) = (rpc.clone(), state.fill_path(chunk));
                fetches.spawn(async move { (chunk, fill_chunk(&rpc, &fetch, path).await) });
            }
        }
        let scanning = (go_on && queue.peek().is_some()) || !scans.is_empty();
        let fetching = !fetches.is_empty() || (go_on && rpc.is_some() && !work.to_fetch.is_empty());
        if !scanning && !fetching {
            break;
        }
        tokio::select! {
            biased;
            // The requests in flight are dropped below, backoffs included; a fill being
            // written is atomic. The chunks being read finish. Then fetches before reads:
            // the endpoint is the bottleneck, and a finished fetch frees its place.
            () = cancel.cancelled(), if !aborted && !fetches.is_empty() => {
                fetches.abort_all();
                aborted = true;
            }
            Some(done) = fetches.join_next(), if !fetches.is_empty() => match done {
                Ok((_, Ok(filled))) => work.fetched(filled),
                Ok((chunk, Err(err))) => {
                    work.failure.get_or_insert_with(|| {
                        format!("blocks {}..{}: {err:#}", chunk.from, chunk.to)
                    });
                }
                // Aborted on cancel: nothing to report.
                Err(err) if err.is_cancelled() => {}
                Err(err) => {
                    work.failure.get_or_insert_with(|| format!("a fetch task failed: {err}"));
                }
            },
            Some(scanned) = scans.join_next(), if !scans.is_empty() => {
                let (chunk, bytes, found) = scanned.wrap_err("a check task failed")?;
                in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
                let found = found.wrap_err_with(|| {
                    format!("blocks {}..{}: failed to read the chunk", chunk.from, chunk.to)
                })?;
                work.scanned(chunk, found);
            }
            _ = tick.tick() => work.log(fetches.len()),
        }
    }
    work.finish(rpc, plan, cancel)
}

/// What a run of [`run`] found and fetched so far.
#[derive(Debug)]
struct Work {
    started: Instant,
    chunks: usize,
    scanned: usize,
    lacking: Tally,
    /// Chunks read with something to fetch, in the order they were read; only counted when
    /// there is no endpoint to fetch from.
    to_fetch: VecDeque<(Chunk, Fetch)>,
    keep: bool,
    /// Block reads from the RPC that scans found, and those done so far.
    queued_blocks: u64,
    fetched_blocks: u64,
    /// Transaction rows the fetched fills added or filled, and headers filled.
    filled_transactions: u64,
    filled_headers: u64,
    scan_rate: Rate,
    fetch_rate: Rate,
    /// The first fetch that failed for good.
    failure: Option<String>,
}

impl Work {
    fn new(chunks: usize, keep: bool) -> Self {
        Self {
            started: Instant::now(),
            chunks,
            scanned: 0,
            lacking: Tally::new(),
            to_fetch: VecDeque::new(),
            keep,
            queued_blocks: 0,
            fetched_blocks: 0,
            filled_transactions: 0,
            filled_headers: 0,
            scan_rate: Rate::new(),
            fetch_rate: Rate::new(),
            failure: None,
        }
    }

    fn scanned(&mut self, chunk: Chunk, found: Scanned) {
        self.scanned = self.scanned.saturating_add(1);
        for (field, (count, first)) in found.missing {
            tally(&mut self.lacking, field, count, first);
        }
        if !found.fetch.is_empty() {
            let blocks = u64::try_from(found.fetch.blocks()).unwrap_or(u64::MAX);
            self.queued_blocks = self.queued_blocks.saturating_add(blocks);
            if self.keep {
                self.to_fetch.push_back((chunk, found.fetch));
            }
        }
    }

    fn fetched(&mut self, fetched: Fetched) {
        self.fetched_blocks = self.fetched_blocks.saturating_add(fetched.blocks);
        self.filled_transactions = self
            .filled_transactions
            .saturating_add(fetched.transactions);
        self.filled_headers = self.filled_headers.saturating_add(fetched.headers);
    }

    /// Logs one progress line: chunks read, and blocks fetched with their speed over the last
    /// minute and the time left for those found so far (more may be found).
    fn log(&mut self, requests_in_flight: usize) {
        let blocks_per_sec = self.fetch_rate.per_sec(self.fetched_blocks);
        info!(
            chunks = self.scanned,
            of = self.chunks,
            chunks_per_sec = self
                .scan_rate
                .per_sec(u64::try_from(self.scanned).unwrap_or(u64::MAX)),
            blocks_fetched = self.fetched_blocks,
            blocks_found = self.queued_blocks,
            blocks_per_sec,
            secs_left = self
                .queued_blocks
                .saturating_sub(self.fetched_blocks)
                .checked_div(blocks_per_sec),
            requests_in_flight,
            "checking and fetching"
        );
    }

    /// Reports the run, and says what went wrong, if anything: a fetch that failed, a stop, or
    /// something to fetch and no endpoint.
    fn finish(
        self,
        rpc: Option<&Rpc>,
        plan: &Plan,
        cancel: &CancellationToken,
    ) -> eyre::Result<()> {
        self.report();
        if let Some(failure) = self.failure {
            eyre::bail!(
                "fetching from {} failed: {failure}; run `download` again, or give another \
                 endpoint with --rpc-endpoint",
                rpc.map_or("the RPC endpoint", Rpc::url)
            );
        }
        eyre::ensure!(
            !cancel.is_cancelled(),
            "stopped while checking the downloaded rows: run `download` again, which goes on \
             where it stopped"
        );
        if rpc.is_none() && self.queued_blocks > 0 {
            eyre::bail!(
                "the archive service left out what {} block reads need (authorization lists, \
                 transactions, header fields), and no RPC endpoint is known for chain {}: give \
                 one with --rpc-endpoint",
                self.queued_blocks,
                plan.chain.chain_id
            );
        }
        Ok(())
    }

    /// Logs what the run found and fetched: one line, then one warning per field missing.
    fn report(&self) {
        info!(
            chunks = self.scanned,
            secs = self.started.elapsed().as_secs(),
            fields_missing = self.lacking.len(),
            blocks_to_fetch = self.queued_blocks,
            blocks_fetched = self.fetched_blocks,
            rpc_filled_transactions = self.filled_transactions,
            rpc_filled_headers = self.filled_headers,
            "downloaded rows checked"
        );
        for (field, (count, first_block)) in &self.lacking {
            warn!(
                row = field.row,
                field = field.field,
                count,
                first_block,
                "downloaded rows lack this field (counted before this run's fills)"
            );
        }
    }
}

/// The chunks downloaded and not sealed yet, with the size of their download. Blocking.
fn to_scan(state: &State, plan: &Plan) -> io::Result<Vec<(Chunk, u64)>> {
    let sealed = state.sealed_through()?;
    let mut chunks = Vec::new();
    for chunk in plan.chunks().filter(|chunk| !covered(sealed, *chunk)) {
        let bytes = match state.raw_path(chunk).metadata() {
            Ok(file) => file.len(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        chunks.push((chunk, bytes));
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
    // Header fields come block by block, in block order.
    let mut headers: Vec<u64> = Vec::new();
    missing(forks, &rows, |field, block| {
        tally(&mut scanned.missing, field, 1, block);
        if field.row == "header" && headers.last() != Some(&block) {
            headers.push(block);
        }
    });
    scanned.fetch.holes = holes(forks, &rows)
        .map(|block| BlockNumHash::new(block.number, block.hash))
        .collect();
    // A hole's block is fetched whole, its header fields with it: but those go to the header
    // row only through `headers`, so a hole lacking them is read for both.
    scanned.fetch.headers = headers
        .into_iter()
        .filter_map(|number| rows.block(number))
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

/// What fetching one chunk's fill added.
#[derive(Debug, Clone, Copy)]
struct Fetched {
    /// Blocks read from the RPC.
    blocks: u64,
    /// Transaction rows it fills or adds.
    transactions: u64,
    /// Headers it fills.
    headers: u64,
}

/// Fetches what one chunk lacks and writes its fill with it added to what it held.
async fn fill_chunk(rpc: &Rpc, fetch: &Fetch, path: PathBuf) -> eyre::Result<Fetched> {
    let mut lists = Vec::new();
    for batch in fetch.wanted.chunks(rpc.batch_calls()) {
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
    for batch in fetch.holes.chunks(rpc.batch_calls() / 2) {
        blocks.extend(rpc.whole_blocks(batch).await?);
    }
    let mut headers = Vec::with_capacity(fetch.headers.len());
    for batch in fetch.headers.chunks(rpc.batch_calls()) {
        headers.extend(rpc.headers(batch).await?);
    }
    let added: usize = blocks.iter().map(|block| block.transactions.len()).sum();
    let fetched = Fetched {
        blocks: u64::try_from(fetch.blocks()).unwrap_or(u64::MAX),
        transactions: u64::try_from(lists.len().saturating_add(added)).unwrap_or(u64::MAX),
        headers: u64::try_from(headers.len()).unwrap_or(u64::MAX),
    };
    tokio::task::spawn_blocking(move || {
        let mut fill = read_json::<Fill>(&path)?.unwrap_or_default();
        fill.transactions.extend(lists);
        fill.blocks.extend(blocks);
        // A header fetched again (one the endpoint gave without a field asked for) replaces
        // the one kept, so the fill does not grow run after run.
        fill.headers
            .retain(|kept| headers.iter().all(|new| new.number != kept.number));
        fill.headers.extend(headers);
        write_json(&path, &fill)
    })
    .await??;
    Ok(fetched)
}
