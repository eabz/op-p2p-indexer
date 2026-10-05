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
//!   lacks any field its forks have, the fields are rebuilt from L1 and the parent block
//!   (`derive`, `--headers-from l1`, the default), and what cannot be rebuilt or does not hash
//!   is fetched (`eth_getBlockByNumber` without transactions); `--headers-from rpc` fetches
//!   them all.
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

mod derive;

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use alloy_eips::BlockNumHash;
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{B256, U128};
use eyre::WrapErr;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use self::derive::{DepositRow, HeaderRow, L1Data, L1Info, Parent};
use crate::progress::{self, Rate};
use crate::rows::{self, LogRow, Rows, TransactionRow};
use crate::rpc::{FilledBlock, Rpc, RpcHeader, Wanted};
use crate::source::HyperSync;
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
    /// Source hashes of deposits whose row lacks it.
    #[serde(default)]
    sources: Vec<FilledSource>,
}

impl Fill {
    /// A fill of what was rebuilt from L1: checked on top of the chunk's fill before it is
    /// added to it ([`Self::add`]).
    fn rebuilt(headers: Vec<RpcHeader>, sources: &[derive::Source]) -> Self {
        Self {
            headers,
            sources: sources
                .iter()
                .map(|source| FilledSource {
                    block_number: source.number,
                    transaction_index: source.index,
                    source_hash: source.hash,
                    mint: source.mint,
                })
                .collect(),
            ..Self::default()
        }
    }

    /// Adds what `other` holds of header fields and source hashes, replacing what it held for
    /// the same blocks and deposits.
    fn add(&mut self, other: Self) {
        self.put_headers(other.headers);
        self.sources.retain(|kept| {
            other.sources.iter().all(|new| {
                (new.block_number, new.transaction_index)
                    != (kept.block_number, kept.transaction_index)
            })
        });
        self.sources.extend(other.sources);
    }

    /// Adds `headers`, replacing what it held for the same blocks: a header fetched again (one
    /// the endpoint gave without a field asked for) replaces the one kept, so the fill does not
    /// grow run after run.
    fn put_headers(&mut self, headers: Vec<RpcHeader>) {
        self.headers
            .retain(|kept| headers.iter().all(|new| new.number != kept.number));
        self.headers.extend(headers);
    }
}

/// What a deposit's row lacks of its source hash and its mint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilledSource {
    block_number: u64,
    transaction_index: u64,
    source_hash: Option<B256>,
    mint: Option<U128>,
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
    for source in fill.sources {
        let key = (source.block_number, source.transaction_index);
        let at = rows
            .transactions
            .binary_search_by_key(&key, |row| (row.block_number, row.transaction_index));
        // Only into a row that lacks it: what the service sent is kept.
        if let Some(row) = at.ok().and_then(|at| rows.transactions.get_mut(at)) {
            let before = (row.source_hash, row.mint);
            row.source_hash = row.source_hash.or(source.source_hash);
            row.mint = row.mint.or(source.mint);
            if (row.source_hash, row.mint) != before {
                filled = filled.saturating_add(1);
            }
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
    /// Every block's header row, for the rebuild from L1 (none when the headers come from
    /// the RPC).
    header_rows: Vec<HeaderRow>,
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
    /// The blocks with deposits whose source hash to fetch; their header fields come with
    /// them.
    sources: Vec<Wanted>,
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
            .saturating_add(self.sources.len())
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
    l1: Option<&HyperSync>,
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
        fill_from = if l1.is_some() { "l1" } else { "rpc" },
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
    // The rebuild from L1 takes the chunks in block order; they are read in that order, and
    // those read early wait here for the one before.
    let mut order: VecDeque<u64> = chunks.iter().map(|(chunk, _)| chunk.from).collect();
    let mut ready: BTreeMap<u64, (Chunk, u64, Scanned)> = BTreeMap::new();
    let mut rebuilding = l1.map(|l1| Rebuilding::new(l1, rpc, plan.chain));
    let mut queue = chunks.into_iter().peekable();
    let (mut scans, mut fetches) = (JoinSet::new(), JoinSet::new());
    let mut checks: JoinSet<Check> = JoinSet::new();
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut in_flight_bytes = 0_u64;
    let mut aborted = false;
    loop {
        let go_on = work.failure.is_none() && !cancel.is_cancelled();
        while go_on
            && scans.len().saturating_add(checks.len()) < threads
            && work.to_fetch.len() < backlog
            && let Some((chunk, bytes)) = queue.next_if(|(_, bytes)| {
                scans.is_empty() || in_flight_bytes.saturating_add(*bytes) <= IN_FLIGHT_BYTES
            })
        {
            in_flight_bytes = in_flight_bytes.saturating_add(bytes);
            let (raw, fill) = (state.raw_path(chunk), state.fill_path(chunk));
            let rebuild = rebuilding.is_some();
            scans.spawn_blocking(move || (chunk, bytes, scan(&forks, &raw, &fill, rebuild)));
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
        if !scanning && !fetching && checks.is_empty() {
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
            Some(done) = fetches.join_next(), if !fetches.is_empty() => work.fetch_done(rpc, done),
            Some(checked) = checks.join_next(), if !checks.is_empty() => {
                let bytes = take_check(state, &mut work, checked.wrap_err("a check task failed")?)?;
                in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
            }
            Some(scanned) = scans.join_next(), if !scans.is_empty() => {
                let (chunk, bytes, found) = scanned.wrap_err("a read task failed")?;
                in_flight_bytes = in_flight_bytes.saturating_sub(bytes);
                let found = found.wrap_err_with(|| {
                    format!("blocks {}..{}: failed to read the chunk", chunk.from, chunk.to)
                })?;
                ready.insert(chunk.from, (chunk, bytes, found));
                // Not after a stop or a failure: the run is ending.
                if work.failure.is_none() && !cancel.is_cancelled() {
                    let mut ordered = Ordered {
                        order: &mut order,
                        ready: &mut ready,
                        rebuilding: rebuilding.as_mut(),
                        checks: &mut checks,
                        in_flight_bytes: &mut in_flight_bytes,
                    };
                    ordered.take(state, forks, &mut work).await?;
                }
            }
            _ = tick.tick() => work.log(fetches.len()),
        }
    }
    work.finish(rpc, plan, cancel)
}

/// A running check of what was rebuilt of a chunk: the chunk, its downloaded bytes (in
/// flight while it runs), the rebuilt fill, what to fetch instead if it does not hash, and why
/// it does not, if so.
type Check = (Chunk, u64, Fill, Fetch, Option<String>);

/// Takes a finished check: writes the rebuilt fill if every block hashes, else has the chunk's
/// fields fetched. Returns the chunk's downloaded bytes, no longer in flight.
fn take_check(state: &State, work: &mut Work, check: Check) -> eyre::Result<u64> {
    let (chunk, bytes, rebuilt, instead, why) = check;
    match why {
        // Every block hashes: only now are the fields written.
        None => {
            let path = state.fill_path(chunk);
            work.rebuilt(&rebuilt);
            tokio::task::block_in_place(|| add_rebuilt(&path, rebuilt))
                .wrap_err_with(|| format!("failed to write {}", path.display()))?;
        }
        Some(why) => work.unrebuilt(chunk, instead, &why),
    }
    Ok(bytes)
}

/// The chunks read, waiting to be taken in block order.
struct Ordered<'a, 'l> {
    /// The chunks still to take, by first block, in block order.
    order: &'a mut VecDeque<u64>,
    /// The chunks read and not taken yet, by first block, with their downloaded bytes.
    ready: &'a mut BTreeMap<u64, (Chunk, u64, Scanned)>,
    rebuilding: Option<&'a mut Rebuilding<'l>>,
    checks: &'a mut JoinSet<Check>,
    /// Downloaded bytes being read or checked, for the memory bound.
    in_flight_bytes: &'a mut u64,
}

impl Ordered<'_, '_> {
    /// Takes the chunks read, in block order, as far as they go without a gap: rebuilds their
    /// header fields from L1 (starting the check of those rebuilt) and hands them to `work`.
    async fn take(&mut self, state: &State, forks: Forks, work: &mut Work) -> eyre::Result<()> {
        while let Some((chunk, bytes, mut found)) =
            self.order.front().and_then(|from| self.ready.remove(from))
        {
            self.order.pop_front();
            if let Some(rebuilding) = self.rebuilding.as_deref_mut()
                && let Some((rebuilt, instead, sources)) =
                    rebuilding.chunk(&mut found, work).await?
            {
                *self.in_flight_bytes = self.in_flight_bytes.saturating_add(bytes);
                let (raw, fill) = (state.raw_path(chunk), state.fill_path(chunk));
                self.checks.spawn_blocking(move || {
                    let overlay = Fill {
                        headers: rebuilt.headers.clone(),
                        sources: rebuilt.sources.clone(),
                        ..Fill::default()
                    };
                    let why = crate::verify::rebuild_error(&forks, chunk, &raw, &fill, overlay)
                        .map(|(block, why)| match block {
                            Some(block) => {
                                let rebuilt = derive::describe(block, &rebuilt.headers, &sources);
                                format!("{why}; {rebuilt}")
                            }
                            None => why,
                        });
                    (chunk, bytes, rebuilt, instead, why)
                });
            }
            work.scanned(chunk, found);
        }
        Ok(())
    }
}

/// The rebuild from L1, chunk after chunk in block order.
struct Rebuilding<'a> {
    l1: &'a HyperSync,
    /// For the parent of a run's first block: one header.
    rpc: Option<&'a Rpc>,
    chain: &'static op_indexer_chainspec::ChainSpec,
    data: L1Data,
    /// The block before the next chunk's first, for its base fee.
    parent: Option<Parent>,
}

impl<'a> Rebuilding<'a> {
    fn new(
        l1: &'a HyperSync,
        rpc: Option<&'a Rpc>,
        chain: &'static op_indexer_chainspec::ChainSpec,
    ) -> Self {
        Self {
            l1,
            rpc,
            chain,
            data: L1Data::default(),
            parent: None,
        }
    }

    /// Rebuilds what the rows of `found` lack, and returns it as a fill, to check before it is
    /// written, with what to fetch instead and the source hashes with what they are from, to
    /// name if it does not hash; or, when a block cannot be rebuilt (counted in `work`) or the chunk needs the RPC
    /// for anything else, lists it all in `found`'s fetch.
    async fn chunk(
        &mut self,
        found: &mut Scanned,
        work: &mut Work,
    ) -> eyre::Result<Option<(Fill, Fetch, Vec<derive::Source>)>> {
        let rows = std::mem::take(&mut found.header_rows);
        // The first block lacking its base fee needs its parent's: the chunk before's last
        // block, read earlier in this run (every chunk not sealed is, in block order); else,
        // with an endpoint, read from it.
        if let Some(first) = rows.first()
            && first.lacks.contains(&"base_fee_per_gas")
            && !self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.precedes(first.number))
            && let Some(rpc) = self.rpc
        {
            let parent = BlockNumHash::new(first.number.saturating_sub(1), first.parent_hash);
            let headers = rpc
                .headers(&[parent])
                .await
                .wrap_err("failed to read the parent of the run's first block from the RPC")?;
            self.parent = headers.first().and_then(Parent::of);
        }
        if let Some((first, last)) = derive::l1_range(&rows) {
            self.data
                .read(self.l1, self.chain, first, last)
                .await
                .wrap_err("failed to read L1 headers and deposits for the fields to rebuild")?;
        }
        let rebuilt = derive::rebuild(self.chain, &rows, &self.data, &mut self.parent);
        if rebuilt.blocks.is_empty() && rebuilt.left.is_empty() {
            return Ok(None);
        }
        for (block, field) in &rebuilt.left {
            work.unrebuildable(field, block.number);
        }
        // What the RPC would be asked for this chunk's rebuilt and unrebuilt blocks.
        let instead = rpc_fetch(&rows);
        // A chunk the RPC is read for anyway (a block not rebuilt, a list, a hole) has all its
        // fields read from it: what is rebuilt is only used once checked whole.
        if !rebuilt.left.is_empty() || !found.fetch.is_empty() {
            found.fetch.headers.extend(instead.headers);
            found.fetch.sources.extend(instead.sources);
            return Ok(None);
        }
        Ok(Some((
            Fill::rebuilt(rebuilt.headers, &rebuilt.sources),
            instead,
            rebuilt.sources,
        )))
    }
}

/// What the RPC is asked for `rows`' missing header fields and source hashes: a block lacking
/// source hashes is read with its transactions, which bring its header fields too.
fn rpc_fetch(rows: &[HeaderRow]) -> Fetch {
    let mut fetch = Fetch::default();
    for row in rows {
        let block = BlockNumHash::new(row.number, row.hash);
        if row.lacks_deposit_fields() {
            fetch.sources.push(Wanted {
                number: row.number,
                hash: row.hash,
                indexes: row
                    .deposits
                    .iter()
                    .filter(|deposit| !deposit.has_source || deposit.lacks_mint)
                    .map(|deposit| deposit.index)
                    .collect(),
            });
        } else if !row.lacks.is_empty() {
            fetch.headers.push(block);
        }
    }
    fetch
}

/// Adds the checked `rebuilt` fill to the fill at `path`. Blocking.
fn add_rebuilt(path: &Path, rebuilt: Fill) -> io::Result<()> {
    let mut fill = read_json::<Fill>(path)?.unwrap_or_default();
    fill.add(rebuilt);
    write_json(path, &fill)
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
    /// Header rows rebuilt from L1, and those of them that did not hash, fetched instead.
    rebuilt_headers: u64,
    rebuilt_sources: u64,
    rebuilt_mints: u64,
    unrebuilt_blocks: u64,
    /// The fields that cannot be rebuilt from L1: how many blocks, and the first.
    unrebuildable: BTreeMap<&'static str, (u64, u64)>,
    scan_rate: Rate,
    fetch_rate: Rate,
    /// What ends the run: the first fetch that failed for good, or without an endpoint the
    /// first chunk whose fields rebuilt from L1 do not hash.
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
            rebuilt_headers: 0,
            rebuilt_sources: 0,
            rebuilt_mints: 0,
            unrebuilt_blocks: 0,
            unrebuildable: BTreeMap::new(),
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

    /// Counts the headers, source hashes and mints of `fill`, rebuilt from L1, checked and
    /// written.
    fn rebuilt(&mut self, fill: &Fill) {
        let count = |count: usize| u64::try_from(count).unwrap_or(u64::MAX);
        let hashes = fill
            .sources
            .iter()
            .filter(|source| source.source_hash.is_some());
        let mints = fill.sources.iter().filter(|source| source.mint.is_some());
        self.rebuilt_headers = self
            .rebuilt_headers
            .saturating_add(count(fill.headers.len()));
        self.rebuilt_sources = self.rebuilt_sources.saturating_add(count(hashes.count()));
        self.rebuilt_mints = self.rebuilt_mints.saturating_add(count(mints.count()));
    }

    /// Counts a block whose `field` cannot be rebuilt from L1.
    fn unrebuildable(&mut self, field: &'static str, block: u64) {
        let entry = self.unrebuildable.entry(field).or_insert((0, block));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.min(block);
    }

    /// Takes what was rebuilt of `chunk` that does not hash (`why`): `instead` is fetched, if
    /// there is an endpoint; nothing rebuilt was written.
    fn unrebuilt(&mut self, chunk: Chunk, instead: Fetch, why: &str) {
        let blocks = u64::try_from(instead.blocks()).unwrap_or(u64::MAX);
        self.unrebuilt_blocks = self.unrebuilt_blocks.saturating_add(blocks);
        if !self.keep {
            self.failure.get_or_insert_with(|| {
                format!(
                    "the fields rebuilt from L1 do not hash, so none of that chunk's were \
                     written: blocks {}..{}: {why}. Give --rpc-endpoint to fetch them instead",
                    chunk.from, chunk.to
                )
            });
            return;
        }
        warn!(
            from = chunk.from,
            to = chunk.to,
            %why,
            "fields rebuilt from L1 do not hash: fetching them from the RPC"
        );
        self.queued_blocks = self.queued_blocks.saturating_add(blocks);
        self.to_fetch.push_back((chunk, instead));
    }

    /// Takes a finished fetch from `rpc`: counts it, or keeps its failure.
    fn fetch_done(
        &mut self,
        rpc: Option<&Rpc>,
        done: Result<(Chunk, eyre::Result<Fetched>), tokio::task::JoinError>,
    ) {
        let what = match done {
            Ok((_, Ok(fetched))) => return self.fetched(fetched),
            Ok((chunk, Err(err))) => format!("blocks {}..{}: {err:#}", chunk.from, chunk.to),
            // Aborted on cancel: nothing to report.
            Err(err) if err.is_cancelled() => return,
            Err(err) => format!("a fetch task failed: {err}"),
        };
        self.failure.get_or_insert_with(|| {
            format!(
                "fetching from {} failed: {what}; run `download` again, or give another \
                 endpoint with --rpc-endpoint",
                rpc.map_or("the RPC endpoint", Rpc::url)
            )
        });
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
            eyre::bail!(failure);
        }
        eyre::ensure!(
            !cancel.is_cancelled(),
            "stopped while checking the downloaded rows: run `download` again, which goes on \
             where it stopped"
        );
        if rpc.is_none() && !self.unrebuildable.is_empty() {
            let fields: Vec<String> = self
                .unrebuildable
                .iter()
                .map(|(field, (count, first))| format!("{field} ({count} blocks from {first})"))
                .collect();
            eyre::bail!(
                "these fields cannot be rebuilt from L1: {}; give --rpc-endpoint to fetch them",
                fields.join(", ")
            );
        }
        if rpc.is_none() && self.queued_blocks > 0 {
            eyre::bail!(
                "the archive service left out what {} block reads need (authorization lists, \
                 transactions, header fields, deposit source hashes), and no RPC endpoint is \
                 known for chain {}: give one with --rpc-endpoint",
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
            l1_rebuilt_headers = self.rebuilt_headers,
            l1_rebuilt_sources = self.rebuilt_sources,
            l1_rebuilt_mints = self.rebuilt_mints,
            l1_rebuilt_not_hashing = self.unrebuilt_blocks,
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

/// Every block's rows as the rebuild from L1 reads them, with the header fields `lacking`
/// lists for it (block by block, in block order) and its deposits.
fn header_rows(rows: &Rows, lacking: Vec<(u64, Vec<&'static str>)>) -> Vec<HeaderRow> {
    let mut lacking = lacking.into_iter().peekable();
    let mut transactions = rows.transactions.as_slice();
    rows.blocks
        .iter()
        .map(|block| {
            let lacks = lacking
                .next_if(|(number, _)| *number == block.number)
                .map(|(_, fields)| fields)
                .unwrap_or_default();
            // Transactions come in block order; a block's deposits are its first ones.
            while transactions
                .first()
                .is_some_and(|tx| tx.block_number < block.number)
            {
                transactions = transactions.get(1..).unwrap_or_default();
            }
            let count = transactions
                .iter()
                .take_while(|tx| tx.block_number == block.number)
                .count();
            let (own, rest) = transactions.split_at(count);
            transactions = rest;
            let deposits: Vec<DepositRow> = own
                .iter()
                .take_while(|tx| tx.kind == Some(op_alloy_consensus::DEPOSIT_TX_TYPE_ID))
                .map(|tx| DepositRow {
                    index: tx.transaction_index,
                    has_source: tx.source_hash.is_some(),
                    lacks_mint: tx.lacks_mint(),
                    from: tx.from,
                    to: tx.to,
                    mint: tx.mint,
                    value: tx.value,
                    gas: tx.gas.to(),
                    input: tx.input.clone(),
                })
                .collect();
            let from_l1 = deposits
                .iter()
                .any(|deposit| !deposit.has_source || deposit.lacks_mint)
                || lacks
                    .iter()
                    .any(|field| matches!(*field, "mix_hash" | "parent_beacon_block_root"));
            let l1_info = from_l1
                .then(|| L1Info::of(&deposits.first()?.input))
                .flatten();
            HeaderRow {
                number: block.number,
                hash: block.hash,
                parent_hash: block.parent_hash,
                timestamp: block.timestamp.to(),
                gas_limit: block.gas_limit.to(),
                gas_used: block.gas_used.to(),
                base_fee: block.base_fee_per_gas.map(|fee| fee.to()),
                extra_data: block.extra_data.clone(),
                lacks,
                l1_info,
                deposits,
            }
        })
        .collect()
}

/// Reads one downloaded chunk with its fill and lists what its rows lack. Blocking.
/// With `rebuild`, the rows are kept for the rebuild from L1 and no header field or source
/// hash is listed to fetch: the rebuild decides.
fn scan(forks: &Forks, raw: &Path, fill: &Path, rebuild: bool) -> eyre::Result<Scanned> {
    let mut rows = rows::read(raw)?;
    if let Some(fill) = read_json(fill)? {
        apply(&mut rows, fill);
    }
    let mut scanned = Scanned::default();
    // Header fields come block by block, in block order.
    let mut headers: Vec<(u64, Vec<&'static str>)> = Vec::new();
    missing(forks, &rows, |field, block| {
        tally(&mut scanned.missing, field, 1, block);
        if field.row != "header" {
            return;
        }
        match headers.last_mut() {
            Some((number, fields)) if *number == block => fields.push(field.field),
            _ => headers.push((block, vec![field.field])),
        }
    });
    if rebuild {
        scanned.header_rows = header_rows(&rows, headers);
        headers = Vec::new();
    }
    scanned.fetch.holes = holes(forks, &rows)
        .map(|block| BlockNumHash::new(block.number, block.hash))
        .collect();
    // A hole's block is fetched whole, its header fields with it: but those go to the header
    // row only through `headers`, so a hole lacking them is read for both.
    scanned.fetch.headers = headers
        .into_iter()
        .filter_map(|(number, _)| rows.block(number))
        .map(|block| BlockNumHash::new(block.number, block.hash))
        .collect();
    scanned.fetch.wanted = wanted(&rows, TransactionRow::lacks_authorization_list);
    // With the rebuild from L1, it decides what the RPC is asked for.
    if !rebuild {
        scanned.fetch.sources = wanted(&rows, |tx| {
            (tx.kind == Some(op_alloy_consensus::DEPOSIT_TX_TYPE_ID) && tx.source_hash.is_none())
                || tx.lacks_mint()
        });
    }
    // A block read with its transactions for its deposits brings its header fields too.
    scanned.fetch.headers.retain(|header| {
        scanned
            .fetch
            .sources
            .binary_search_by_key(&header.number, |block| block.number)
            .is_err()
    });
    Ok(scanned)
}

/// The blocks with transactions `lacks` a field of, with those transactions' indexes, in block
/// order.
fn wanted(rows: &Rows, lacks: impl Fn(&TransactionRow) -> bool) -> Vec<Wanted> {
    let mut indexes: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for tx in rows.transactions.iter().filter(|tx| lacks(tx)) {
        indexes
            .entry(tx.block_number)
            .or_default()
            .push(tx.transaction_index);
    }
    indexes
        .into_iter()
        // A block the chunk lacks fails `verify` on its own; nothing to fetch for it.
        .filter_map(|(number, indexes)| {
            let block = rows.block(number)?;
            Some(Wanted {
                number,
                hash: block.hash,
                indexes,
            })
        })
        .collect()
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
    let mut sources = Vec::new();
    for batch in fetch.sources.chunks(rpc.batch_calls()) {
        for (block, (header, deposits)) in batch.iter().zip(rpc.deposit_sources(batch).await?) {
            headers.push(header);
            sources.extend(block.indexes.iter().zip(deposits).map(
                |(&index, (source_hash, mint))| FilledSource {
                    block_number: block.number,
                    transaction_index: index,
                    source_hash: Some(source_hash),
                    mint: Some(mint),
                },
            ));
        }
    }
    let added: usize = blocks.iter().map(|block| block.transactions.len()).sum();
    let fetched = Fetched {
        blocks: u64::try_from(fetch.blocks()).unwrap_or(u64::MAX),
        transactions: u64::try_from(
            lists
                .len()
                .saturating_add(added)
                .saturating_add(sources.len()),
        )
        .unwrap_or(u64::MAX),
        headers: u64::try_from(headers.len()).unwrap_or(u64::MAX),
    };
    tokio::task::spawn_blocking(move || {
        let mut fill = read_json::<Fill>(&path)?.unwrap_or_default();
        fill.transactions.extend(lists);
        fill.blocks.extend(blocks);
        fill.put_headers(headers);
        fill.sources.extend(sources);
        write_json(&path, &fill)
    })
    .await??;
    Ok(fetched)
}
