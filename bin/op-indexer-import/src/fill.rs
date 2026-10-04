//! What the archive service leaves out of some rows, fetched from the chain's JSON-RPC
//! ([`Rpc`]) by `download` and kept next to the chunk, so `verify` stays offline.
//!
//! Known so far: on Unichain the service sends EIP-7702 transactions without their
//! authorization list (OP Mainnet's carry it). The list is not optional, so such a row cannot
//! be rebuilt from the download alone.
//!
//! After the chunks are downloaded, every chunk not verified yet is read, several at once, and
//! checked for every field its rows lack ([`crate::verify::missing`]): all of them are logged
//! in one summary, so they can be dealt with together rather than one `verify` failure at a
//! time. The authorization lists missing are fetched, one block per request, a few requests at
//! a time, and written to the chunk's fill (`<from>-<to>.fill.json` in `raw/`, written
//! atomically). The downloaded chunk itself is kept as received. `verify` merges the fill into
//! the rows; a row that still lacks its list fails there with a message to run `download`.
//!
//! The trust is unchanged: what the fill holds goes into the rebuilt transaction, and the
//! block's header hash proves it.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::B256;
use eyre::WrapErr;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chunk::{self, ChunkFile};
use crate::progress::{self, Rate};
use crate::rows::{self, Rows};
use crate::rpc::Rpc;
use crate::state::{Chunk, Plan, State, write_atomic};
use crate::verify::{Forks, Missing, missing};

/// Requests to the RPC endpoint in flight at once: it is a public endpoint, used politely.
const RPC_REQUESTS: usize = 4;

/// What a chunk's rows lack, fetched from the chain's RPC.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Fill {
    /// Transactions whose authorization list the service left out.
    transactions: Vec<FilledTransaction>,
}

/// One transaction's authorization list, in the RPC's form.
#[derive(Debug, Serialize, Deserialize)]
struct FilledTransaction {
    block_number: u64,
    transaction_index: u64,
    authorization_list: Vec<SignedAuthorization>,
}

/// Reads the fill at `path`, if there is one. Blocking.
///
/// # Errors
///
/// Returns the I/O error, or `InvalidData` if the file is not a fill.
pub(crate) fn read(path: &Path) -> io::Result<Option<Fill>> {
    let content = match fs::read(path) {
        Ok(content) => content,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    serde_json::from_slice(&content).map(Some).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is damaged: {err}; delete it and run `download`",
                path.display()
            ),
        )
    })
}

/// Puts what `fill` holds into the rows it is for. Returns how many rows it filled.
pub(crate) fn apply(rows: &mut Rows, fill: Fill) -> u64 {
    let mut filled = 0_u64;
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

/// A block with transactions whose authorization list is missing and not filled.
#[derive(Debug)]
struct ToFill {
    number: u64,
    hash: B256,
    /// The transactions' indexes in the block.
    indexes: Vec<u64>,
}

/// What the scan found in one chunk.
#[derive(Debug, Default)]
struct Scanned {
    /// Each field missing, how often, and the first block lacking it.
    missing: BTreeMap<Missing, (u64, u64)>,
    to_fill: Vec<ToFill>,
}

/// Lists every field the rows of the downloaded chunks not verified yet lack, logs them, and
/// fetches the authorization lists missing from `rpc` into the chunks' fills. `threads` chunks
/// are read at once.
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
    let mut queue = chunks.into_iter();
    let mut tasks = JoinSet::new();
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rate = Rate::new();
    let mut scanned = 0_usize;
    let mut lacking: BTreeMap<Missing, (u64, u64)> = BTreeMap::new();
    let mut to_fill: Vec<(Chunk, Vec<ToFill>)> = Vec::new();
    loop {
        while !cancel.is_cancelled()
            && tasks.len() < threads
            && let Some(chunk) = queue.next()
        {
            let (raw, fill) = (state.raw_path(chunk), state.fill_path(chunk));
            tasks.spawn_blocking(move || (chunk, scan(&forks, chunk, &raw, &fill)));
        }
        tokio::select! {
            finished = tasks.join_next() => match finished {
                Some(Ok((chunk, found))) => {
                    let found = found.wrap_err_with(|| {
                        format!("blocks {}..{}: failed to read the chunk", chunk.from, chunk.to)
                    })?;
                    scanned = scanned.saturating_add(1);
                    for (field, (count, first)) in found.missing {
                        let entry = lacking.entry(field).or_insert((0, first));
                        entry.0 = entry.0.saturating_add(count);
                        entry.1 = entry.1.min(first);
                    }
                    if !found.to_fill.is_empty() {
                        to_fill.push((chunk, found.to_fill));
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
    let transactions: usize = to_fill
        .iter()
        .flat_map(|(_, blocks)| blocks)
        .map(|block| block.indexes.len())
        .sum();
    info!(
        chunks = scanned,
        secs = started.elapsed().as_secs(),
        fields_missing = lacking.len(),
        authorization_lists_to_fetch = transactions,
        "downloaded rows checked"
    );
    for (field, (count, first_block)) in &lacking {
        warn!(
            row = field.row,
            field = field.field,
            count,
            first_block,
            "downloaded rows lack this field"
        );
    }
    if to_fill.is_empty() {
        return Ok(());
    }
    let rpc = rpc.ok_or_else(|| {
        eyre::eyre!(
            "{transactions} EIP-7702 transactions lack their authorization list, which the \
             archive service left out, and no RPC endpoint is known for chain {}: give one with \
             --rpc-endpoint",
            plan.chain.chain_id
        )
    })?;
    fetch(state, rpc, to_fill, transactions, cancel).await
}

/// The chunks downloaded and not verified yet. Blocking.
fn to_scan(state: &State, plan: &Plan) -> io::Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    for chunk in plan.chunks() {
        if state.raw_path(chunk).try_exists()?
            && chunk::check(&state.verified_path(chunk))? != ChunkFile::Present
        {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

/// Reads one downloaded chunk with its fill and lists what its rows lack. Blocking.
fn scan(forks: &Forks, chunk: Chunk, raw: &Path, fill: &Path) -> eyre::Result<Scanned> {
    let mut rows = rows::read(raw)?;
    if let Some(fill) = read(fill)? {
        apply(&mut rows, fill);
    }
    let mut scanned = Scanned::default();
    let mut lists: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    missing(forks, &rows, |field, block| {
        let entry = scanned.missing.entry(field).or_insert((0, block));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.min(block);
    });
    for tx in &rows.transactions {
        let unfilled = tx.kind == Some(4)
            && tx.filled_authorization_list.is_none()
            && tx
                .authorization_list
                .as_ref()
                .is_none_or(|list| list.is_empty());
        if unfilled && (chunk.from..chunk.to).contains(&tx.block_number) {
            lists
                .entry(tx.block_number)
                .or_default()
                .push(tx.transaction_index);
        }
    }
    let hashes: HashMap<u64, B256> = rows
        .blocks
        .iter()
        .map(|block| (block.number, block.hash))
        .collect();
    for (number, indexes) in lists {
        // A block the chunk lacks fails `verify` on its own; nothing to fetch for it.
        if let Some(hash) = hashes.get(&number) {
            scanned.to_fill.push(ToFill {
                number,
                hash: *hash,
                indexes,
            });
        }
    }
    Ok(scanned)
}

/// Fetches the authorization lists of `to_fill`, [`RPC_REQUESTS`] blocks at a time, and adds
/// them to each chunk's fill.
async fn fetch(
    state: &State,
    rpc: &Rpc,
    to_fill: Vec<(Chunk, Vec<ToFill>)>,
    transactions: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let started = Instant::now();
    info!(
        chunks = to_fill.len(),
        transactions,
        endpoint = rpc.url(),
        requests = RPC_REQUESTS,
        "fetching the authorization lists the archive service left out"
    );
    let mut queue = to_fill.into_iter();
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
            tasks.spawn(async move { (chunk, fill_chunk(&rpc, blocks, path).await) });
        }
        match tasks.join_next().await {
            Some(Ok((_, Ok(count)))) => filled = filled.saturating_add(count),
            Some(Ok((chunk, Err(err)))) => {
                failure
                    .get_or_insert_with(|| format!("blocks {}..{}: {err:#}", chunk.from, chunk.to));
            }
            Some(Err(err)) => {
                failure.get_or_insert_with(|| format!("a fetch task failed: {err}"));
            }
            None => break,
        }
    }
    info!(
        rpc_filled_transactions = filled,
        secs = started.elapsed().as_secs(),
        "authorization lists fetched"
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

/// Fetches one chunk's missing lists, block by block, and writes its fill with them added to
/// what it held. Returns how many transactions it filled.
async fn fill_chunk(rpc: &Rpc, blocks: Vec<ToFill>, path: std::path::PathBuf) -> eyre::Result<u64> {
    let mut fetched = Vec::new();
    for ToFill {
        number,
        hash,
        indexes,
    } in blocks
    {
        let lists = rpc.authorization_lists(number, hash, &indexes).await?;
        fetched.extend(
            indexes
                .into_iter()
                .zip(lists)
                .map(|(index, list)| FilledTransaction {
                    block_number: number,
                    transaction_index: index,
                    authorization_list: list,
                }),
        );
    }
    let count = u64::try_from(fetched.len()).unwrap_or(u64::MAX);
    tokio::task::spawn_blocking(move || {
        let mut fill = read(&path)?.unwrap_or_default();
        fill.transactions.extend(fetched);
        let content = serde_json::to_vec(&fill)?;
        write_atomic(&path, |file| file.write_all(&content))
    })
    .await??;
    Ok(count)
}
