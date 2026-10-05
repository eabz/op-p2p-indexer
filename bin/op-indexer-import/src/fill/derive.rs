//! Header fields the archive service left out, rebuilt from L1 instead of fetched from the
//! chain's RPC (`--headers-from l1`). Base's rows lack `mix_hash` and the base fee in large
//! stretches; reading millions of headers from a public RPC takes days, while every such field
//! follows from what the download and L1 already have:
//!
//! - `mix_hash` is the L1 origin's `mix_hash` (its `prevrandao`), and from Ecotone on the
//!   parent beacon block root is the L1 origin's own `parent_beacon_block_root`. The L1 origin
//!   is named by the block's L1-attributes deposit (its first transaction): its number in bytes
//!   28..36 of the calldata and its hash in bytes 100..132, in the Bedrock form (ABI words) and
//!   in the packed forms of Ecotone and later alike. The L1 header read for that number must
//!   have that hash. L1 headers are read from L1's `HyperSync`, a span of blocks per request.
//! - The base fee follows from the parent's by EIP-1559 with the chain's parameters
//!   ([`op_indexer_chainspec::Eip1559`]: the denominator changes at Canyon), and from Holocene
//!   on with the parameters in the parent's `extraData` (both zero: the chain's own); alloy's
//!   and op-alloy's code computes and decodes them. Computed in block order, across
//!   chunks; not from Jovian on (its minimum base fee and data footprint are not rebuilt here).
//! - The withdrawals root from Canyon is the empty trie's root until Isthmus (from Isthmus it
//!   is the message passer's storage root, which cannot be rebuilt); the blob gas used and the
//!   excess blob gas from Ecotone are zero (the blob gas used until Jovian, which makes it the
//!   data availability footprint).
//!
//! Checked on 2026-10-05 against Base's own headers at blocks 1.04 M (Bedrock deposit, before
//! Canyon), 11.5 M (Canyon), 12.52 M (Ecotone) and 30 M (Holocene). Nothing here is trusted:
//! the header hash proves every rebuilt block, and `fill` checks a chunk's before writing them.

use std::collections::BTreeMap;

use alloy_consensus::EMPTY_ROOT_HASH;
use alloy_eips::BlockNumHash;
use alloy_eips::eip1559::{BaseFeeParams, calc_next_block_base_fee};
use alloy_primitives::{B256, Bytes, U64};
use op_alloy_consensus::decode_holocene_extra_data;
use op_indexer_chainspec::ChainSpec;

use crate::rpc::RpcHeader;
use crate::source::{HyperSync, L1Header, SourceError};

/// L1 blocks read per request, at least: about 33 hours of L1, so of L2 (about 60,000 Base
/// blocks) per span.
const L1_SPAN: u64 = 10_000;

/// What the rebuild reads of one block's header row.
#[derive(Debug, Clone)]
pub(super) struct HeaderRow {
    pub(super) number: u64,
    pub(super) hash: B256,
    pub(super) parent_hash: B256,
    pub(super) timestamp: u64,
    pub(super) gas_limit: u64,
    pub(super) gas_used: u64,
    pub(super) base_fee: Option<u64>,
    pub(super) extra_data: Bytes,
    /// The header fields of its forks the row lacks.
    pub(super) lacks: Vec<&'static str>,
    /// Its L1 origin's number and hash, from its L1-attributes deposit, if it lacks a field
    /// taken from there.
    pub(super) l1_origin: Option<(u64, B256)>,
}

/// The L1 origin's number and hash in an L1-attributes deposit's calldata: the same bytes in
/// the Bedrock form and the packed forms of Ecotone and later. `None` if it is too short.
pub(super) fn l1_origin(input: &[u8]) -> Option<(u64, B256)> {
    let number = u64::from_be_bytes(input.get(28..36)?.try_into().ok()?);
    let hash = B256::from_slice(input.get(100..132)?);
    Some((number, hash))
}

/// The parent of the next block, as the base fee reads it.
#[derive(Debug, Clone)]
pub(super) struct Parent {
    number: u64,
    timestamp: u64,
    gas_limit: u64,
    gas_used: u64,
    base_fee: u64,
    extra_data: Bytes,
}

impl Parent {
    /// Whether this is the block before block `number`.
    pub(super) const fn precedes(&self, number: u64) -> bool {
        self.number.saturating_add(1) == number
    }

    /// The parent a header read from the RPC gives, if it has what the base fee reads.
    pub(super) fn of(header: &RpcHeader) -> Option<Self> {
        Some(Self {
            number: header.number.to(),
            timestamp: header.timestamp?.to(),
            gas_limit: header.gas_limit?.to(),
            gas_used: header.gas_used?.to(),
            base_fee: header.base_fee_per_gas?.to(),
            extra_data: header.extra_data.clone()?,
        })
    }
}

/// The base fee of the block after `parent`, at `timestamp`, by EIP-1559 as the OP Stack runs
/// it (alloy's computation); `None` from Jovian on, or with Holocene parameters that do not
/// parse.
fn base_fee(chain: &ChainSpec, parent: &Parent, timestamp: u64) -> Option<u64> {
    if parent.timestamp >= chain.jovian_time() {
        return None;
    }
    let params = chain.eip1559;
    let denominator = if timestamp >= chain.canyon_time() {
        params.denominator_canyon
    } else {
        params.denominator
    };
    let chain_params = (params.elasticity, denominator);
    let (elasticity, denominator) = if parent.timestamp >= chain.holocene_time() {
        match decode_holocene_extra_data(&parent.extra_data).ok()? {
            // Both zero: the chain's own.
            (0, 0) => chain_params,
            (0, _) | (_, 0) => return None,
            (elasticity, denominator) => (u64::from(elasticity), u64::from(denominator)),
        }
    } else {
        chain_params
    };
    let params = BaseFeeParams::new(u128::from(denominator), u128::from(elasticity));
    Some(calc_next_block_base_fee(
        parent.gas_used,
        parent.gas_limit,
        parent.base_fee,
        params,
    ))
}

/// L1 headers read so far, by number.
#[derive(Debug, Default)]
pub(super) struct L1Headers {
    headers: BTreeMap<u64, L1Header>,
    /// Read from here on.
    from: u64,
    /// Read up to here, excluded.
    to: u64,
}

impl L1Headers {
    /// Makes sure blocks `first..=last` are read, reading a span from `first` if not; forgets
    /// those below `first`, which later blocks no longer need.
    pub(super) async fn read(
        &mut self,
        l1: &HyperSync,
        first: u64,
        last: u64,
    ) -> Result<(), SourceError> {
        if first >= self.from && last < self.to {
            return Ok(());
        }
        let to = last.saturating_add(1).max(first.saturating_add(L1_SPAN));
        let read = l1.l1_headers(first, to).await?;
        self.headers = self.headers.split_off(&first);
        self.headers
            .extend(read.into_iter().map(|header| (header.number, header)));
        (self.from, self.to) = (first, to);
        Ok(())
    }

    /// The header of L1 block `number`, if read and its hash is `hash`.
    fn get(&self, (number, hash): (u64, B256)) -> Option<&L1Header> {
        self.headers
            .get(&number)
            .filter(|header| header.hash == hash)
    }
}

/// The rebuild of one chunk's header fields.
#[derive(Debug, Default)]
pub(super) struct Rebuilt {
    /// The blocks rebuilt, and their headers with the fields their rows lack.
    pub(super) blocks: Vec<BlockNumHash>,
    pub(super) headers: Vec<RpcHeader>,
    /// The blocks a field of which cannot be rebuilt (a later fork's, no parent known, an L1
    /// origin not read or of another hash).
    pub(super) left: Vec<BlockNumHash>,
}

/// Rebuilds the header fields `rows` (one chunk's, in block order) lack, from `l1` and from
/// `parent`, the block before the chunk's first, which it leaves at the chunk's last.
pub(super) fn rebuild(
    chain: &ChainSpec,
    rows: &[HeaderRow],
    l1: &L1Headers,
    parent: &mut Option<Parent>,
) -> Rebuilt {
    let mut rebuilt = Rebuilt::default();
    for row in rows {
        let parent_of_row = parent.take().filter(|parent| parent.precedes(row.number));
        let origin = row.l1_origin.and_then(|origin| l1.get(origin));
        let mut header = RpcHeader::new(row.number);
        let mut whole = true;
        for &field in &row.lacks {
            let done = match field {
                "mix_hash" => {
                    header.mix_hash = origin.and_then(|origin| origin.mix_hash);
                    header.mix_hash.is_some()
                }
                "parent_beacon_block_root" => {
                    header.parent_beacon_block_root =
                        origin.and_then(|origin| origin.parent_beacon_block_root);
                    header.parent_beacon_block_root.is_some()
                }
                "base_fee_per_gas" => {
                    header.base_fee_per_gas = parent_of_row
                        .as_ref()
                        .and_then(|parent| base_fee(chain, parent, row.timestamp))
                        .map(U64::from);
                    header.base_fee_per_gas.is_some()
                }
                "withdrawals_root" => {
                    header.withdrawals_root =
                        (row.timestamp < chain.isthmus_time()).then_some(EMPTY_ROOT_HASH);
                    header.withdrawals_root.is_some()
                }
                "blob_gas_used" => {
                    header.blob_gas_used =
                        (row.timestamp < chain.jovian_time()).then_some(U64::ZERO);
                    header.blob_gas_used.is_some()
                }
                "excess_blob_gas" => {
                    header.excess_blob_gas = Some(U64::ZERO);
                    true
                }
                _ => false,
            };
            whole &= done;
        }
        let base_fee = row
            .base_fee
            .or_else(|| header.base_fee_per_gas.map(|fee| fee.to()));
        *parent = base_fee.map(|base_fee| Parent {
            number: row.number,
            timestamp: row.timestamp,
            gas_limit: row.gas_limit,
            gas_used: row.gas_used,
            base_fee,
            extra_data: row.extra_data.clone(),
        });
        if row.lacks.is_empty() {
            continue;
        }
        let block = BlockNumHash::new(row.number, row.hash);
        if whole {
            rebuilt.blocks.push(block);
            rebuilt.headers.push(header);
        } else {
            rebuilt.left.push(block);
        }
    }
    rebuilt
}

/// The L1 blocks `rows` need, lowest and highest.
pub(super) fn l1_range(rows: &[HeaderRow]) -> Option<(u64, u64)> {
    let mut numbers = rows
        .iter()
        .filter_map(|row| row.l1_origin.map(|(number, _)| number));
    let first = numbers.next()?;
    Some(numbers.fold((first, first), |(low, high), number| {
        (low.min(number), high.max(number))
    }))
}
