//! What the archive service left out of a block's rows, rebuilt from L1 instead of fetched
//! from the chain's RPC (`--fill-from l1`). Base's rows lack `mix_hash`, the base fee and every
//! deposit's `source_hash` in large stretches; reading millions of blocks from a public RPC
//! takes days, while each of them follows from what the download and L1 already have:
//!
//! - `mix_hash` is the L1 origin's `mix_hash` (its `prevrandao`), and from Ecotone on the
//!   parent beacon block root is the L1 origin's own `parent_beacon_block_root`. The L1 origin
//!   is named by the block's L1-attributes deposit (its first transaction), [`L1Info`]. The L1
//!   header read for that number must have that hash.
//! - The base fee follows from the parent's by EIP-1559 with the chain's parameters
//!   ([`op_indexer_chainspec::Eip1559`]: the denominator changes at Canyon), and from Holocene
//!   on with the parameters in the parent's `extraData` (both zero: the chain's own); alloy's
//!   and op-alloy's code computes and decodes them. Computed in block order across chunks; a
//!   run starts from the last block of the chunk before its first, as downloaded and filled
//!   ([`Parent`]). Not from Jovian on (its minimum base fee and data footprint are not rebuilt
//!   here).
//! - The withdrawals root from Canyon is the empty trie's root until Isthmus (from Isthmus it
//!   is the message passer's storage root, which cannot be rebuilt); the blob gas used and the
//!   excess blob gas from Ecotone are zero (the blob gas used until Jovian, which makes it the
//!   data availability footprint).
//! - A deposit's source hash, by its kind (the deposit spec; op-alloy's code hashes them): the
//!   L1-attributes deposit's from its L1 origin's hash and its sequence number; in the first
//!   block of an epoch (sequence number 0) the next deposits are the users', one per
//!   `TransactionDeposited` log of the chain's `OptimismPortal` in the L1 origin, in log order,
//!   each from the origin's hash and the log's index in its block; the deposits after them, in
//!   a fork's first block, are its upgrade transactions, from their intents (Ecotone's and
//!   Fjord's are known here; another fork's fails naming the block).
//!
//! L1 headers and the portal's logs are read from L1's `HyperSync`, one query per span of at
//! least [`L1_SPAN`] blocks. Checked on 2026-10-05 against Base's own blocks: headers at 1.04 M
//! (Bedrock deposit, before Canyon), 11.5 M (Canyon), 12.52 M (Ecotone) and 30 M (Holocene);
//! source hashes at 11.5 M (user deposits), 11,792,527 (Ecotone's upgrade) and 16,918,927
//! (Fjord's). Nothing here is trusted: the header hash proves every rebuilt block, and `fill`
//! checks a chunk's before writing them.

use std::collections::BTreeMap;
use std::fmt;

use alloy_consensus::EMPTY_ROOT_HASH;
use alloy_eips::BlockNumHash;
use alloy_eips::eip1559::{BaseFeeParams, calc_next_block_base_fee};
use alloy_primitives::{B256, Bytes, U64};
use op_alloy_consensus::{
    L1InfoDepositSource, UpgradeDepositSource, UserDepositSource, decode_holocene_extra_data,
};
use op_indexer_chainspec::{ChainSpec, Hardfork};

use crate::rpc::RpcHeader;
use crate::source::{HyperSync, L1Header, SourceError};

/// L1 blocks read per request, at least: about 33 hours of L1, so of L2 (about 60,000 Base
/// blocks) per span.
const L1_SPAN: u64 = 10_000;

/// Selector of the Bedrock form of the L1-attributes deposit (`setL1BlockValues`, ABI words);
/// the forms from Ecotone on are packed.
const BEDROCK_SELECTOR: [u8; 4] = [0x01, 0x5d, 0x8e, 0xb9];

/// The upgrade transactions of a fork's first block, by their intents, in their order
/// (op-node's): the forks whose blocks the rebuild knows.
const UPGRADES: &[(Hardfork, &[&str])] = &[
    (
        Hardfork::Ecotone,
        &[
            "Ecotone: L1 Block Deployment",
            "Ecotone: Gas Price Oracle Deployment",
            "Ecotone: L1 Block Proxy Update",
            "Ecotone: Gas Price Oracle Proxy Update",
            "Ecotone: Gas Price Oracle Set Ecotone",
            "Ecotone: beacon block roots contract deployment",
        ],
    ),
    (
        Hardfork::Fjord,
        &[
            "Fjord: Gas Price Oracle Deployment",
            "Fjord: Gas Price Oracle Proxy Update",
            "Fjord: Gas Price Oracle Set Fjord",
        ],
    ),
];

/// What a block's L1-attributes deposit says of its L1 origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct L1Info {
    /// The L1 origin's number and hash.
    pub(super) number: u64,
    pub(super) hash: B256,
    /// The block's place in its epoch: 0 for the epoch's first, where its user deposits are.
    pub(super) sequence: u64,
}

impl L1Info {
    /// Reads the deposit's calldata: the origin's number at bytes 28..36 and hash at 100..132,
    /// in the Bedrock form and the packed forms alike, and the sequence number at 156..164 in
    /// the Bedrock form, 12..20 in the packed ones. `None` if it is too short.
    pub(super) fn of(input: &[u8]) -> Option<Self> {
        let word = |at: std::ops::Range<usize>| -> Option<u64> {
            Some(u64::from_be_bytes(input.get(at)?.try_into().ok()?))
        };
        let sequence = if input.get(..4) == Some(&BEDROCK_SELECTOR[..]) {
            word(156..164)?
        } else {
            word(12..20)?
        };
        Some(Self {
            number: word(28..36)?,
            hash: B256::from_slice(input.get(100..132)?),
            sequence,
        })
    }
}

/// What the rebuild reads of one block's rows.
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
    /// Its L1-attributes deposit, read if a field it lacks comes from there.
    pub(super) l1_info: Option<L1Info>,
    /// Its deposits (the first transactions, from the L1-attributes one), and those whose row
    /// lacks the source hash, by index.
    pub(super) deposits: u64,
    pub(super) lacking_sources: Vec<u64>,
}

impl HeaderRow {
    /// Whether the rebuild of its fields reads its L1 origin: a header field from there, or
    /// the source hashes of user deposits.
    fn reads_l1(&self) -> bool {
        self.lacks
            .iter()
            .any(|field| matches!(*field, "mix_hash" | "parent_beacon_block_root"))
            || (!self.lacking_sources.is_empty()
                && self.deposits > 1
                && self.l1_info.is_some_and(|info| info.sequence == 0))
    }
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

/// The source hashes of a block's deposits, in their order, with what each is from: its
/// L1-attributes deposit, the portal's logs in its L1 origin (if the epoch starts here) and
/// the fork it is the first block of; `None` if the deposits are not those.
fn sources(chain: &ChainSpec, row: &HeaderRow, l1: &L1Data) -> Option<Vec<(B256, Derivation)>> {
    let info = row.l1_info?;
    let deposits = usize::try_from(row.deposits).ok()?;
    let mut hashes = Vec::with_capacity(deposits);
    hashes.push((
        L1InfoDepositSource::new(info.hash, info.sequence).source_hash(),
        Derivation::L1Info {
            origin: info.number,
            sequence: info.sequence,
        },
    ));
    if info.sequence == 0 && deposits > 1 {
        hashes.extend(l1.logs(info)?.iter().map(|&log_index| {
            (
                UserDepositSource::new(info.hash, log_index).source_hash(),
                Derivation::User {
                    origin: info.number,
                    log_index,
                },
            )
        }));
    }
    let upgrades = deposits.checked_sub(hashes.len())?;
    if upgrades > 0 {
        // The fork whose first block this is.
        let starts = |fork: Hardfork| {
            chain.activation(fork).is_some_and(|time| {
                row.timestamp >= time && row.timestamp < time.saturating_add(chain.block_time_secs)
            })
        };
        let (_, intents) = UPGRADES.iter().find(|(fork, _)| starts(*fork))?;
        if intents.len() != upgrades {
            return None;
        }
        hashes.extend(intents.iter().map(|&intent| {
            (
                UpgradeDepositSource::new(intent.to_owned()).source_hash(),
                Derivation::Upgrade(intent),
            )
        }));
    }
    Some(hashes)
}

/// What a span of L1 gave: headers and the portal's deposit logs, by L1 block number.
#[derive(Debug, Default)]
pub(super) struct L1Data {
    headers: BTreeMap<u64, L1Header>,
    /// Each block's deposit log indexes, ascending; a block without any is absent.
    logs: BTreeMap<u64, Vec<u64>>,
    /// Read from here on.
    from: u64,
    /// Read up to here, excluded.
    to: u64,
}

impl L1Data {
    /// Makes sure blocks `first..=last` are read, reading a span from `first` if not; forgets
    /// those below `first`, which later blocks no longer need.
    pub(super) async fn read(
        &mut self,
        l1: &HyperSync,
        chain: &ChainSpec,
        first: u64,
        last: u64,
    ) -> Result<(), SourceError> {
        if first >= self.from && last < self.to {
            return Ok(());
        }
        let to = last.saturating_add(1).max(first.saturating_add(L1_SPAN));
        let span = l1.l1_span(first, to, chain.optimism_portal).await?;
        self.headers = self.headers.split_off(&first);
        self.logs = self.logs.split_off(&first);
        self.headers.extend(
            span.headers
                .into_iter()
                .map(|header| (header.number, header)),
        );
        for log in span.deposits {
            self.logs
                .entry(log.block_number)
                .or_default()
                .push(log.log_index);
        }
        (self.from, self.to) = (first, to);
        Ok(())
    }

    /// The header of the L1 origin `info` names, if read and of its hash.
    fn header(&self, info: L1Info) -> Option<&L1Header> {
        self.headers
            .get(&info.number)
            .filter(|header| header.hash == info.hash)
    }

    /// The deposit log indexes of the L1 origin `info` names, if it was read (its header has
    /// its hash): none is an empty list.
    fn logs(&self, info: L1Info) -> Option<&[u64]> {
        self.header(info)?;
        Some(self.logs.get(&info.number).map_or(&[], Vec::as_slice))
    }
}

/// A deposit's source hash, rebuilt.
#[derive(Debug, Clone, Copy)]
pub(super) struct Source {
    pub(super) number: u64,
    pub(super) index: u64,
    pub(super) hash: B256,
    from: Derivation,
}

/// What a rebuilt source hash is derived from, by the deposit's kind.
#[derive(Debug, Clone, Copy)]
enum Derivation {
    L1Info { origin: u64, sequence: u64 },
    User { origin: u64, log_index: u64 },
    Upgrade(&'static str),
}

impl fmt::Display for Derivation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::L1Info { origin, sequence } => write!(
                f,
                "L1-attributes deposit of L1 origin {origin}, sequence number {sequence}"
            ),
            Self::User { origin, log_index } => write!(
                f,
                "user deposit of the portal log at index {log_index} of L1 block {origin}"
            ),
            Self::Upgrade(intent) => write!(f, "upgrade deposit \"{intent}\""),
        }
    }
}

/// The rebuild of one chunk's missing fields.
#[derive(Debug, Default)]
pub(super) struct Rebuilt {
    /// The blocks rebuilt whole, with their headers' missing fields and their deposits'
    /// missing source hashes.
    pub(super) blocks: Vec<BlockNumHash>,
    pub(super) headers: Vec<RpcHeader>,
    pub(super) sources: Vec<Source>,
    /// The blocks with a field that cannot be rebuilt, and that field (the first).
    pub(super) left: Vec<(BlockNumHash, &'static str)>,
}

/// Every field rebuilt for block `number` among `headers` and `sources`, with its value and,
/// for a source hash, what it is from: the header hash covers them all at once, and the rows
/// carry no transaction hashes or roots to tell them apart.
pub(super) fn describe(number: u64, headers: &[RpcHeader], sources: &[Source]) -> String {
    let mut fields = Vec::new();
    if let Some(header) = headers
        .iter()
        .find(|header| header.number.to::<u64>() == number)
    {
        let hashes = [
            ("mix_hash", header.mix_hash),
            ("parent_beacon_block_root", header.parent_beacon_block_root),
            ("withdrawals_root", header.withdrawals_root),
        ];
        let numbers = [
            ("base_fee_per_gas", header.base_fee_per_gas),
            ("blob_gas_used", header.blob_gas_used),
            ("excess_blob_gas", header.excess_blob_gas),
        ];
        fields.extend(
            hashes
                .iter()
                .filter_map(|(field, value)| Some(format!("{field} {}", (*value)?))),
        );
        fields.extend(
            numbers
                .iter()
                .filter_map(|(field, value)| Some(format!("{field} {}", (*value)?))),
        );
    }
    fields.extend(
        sources
            .iter()
            .filter(|source| source.number == number)
            .map(|source| {
                format!(
                    "transaction {}'s source_hash {} ({})",
                    source.index, source.hash, source.from
                )
            }),
    );
    if fields.is_empty() {
        return format!("nothing was rebuilt for block {number}");
    }
    format!("rebuilt for block {number}: {}", fields.join(", "))
}

/// Rebuilds the fields `rows` (one chunk's, in block order) lack, from `l1` and from `parent`,
/// the block before the chunk's first, which it leaves at the chunk's last.
pub(super) fn rebuild(
    chain: &ChainSpec,
    rows: &[HeaderRow],
    l1: &L1Data,
    parent: &mut Option<Parent>,
) -> Rebuilt {
    let mut rebuilt = Rebuilt::default();
    for row in rows {
        let parent_of_row = parent.take().filter(|parent| parent.precedes(row.number));
        let origin = row.l1_info.and_then(|info| l1.header(info));
        let mut header = RpcHeader::new(row.number);
        let mut left = None;
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
            if !done {
                left.get_or_insert(field);
            }
        }
        let mut sources = Vec::new();
        if !row.lacking_sources.is_empty() {
            match self::sources(chain, row, l1) {
                Some(hashes) => sources.extend(row.lacking_sources.iter().filter_map(|&index| {
                    let (hash, from) = *hashes.get(usize::try_from(index).ok()?)?;
                    Some(Source {
                        number: row.number,
                        index,
                        hash,
                        from,
                    })
                })),
                None => {
                    left.get_or_insert("source_hash");
                }
            }
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
        if row.lacks.is_empty() && row.lacking_sources.is_empty() {
            continue;
        }
        let block = BlockNumHash::new(row.number, row.hash);
        if let Some(field) = left {
            rebuilt.left.push((block, field));
        } else {
            rebuilt.blocks.push(block);
            if !row.lacks.is_empty() {
                rebuilt.headers.push(header);
            }
            rebuilt.sources.extend(sources);
        }
    }
    rebuilt
}

/// The L1 blocks `rows` need read, lowest and highest.
pub(super) fn l1_range(rows: &[HeaderRow]) -> Option<(u64, u64)> {
    let mut numbers = rows
        .iter()
        .filter(|row| row.reads_l1())
        .filter_map(|row| row.l1_info.map(|info| info.number));
    let first = numbers.next()?;
    Some(numbers.fold((first, first), |(low, high), number| {
        (low.min(number), high.max(number))
    }))
}
