//! Rebuilds the blocks of one downloaded chunk and checks them.
//!
//! For each block, from the rows the service returned:
//!
//! 1. each transaction is encoded (`transaction`) and the transactions root computed over
//!    those encodings;
//! 2. each receipt is rebuilt (`receipt`) and the receipts root computed;
//! 3. the header is rebuilt with those two roots and the union of the receipts' blooms; the
//!    keccak of its RLP must be the reported block hash, which proves the roots, the bloom,
//!    and through them every transaction and receipt (none of the three is downloaded);
//! 4. its parent hash must be the previous block's hash.
//!
//! The bytes that passed are what is sealed: nothing is encoded again later.
//!
//! `fields` lists what this rebuild needs from a row, for `download`'s one-pass report: a field
//! the rebuild starts to need, or to default, goes there too.

use std::path::Path;

use alloy_consensus::{EMPTY_OMMER_ROOT_HASH, Header};
use alloy_eips::eip1559::INITIAL_BASE_FEE;
use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
use alloy_primitives::{B256, Bloom, keccak256};
use op_indexer_primitives::{
    ArchivedBlock, EncodedBlock, encode_body, encode_receipts, receipts_root, transactions_root,
};

use tracing::info;

use super::receipt::BloomHashes;
use super::{Check, ChunkError, Forks, Stats, receipt, transaction};
use crate::fill::{self, Fill};
use crate::rows::{self, BlockRow, LogRow, TransactionRow};
use crate::state::{Chunk, read_json};

/// Verifies the downloaded chunk at `raw`, with what its fill at `fill` holds, and returns its
/// blocks as verified, in block order. Blocking, CPU-bound.
pub(super) fn verify_chunk(
    forks: &Forks,
    chunk: Chunk,
    raw: &Path,
    fill: &Path,
) -> Result<(Stats, Vec<ArchivedBlock>), ChunkError> {
    let mut rows = rows::read(raw).map_err(|source| ChunkError::Rows {
        from: chunk.from,
        to: chunk.to,
        source,
    })?;
    let mut stats = Stats::default();
    if let Some(fill) = read_json::<Fill>(fill).map_err(ChunkError::Fill)? {
        stats.rpc_filled_transactions = fill::apply(&mut rows, fill);
    }
    let mut block_rows = rows.blocks.iter().peekable();
    let mut transactions = rows.transactions.as_slice();
    let mut logs = rows.logs.as_slice();

    let mut hashes = BloomHashes::default();
    let mut blocks: Vec<ArchivedBlock> = Vec::new();
    for number in chunk.from..chunk.to {
        let failed = |check| ChunkError::Block { number, check };
        // Rows of blocks outside the chunk, if the service sent any, are not used.
        while block_rows.next_if(|row| row.number < number).is_some() {}
        let _before = take_while(&mut transactions, |tx| tx.block_number < number);
        let _before = take_while(&mut logs, |log| log.block_number < number);
        let row = block_rows
            .next_if(|row| row.number == number)
            .ok_or_else(|| failed(Check::Missing))?;
        let block_transactions = take_while(&mut transactions, |tx| tx.block_number == number);
        let block_logs = take_while(&mut logs, |log| log.block_number == number);

        let block = verify_block(
            forks,
            row,
            block_transactions,
            block_logs,
            &mut hashes,
            &mut stats,
        )
        .map_err(failed)?;
        if let Some(previous) = blocks.last()
            && previous.encoded.hash != row.parent_hash
        {
            return Err(failed(Check::ParentLink {
                parent: row.parent_hash,
                previous: previous.encoded.hash,
            }));
        }
        stats.blocks = stats.blocks.saturating_add(1);
        stats.transactions = stats
            .transactions
            .saturating_add(u64::try_from(block.senders.len()).unwrap_or(u64::MAX));
        blocks.push(block);
    }
    if stats.rebuilt_header_fields > 0 {
        info!(
            from = chunk.from,
            to = chunk.to,
            rebuilt_header_fields = stats.rebuilt_header_fields,
            "blocks without `mix_hash` rebuilt with zero, proven by their hash"
        );
    }
    Ok((stats, blocks))
}

/// Rebuilds one block from its rows and checks it against the reported block hash. Returns
/// the verified encodings, and counts in `stats` the transactions signed with all zeros and
/// a header whose missing `mix_hash` was taken as zero (up to the Bedrock block only).
fn verify_block(
    forks: &Forks,
    row: &BlockRow,
    transactions: &[TransactionRow],
    mut logs: &[LogRow],
    hashes: &mut BloomHashes,
    stats: &mut Stats,
) -> Result<ArchivedBlock, Check> {
    // Every block after the Bedrock block has the L1-attributes deposit: none is a block whose
    // rows the service left out, which the fill brings.
    if transactions.is_empty() && row.number > forks.bedrock_block {
        return Err(Check::Hole);
    }
    let timestamp: u64 = row.timestamp.to();
    // Every legacy header has a zero `mix_hash`, and so has the Bedrock block itself (the
    // genesis of a chain that began with Bedrock); the header hash below proves the guess.
    // After Bedrock it is the L1 block's randomness and cannot be rebuilt.
    let (mix_hash, rebuilt) = match row.mix_hash {
        Some(mix_hash) => (mix_hash, false),
        None if row.number <= forks.bedrock_block => (B256::ZERO, true),
        None => return Err(Check::MissingMixHash),
    };
    // The Bedrock block is the chain's first London block, whose base fee is EIP-1559's
    // initial one; the header hash proves the guess. Any later block's base fee is required.
    let (base_fee, rebuilt_fee) = match row.base_fee_per_gas {
        Some(fee) => (Some(fee.to()), false),
        None if row.number == forks.bedrock_block => (Some(INITIAL_BASE_FEE), true),
        None => (None, false),
    };
    stats.rebuilt_header_fields = stats
        .rebuilt_header_fields
        .saturating_add(u64::from(rebuilt))
        .saturating_add(u64::from(rebuilt_fee));
    let mut encodings = Vec::with_capacity(transactions.len());
    let mut senders = Vec::with_capacity(transactions.len());
    let mut receipts = Vec::with_capacity(transactions.len());
    let mut logs_bloom = Bloom::ZERO;
    for (index, tx) in (0_u64..).zip(transactions) {
        if tx.transaction_index != index {
            return Err(Check::TransactionIndex {
                position: index,
                found: tx.transaction_index,
            });
        }
        let mut encoding = Vec::new();
        let (sender, zero_signature) =
            transaction::encode(index, tx, timestamp < forks.regolith, &mut encoding)?;
        stats.zero_signatures = stats
            .zero_signatures
            .saturating_add(u64::from(zero_signature));
        encodings.push(encoding);
        senders.push(sender);

        let _before = take_while(&mut logs, |log| log.transaction_index < index);
        let tx_logs = take_while(&mut logs, |log| log.transaction_index == index);
        let receipt = receipt::rebuild(index, tx, tx_logs, timestamp >= forks.canyon, hashes)?;
        logs_bloom.accrue_bloom(receipt.logs_bloom());
        receipts.push(receipt);
    }

    // The body is written without ommers, which the header's hash of them must agree with.
    if row.sha3_uncles != EMPTY_OMMER_ROOT_HASH {
        return Err(Check::Ommers(row.sha3_uncles));
    }
    let transactions_root = transactions_root(&encodings);
    let receipts_root = receipts_root(&receipts, timestamp, forks.canyon);
    let header = encode_header(
        forks,
        row,
        transactions_root,
        receipts_root,
        logs_bloom,
        (mix_hash, base_fee),
    );
    let computed = keccak256(&header);
    if computed != row.hash {
        return Err(Check::HeaderHash {
            computed,
            reported: row.hash,
        });
    }

    let block = ArchivedBlock {
        encoded: EncodedBlock {
            hash: row.hash,
            header: header.into(),
            body: encode_body(&encodings, row.withdrawals_root.is_some()),
            receipts: Some(encode_receipts(&receipts)),
        },
        senders,
    };
    Ok(block)
}

/// Encodes the header of `row` with the roots and the bloom computed from the block's
/// transactions, receipts and logs, and `mix_hash` and the base fee (the row's, or rebuilt
/// where the row lacks them, see [`verify_block`]).
fn encode_header(
    forks: &Forks,
    row: &BlockRow,
    transactions_root: B256,
    receipts_root: B256,
    logs_bloom: Bloom,
    (mix_hash, base_fee_per_gas): (B256, Option<u64>),
) -> Vec<u8> {
    let timestamp: u64 = row.timestamp.to();
    alloy_rlp::encode(Header {
        parent_hash: row.parent_hash,
        ommers_hash: row.sha3_uncles,
        beneficiary: row.miner,
        state_root: row.state_root,
        transactions_root,
        receipts_root,
        logs_bloom,
        difficulty: row.difficulty,
        number: row.number,
        gas_limit: row.gas_limit.to(),
        gas_used: row.gas_used.to(),
        timestamp,
        extra_data: row.extra_data.clone(),
        mix_hash,
        nonce: row.nonce,
        base_fee_per_gas,
        withdrawals_root: row.withdrawals_root,
        blob_gas_used: row.blob_gas_used.map(|gas| gas.to()),
        excess_blob_gas: row.excess_blob_gas.map(|gas| gas.to()),
        parent_beacon_block_root: row.parent_beacon_block_root,
        // OP Stack blocks have no execution requests; the service has no column for the hash.
        requests_hash: (row.withdrawals_root.is_some() && timestamp >= forks.isthmus)
            .then_some(EMPTY_REQUESTS_HASH),
        ..Default::default()
    })
}

/// Splits off the leading rows that satisfy `belongs`.
fn take_while<'a, T>(rows: &mut &'a [T], belongs: impl Fn(&T) -> bool) -> &'a [T] {
    let count = rows.iter().take_while(|row| belongs(row)).count();
    let (taken, rest) = rows.split_at(count);
    *rows = rest;
    taken
}
