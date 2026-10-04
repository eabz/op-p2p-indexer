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
//! The bytes that passed are what is written (see `chunk`): nothing is encoded again later.

use std::io;
use std::path::Path;

use alloy_consensus::{EMPTY_OMMER_ROOT_HASH, Header};
use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
use alloy_primitives::{B256, Bloom, keccak256};
use op_indexer_primitives::{
    EncodedBlock, encode_body, encode_receipts, receipts_root, transactions_root,
};

use super::{Check, ChunkError, Forks, Stats, receipt, transaction};
use crate::chunk::{self, Link, VerifiedBlock};
use crate::rows::{self, BlockRow, LogRow, TransactionRow};
use crate::state::Chunk;

/// Verifies the downloaded chunk at `raw` and writes it to `verified`. Blocking, CPU-bound.
pub(super) fn verify_chunk(
    forks: &Forks,
    chunk: Chunk,
    raw: &Path,
    verified: &Path,
) -> Result<Stats, ChunkError> {
    let rows = rows::read(raw).map_err(|source| ChunkError::Rows {
        from: chunk.from,
        to: chunk.to,
        source,
    })?;
    let mut block_rows = rows.blocks.iter().peekable();
    let mut transactions = rows.transactions.as_slice();
    let mut logs = rows.logs.as_slice();

    let mut stats = Stats::default();
    let mut blocks = Vec::new();
    let mut link: Option<Link> = None;
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

        let (block, zero_signatures) =
            verify_block(forks, row, block_transactions, block_logs).map_err(failed)?;
        if let Some(link) = &link
            && link.last_hash != row.parent_hash
        {
            return Err(failed(Check::ParentLink {
                parent: row.parent_hash,
                previous: link.last_hash,
            }));
        }
        link = Some(Link {
            first_parent: link.map_or(row.parent_hash, |link| link.first_parent),
            last_hash: row.hash,
        });
        stats.blocks = stats.blocks.saturating_add(1);
        stats.transactions = stats
            .transactions
            .saturating_add(u64::try_from(block.senders.len()).unwrap_or(u64::MAX));
        stats.zero_signatures = stats.zero_signatures.saturating_add(zero_signatures);
        blocks.push(block);
    }
    if let Some(link) = link {
        chunk::write(verified, link, &blocks)?;
        stats.disk_bytes = std::fs::metadata(verified)?.len();
    }
    Ok(stats)
}

/// Rebuilds one block from its rows and checks it against the reported block hash. Returns
/// the verified encodings and the number of transactions signed with all zeros.
fn verify_block(
    forks: &Forks,
    row: &BlockRow,
    transactions: &[TransactionRow],
    mut logs: &[LogRow],
) -> Result<(VerifiedBlock, u64), Check> {
    let timestamp: u64 = row.timestamp.to();
    let mut encodings = Vec::with_capacity(transactions.len());
    let mut senders = Vec::with_capacity(transactions.len());
    let mut receipts = Vec::with_capacity(transactions.len());
    let mut logs_bloom = Bloom::ZERO;
    let mut zero_signatures = 0_u64;
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
        zero_signatures = zero_signatures.saturating_add(u64::from(zero_signature));
        encodings.push(encoding);
        senders.push(sender);

        let _before = take_while(&mut logs, |log| log.transaction_index < index);
        let tx_logs = take_while(&mut logs, |log| log.transaction_index == index);
        let receipt = receipt::rebuild(index, tx, tx_logs, timestamp >= forks.canyon)?;
        logs_bloom.accrue_bloom(receipt.logs_bloom());
        receipts.push(receipt);
    }

    // The body is written without ommers, which the header's hash of them must agree with.
    if row.sha3_uncles != EMPTY_OMMER_ROOT_HASH {
        return Err(Check::Ommers(row.sha3_uncles));
    }
    let transactions_root = transactions_root(&encodings);
    let receipts_root = receipts_root(&receipts, timestamp, forks.canyon);
    let header = encode_header(forks, row, transactions_root, receipts_root, logs_bloom);
    let computed = keccak256(&header);
    if computed != row.hash {
        return Err(Check::HeaderHash {
            computed,
            reported: row.hash,
        });
    }

    let block = VerifiedBlock {
        encoded: EncodedBlock {
            hash: row.hash,
            header: header.into(),
            body: encode_body(&encodings, row.withdrawals_root.is_some()),
            receipts: Some(encode_receipts(&receipts)),
        },
        senders,
    };
    Ok((block, zero_signatures))
}

/// Encodes the header of `row` with the roots and the bloom computed from the block's
/// transactions, receipts and logs.
fn encode_header(
    forks: &Forks,
    row: &BlockRow,
    transactions_root: B256,
    receipts_root: B256,
    logs_bloom: Bloom,
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
        mix_hash: row.mix_hash,
        nonce: row.nonce,
        base_fee_per_gas: row.base_fee_per_gas.map(|fee| fee.to()),
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

/// Maps a failed write of the verified chunk.
impl From<io::Error> for ChunkError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
