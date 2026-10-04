//! The rows of a downloaded chunk, as `HyperSync`'s JSON gives them.
//!
//! Quantities are hex strings; block numbers and indexes are JSON numbers; a field the block
//! or transaction does not have is absent or null. Fields this tool does not read (the blooms,
//! the L1 fee fields) are skipped. Nothing here is verified: see `verify`.

use std::fs::File;
use std::io;
use std::path::Path;

use alloy_primitives::{Address, B64, B256, Bytes, U64, U128, U256};
use serde::Deserialize;

/// One answer of the service.
#[derive(Debug, Deserialize)]
struct Response {
    data: Vec<Batch>,
}

/// One batch of an answer.
#[derive(Debug, Deserialize)]
struct Batch {
    #[serde(default)]
    blocks: Vec<BlockRow>,
    #[serde(default)]
    transactions: Vec<TransactionRow>,
    #[serde(default)]
    logs: Vec<LogRow>,
}

/// A block header.
#[derive(Debug, Deserialize)]
pub(crate) struct BlockRow {
    pub(crate) number: u64,
    pub(crate) hash: B256,
    pub(crate) parent_hash: B256,
    pub(crate) sha3_uncles: B256,
    pub(crate) miner: Address,
    pub(crate) state_root: B256,
    pub(crate) transactions_root: B256,
    pub(crate) receipts_root: B256,
    pub(crate) difficulty: U256,
    pub(crate) gas_limit: U64,
    pub(crate) gas_used: U64,
    pub(crate) timestamp: U64,
    pub(crate) extra_data: Bytes,
    pub(crate) mix_hash: B256,
    pub(crate) nonce: B64,
    pub(crate) base_fee_per_gas: Option<U64>,
    pub(crate) withdrawals_root: Option<B256>,
    pub(crate) blob_gas_used: Option<U64>,
    pub(crate) excess_blob_gas: Option<U64>,
    pub(crate) parent_beacon_block_root: Option<B256>,
}

/// A transaction with the fields of its receipt.
#[derive(Debug, Deserialize)]
pub(crate) struct TransactionRow {
    pub(crate) block_number: u64,
    pub(crate) transaction_index: u64,
    pub(crate) hash: B256,
    pub(crate) from: Option<Address>,
    pub(crate) to: Option<Address>,
    pub(crate) gas: U64,
    pub(crate) gas_price: Option<U128>,
    pub(crate) input: Bytes,
    pub(crate) value: U256,
    pub(crate) nonce: U64,
    pub(crate) v: Option<U256>,
    pub(crate) r: Option<U256>,
    pub(crate) s: Option<U256>,
    /// The transaction type; absent means legacy.
    #[serde(rename = "type")]
    pub(crate) kind: Option<u8>,
    pub(crate) status: Option<u8>,
    /// State root of a receipt from before Byzantium, in place of the status.
    pub(crate) root: Option<B256>,
    pub(crate) cumulative_gas_used: U64,
    pub(crate) chain_id: Option<U64>,
    pub(crate) max_fee_per_gas: Option<U128>,
    pub(crate) max_priority_fee_per_gas: Option<U128>,
    /// Parity of a typed transaction's signature, where the service gives it apart from `v`.
    pub(crate) y_parity: Option<U256>,
    /// The access list in the Ethereum RPC's JSON form; parsed when the transaction is rebuilt,
    /// so an unexpected form fails that transaction's check, not the whole file.
    pub(crate) access_list: Option<serde_json::Value>,
    /// The EIP-7702 authorizations, as the access list.
    pub(crate) authorization_list: Option<serde_json::Value>,
    /// Deposit transactions: the hash that identifies the deposit's origin.
    pub(crate) source_hash: Option<B256>,
    /// Deposit transactions: ETH minted on L2.
    pub(crate) mint: Option<U128>,
    /// Deposit receipts: the sender's nonce before the transaction.
    pub(crate) deposit_nonce: Option<U64>,
    /// Deposit receipts: 1 from Canyon on.
    pub(crate) deposit_receipt_version: Option<U64>,
}

/// A log.
#[derive(Debug, Deserialize)]
pub(crate) struct LogRow {
    pub(crate) block_number: u64,
    pub(crate) transaction_index: u64,
    pub(crate) log_index: u64,
    pub(crate) address: Address,
    pub(crate) data: Bytes,
    pub(crate) topic0: Option<B256>,
    pub(crate) topic1: Option<B256>,
    pub(crate) topic2: Option<B256>,
    pub(crate) topic3: Option<B256>,
}

/// An L1 transaction, as far as the lookup of dispute games reads it.
#[derive(Debug, Deserialize)]
pub(crate) struct L1TransactionRow {
    pub(crate) block_number: u64,
    pub(crate) transaction_index: u64,
    pub(crate) to: Option<Address>,
    pub(crate) input: Bytes,
}

/// The rows of a chunk, each kind in block order.
#[derive(Debug, Default)]
pub(crate) struct Rows {
    /// By block number.
    pub(crate) blocks: Vec<BlockRow>,
    /// By block number, then index in the block.
    pub(crate) transactions: Vec<TransactionRow>,
    /// By block number, then transaction index, then log index.
    pub(crate) logs: Vec<LogRow>,
}

/// Reads a downloaded chunk: one or more answers, compressed, one after the other. Blocking.
///
/// # Errors
///
/// Returns the I/O error; `InvalidData` if the content is not the expected JSON.
pub(crate) fn read(path: &Path) -> io::Result<Rows> {
    let data = zstd::stream::decode_all(File::open(path)?)?;
    let mut rows = Rows::default();
    for response in serde_json::Deserializer::from_slice(&data).into_iter::<Response>() {
        for batch in response?.data {
            rows.blocks.extend(batch.blocks);
            rows.transactions.extend(batch.transactions);
            rows.logs.extend(batch.logs);
        }
    }
    rows.blocks.sort_unstable_by_key(|block| block.number);
    rows.transactions
        .sort_unstable_by_key(|tx| (tx.block_number, tx.transaction_index));
    rows.logs
        .sort_unstable_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    // Answers of one chunk do not overlap, but nothing downstream should depend on that.
    rows.blocks.dedup_by_key(|block| block.number);
    rows.transactions
        .dedup_by_key(|tx| (tx.block_number, tx.transaction_index));
    rows.logs
        .dedup_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    Ok(rows)
}
