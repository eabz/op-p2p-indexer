//! ClickHouse rows, column for column with `migrations/clickhouse/`, and the mapping from an
//! [`DecodedBlock`] to them.
//!
//! Hashes are `FixedString(32)` (`[u8; 32]`), addresses `FixedString(20)`, wei amounts
//! `UInt256`, timestamps `DateTime('UTC')` (`u32` seconds) and `version` / `updated_at` `UInt64`
//! microseconds. Byte strings are `String` columns written with `serde_bytes`.
//!
//! Does not check that a block is storable beyond what building a row needs (the shared
//! `validate_block` runs first), and does not talk to ClickHouse.
//!
//! Integer conversion errors are discarded as `_out_of_range`: they carry no information beyond
//! the failed conversion, which the [`InvalidBlockReason`] names.

use alloy_consensus::Transaction;
use alloy_primitives::{Address, B256, BlockNumber, ChainId, U256};
use clickhouse::Row;
use clickhouse::types::UInt256;
use op_alloy_consensus::{OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_primitives::{BlockRef, BlockSource, DecodedBlock, encode_transaction};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize, Serializer};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{InvalidBlockReason, StorageError};

/// `chain_state.key`: `Enum8('safe_head' = 0, 'finalized_head' = 1)`.
#[derive(Debug, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub(super) enum HeadKey {
    SafeHead = 0,
    FinalizedHead = 1,
}

/// All rows for a batch of blocks, one vector per table.
#[derive(Debug, Default)]
pub(super) struct Rows {
    pub(super) blocks: Vec<BlockRow>,
    pub(super) transactions: Vec<TransactionRow>,
    pub(super) receipts: Vec<ReceiptRow>,
    pub(super) logs: Vec<LogRow>,
}

/// A row of `blocks`.
#[derive(Debug, Row, Serialize)]
pub(super) struct BlockRow {
    chain_id: ChainId,
    number: BlockNumber,
    hash: [u8; 32],
    parent_hash: [u8; 32],
    timestamp: u32,
    fee_recipient: [u8; 20],
    state_root: [u8; 32],
    transactions_root: [u8; 32],
    receipts_root: [u8; 32],
    #[serde(serialize_with = "fixed_string")]
    logs_bloom: [u8; 256],
    prev_randao: [u8; 32],
    gas_limit: u64,
    gas_used: u64,
    base_fee_per_gas: Option<u64>,
    #[serde(with = "serde_bytes")]
    extra_data: Vec<u8>,
    tx_count: u32,
    withdrawals_root: Option<[u8; 32]>,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
    parent_beacon_block_root: Option<[u8; 32]>,
    requests_hash: Option<[u8; 32]>,
    source: SourceColumn,
    has_receipts: bool,
    #[serde(rename = "version")]
    version_micros: u64,
}

/// A row of `transactions`.
#[derive(Debug, Row, Serialize)]
pub(super) struct TransactionRow {
    chain_id: ChainId,
    block_number: BlockNumber,
    tx_index: u32,
    block_hash: [u8; 32],
    block_timestamp: u32,
    hash: [u8; 32],
    tx_type: u8,
    from: [u8; 20],
    to: Option<[u8; 20]>,
    nonce: Option<u64>,
    value: UInt256,
    gas_limit: u64,
    gas_price: Option<u128>,
    max_fee_per_gas: Option<u128>,
    max_priority_fee_per_gas: Option<u128>,
    #[serde(with = "serde_bytes")]
    input: Vec<u8>,
    source_hash: Option<[u8; 32]>,
    mint: Option<UInt256>,
    is_system_tx: Option<bool>,
    #[serde(with = "serde_bytes")]
    raw: Vec<u8>,
    #[serde(rename = "version")]
    version_micros: u64,
}

/// A row of `receipts`.
#[derive(Debug, Row, Serialize)]
pub(super) struct ReceiptRow {
    chain_id: ChainId,
    block_number: BlockNumber,
    tx_index: u32,
    block_hash: [u8; 32],
    block_timestamp: u32,
    tx_hash: [u8; 32],
    status: u8,
    cumulative_gas_used: u64,
    logs_count: u32,
    deposit_nonce: Option<u64>,
    deposit_receipt_version: Option<u64>,
    #[serde(rename = "version")]
    version_micros: u64,
}

/// A row of `logs`.
#[derive(Debug, Row, Serialize)]
pub(super) struct LogRow {
    chain_id: ChainId,
    block_number: BlockNumber,
    log_index: u32,
    block_hash: [u8; 32],
    block_timestamp: u32,
    tx_index: u32,
    tx_hash: [u8; 32],
    address: [u8; 20],
    topic0: Option<[u8; 32]>,
    topic1: Option<[u8; 32]>,
    topic2: Option<[u8; 32]>,
    topic3: Option<[u8; 32]>,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
    #[serde(rename = "version")]
    version_micros: u64,
}

/// A row of `chain_state`, written by [`crate::CommittedStore::set_l1_heads`] and
/// [`crate::CommittedStore::rollback_to`].
#[derive(Debug, Row, Serialize)]
pub(super) struct ChainStateRow {
    chain_id: ChainId,
    key: HeadKey,
    number: BlockNumber,
    hash: [u8; 32],
    #[serde(rename = "updated_at")]
    updated_at_micros: u64,
}

/// A head read back from `chain_state`.
#[derive(Debug, Row, Deserialize)]
pub(super) struct StoredHead {
    key: HeadKey,
    number: BlockNumber,
    hash: [u8; 32],
}

/// `blocks.source`: `Enum8('gossip' = 0, 'l1' = 1, 'import' = 2, 'sync' = 3)`.
#[derive(Debug, Serialize_repr)]
#[repr(i8)]
enum SourceColumn {
    Gossip = 0,
    L1 = 1,
    Import = 2,
    Sync = 3,
}

/// The block's columns repeated on each of its rows.
struct BlockColumns {
    chain_id: ChainId,
    number: BlockNumber,
    hash: [u8; 32],
    timestamp: u32,
    tx_count: u32,
    version_micros: u64,
}

impl Rows {
    /// Adds the rows of `block`, all stamped with `version_micros`.
    ///
    /// Receipts and logs are added only when the block has receipts.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] if the block does not fit the schema, and
    /// [`StorageError::UnsupportedTransaction`] for a transaction type without columns.
    pub(super) fn push(
        &mut self,
        chain_id: ChainId,
        block: &DecodedBlock,
        version_micros: u64,
    ) -> Result<(), StorageError> {
        let header = &block.block.header;
        let body = &block.block.body;
        let invalid = |reason| invalid_block(header.number, reason);
        let transactions = &body.transactions;
        let columns = BlockColumns {
            chain_id,
            number: header.number,
            hash: block.hash.0,
            timestamp: u32::try_from(header.timestamp)
                .map_err(|_out_of_range| invalid(InvalidBlockReason::TimestampRange))?,
            tx_count: u32::try_from(transactions.len())
                .map_err(|_out_of_range| invalid(InvalidBlockReason::TooManyTransactions))?,
            version_micros,
        };
        self.blocks.push(block_row(&columns, block));
        // `tx_count` fits in `u32`, so every index does too.
        for (tx_index, (tx, sender)) in (0..).zip(transactions.iter().zip(&block.senders)) {
            self.transactions
                .push(transaction_row(&columns, tx_index, tx, *sender)?);
        }
        let Some(receipts) = &block.receipts else {
            return Ok(());
        };
        self.push_receipts(&columns, receipts, transactions)
    }

    /// Adds the receipt and log rows of one block. `log_index` runs across the whole block.
    ///
    /// Deposit receipts also fill `deposit_nonce` and `deposit_receipt_version`; see
    /// <https://specs.optimism.io/protocol/deposits.html#deposit-receipt>.
    fn push_receipts(
        &mut self,
        block: &BlockColumns,
        receipts: &[OpReceiptEnvelope],
        transactions: &[OpTxEnvelope],
    ) -> Result<(), StorageError> {
        let mut log_index: u32 = 0;
        for (tx_index, (receipt, tx)) in (0..).zip(receipts.iter().zip(transactions)) {
            let tx_hash = tx.tx_hash().0;
            let logs = receipt.logs();
            self.receipts.push(ReceiptRow {
                chain_id: block.chain_id,
                block_number: block.number,
                tx_index,
                block_hash: block.hash,
                block_timestamp: block.timestamp,
                tx_hash,
                status: u8::from(receipt.status()),
                cumulative_gas_used: receipt.cumulative_gas_used(),
                logs_count: u32::try_from(logs.len())
                    .map_err(|_out_of_range| block.invalid(InvalidBlockReason::TooManyLogs))?,
                deposit_nonce: receipt.deposit_nonce(),
                deposit_receipt_version: receipt.deposit_receipt_version(),
                version_micros: block.version_micros,
            });
            for log in logs {
                let topics = log.data.topics();
                let topic = |i: usize| topics.get(i).map(|topic| topic.0);
                self.logs.push(LogRow {
                    chain_id: block.chain_id,
                    block_number: block.number,
                    log_index,
                    block_hash: block.hash,
                    block_timestamp: block.timestamp,
                    tx_index,
                    tx_hash,
                    address: log.address.into_array(),
                    topic0: topic(0),
                    topic1: topic(1),
                    topic2: topic(2),
                    topic3: topic(3),
                    data: log.data.data.to_vec(),
                    version_micros: block.version_micros,
                });
                log_index = log_index
                    .checked_add(1)
                    .ok_or_else(|| block.invalid(InvalidBlockReason::TooManyLogs))?;
            }
        }
        Ok(())
    }
}

impl ChainStateRow {
    /// A row recording `head` under `key`.
    pub(super) const fn new(
        chain_id: ChainId,
        key: HeadKey,
        head: BlockRef,
        updated_at_micros: u64,
    ) -> Self {
        Self {
            chain_id,
            key,
            number: head.number,
            hash: head.hash.0,
            updated_at_micros,
        }
    }
}

impl StoredHead {
    /// The head and which one it is.
    pub(super) const fn into_head(self) -> (HeadKey, BlockRef) {
        (
            self.key,
            BlockRef {
                number: self.number,
                hash: B256::new(self.hash),
            },
        )
    }
}

impl BlockColumns {
    const fn invalid(&self, reason: InvalidBlockReason) -> StorageError {
        invalid_block(self.number, reason)
    }
}

const fn invalid_block(number: BlockNumber, reason: InvalidBlockReason) -> StorageError {
    StorageError::InvalidBlock { number, reason }
}

/// Maps the block header.
fn block_row(columns: &BlockColumns, block: &DecodedBlock) -> BlockRow {
    let header = &block.block.header;
    BlockRow {
        chain_id: columns.chain_id,
        number: columns.number,
        hash: columns.hash,
        parent_hash: header.parent_hash.0,
        timestamp: columns.timestamp,
        fee_recipient: header.beneficiary.into_array(),
        state_root: header.state_root.0,
        transactions_root: header.transactions_root.0,
        receipts_root: header.receipts_root.0,
        logs_bloom: header.logs_bloom.0.0,
        prev_randao: header.mix_hash.0,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        base_fee_per_gas: header.base_fee_per_gas,
        extra_data: header.extra_data.to_vec(),
        tx_count: columns.tx_count,
        withdrawals_root: header.withdrawals_root.map(|root| root.0),
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_block_root: header.parent_beacon_block_root.map(|root| root.0),
        requests_hash: header.requests_hash.map(|hash| hash.0),
        source: match block.source {
            BlockSource::Gossip => SourceColumn::Gossip,
            BlockSource::L1 => SourceColumn::L1,
            BlockSource::Import => SourceColumn::Import,
            BlockSource::Sync => SourceColumn::Sync,
        },
        has_receipts: block.receipts.is_some(),
        version_micros: columns.version_micros,
    }
}

/// Maps one transaction. Fee columns come from the transaction type: `gas_price` for legacy and
/// EIP-2930, the EIP-1559 fee caps for EIP-1559 and EIP-7702.
///
/// Deposits have no nonce (theirs is assigned at execution, in the receipt) and fill
/// `source_hash`, `mint` and `is_system_tx`; see
/// <https://specs.optimism.io/protocol/deposits.html#the-deposited-transaction-type>.
/// [`OpTxEnvelope::PostExec`] has no columns and is rejected rather than stored partially.
fn transaction_row(
    block: &BlockColumns,
    tx_index: u32,
    tx: &OpTxEnvelope,
    sender: Address,
) -> Result<TransactionRow, StorageError> {
    let mut row = TransactionRow {
        chain_id: block.chain_id,
        block_number: block.number,
        tx_index,
        block_hash: block.hash,
        block_timestamp: block.timestamp,
        hash: tx.tx_hash().0,
        tx_type: u8::from(tx.tx_type()),
        from: sender.into_array(),
        to: tx.to().map(Address::into_array),
        nonce: Some(tx.nonce()),
        value: uint256(tx.value()),
        gas_limit: tx.gas_limit(),
        gas_price: tx.gas_price(),
        // `max_fee_per_gas()` falls back to the gas price for legacy and EIP-2930.
        max_fee_per_gas: tx.is_dynamic_fee().then(|| tx.max_fee_per_gas()),
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas(),
        input: tx.input().to_vec(),
        source_hash: None,
        mint: None,
        is_system_tx: None,
        raw: {
            let mut raw = Vec::new();
            encode_transaction(tx, &mut raw);
            raw
        },
        version_micros: block.version_micros,
    };
    match tx {
        OpTxEnvelope::Legacy(_)
        | OpTxEnvelope::Eip2930(_)
        | OpTxEnvelope::Eip1559(_)
        | OpTxEnvelope::Eip7702(_) => {}
        OpTxEnvelope::Deposit(deposit) => {
            row.nonce = None;
            row.source_hash = Some(deposit.source_hash.0);
            row.mint = Some(UInt256::from(deposit.mint));
            row.is_system_tx = Some(deposit.is_system_transaction);
        }
        OpTxEnvelope::PostExec(_) => {
            return Err(StorageError::UnsupportedTransaction {
                number: block.number,
                tx_type: row.tx_type,
            });
        }
    }
    Ok(row)
}

/// Writes a byte array longer than serde's 32-element limit as a `FixedString(N)`: an `N`-tuple
/// of bytes, the shape serde gives shorter arrays.
fn fixed_string<S: Serializer, const N: usize>(
    bytes: &[u8; N],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut tuple = serializer.serialize_tuple(N)?;
    for byte in bytes {
        tuple.serialize_element(byte)?;
    }
    tuple.end()
}

fn uint256(value: U256) -> UInt256 {
    UInt256::from_le_bytes(value.to_le_bytes())
}
