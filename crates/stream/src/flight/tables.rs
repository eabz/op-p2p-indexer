//! Converts blocks into record batches for the four Arrow tables Flight serves.
//!
//! Each table builds a list of columns and checks it against the shared API schema.
//! Hashes are `FixedSizeBinary(32)`, addresses `FixedSizeBinary(20)`, wei amounts
//! `FixedSizeBinary(32)` big-endian (a 256-bit value does not fit `Decimal256`, whose 76 digits
//! stop short of 2^256), timestamps `Timestamp(Second, UTC)`.
//!
//! `blocks` reads the header alone; the other tables decode the block. CPU work: callers run
//! it off the async runtime.

use std::borrow::Cow;
use std::sync::Arc;

use alloy_consensus::Transaction;
use alloy_eips::Typed2718;
use alloy_primitives::{Address, B256, Log, U256};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, RecordBatch, StringArray,
    TimestampSecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_flight::error::FlightError;
use arrow_schema::ArrowError;
use op_alloy_consensus::{OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_api::ticket::Table;
use op_indexer_primitives::{DecodedBlock, L1Heads};

use crate::convert::{Prepared, encoded, max_fee_per_gas, nonce, status};
use crate::proto;

/// A column: its name, its values, and whether it may be null.
type Column = (&'static str, ArrayRef, bool);

/// Converts shared table identifiers into this server's Arrow rows.
pub(super) trait TableRows {
    fn batch(self, blocks: &[Prepared], heads: &L1Heads) -> Result<RecordBatch, FlightError>;
}
impl TableRows for Table {
    /// The table's rows for `blocks`, as one record batch, with each block's status under
    /// `heads`. Blocks without receipts add no rows to `receipts` and `logs`.
    fn batch(self, blocks: &[Prepared], heads: &L1Heads) -> Result<RecordBatch, FlightError> {
        let columns = match self {
            Self::Blocks => blocks_columns(blocks, heads)?,
            Self::Transactions => transactions_columns(&decode(blocks)?)?,
            Self::Receipts => receipts_columns(&decode(blocks)?)?,
            Self::Logs => logs_columns(&decode(blocks)?)?,
        };
        // Check every batch against the same contract advertised by the directory.
        let batch = RecordBatch::try_from_iter_with_nullable(columns)?;
        if batch.schema() != self.schema() {
            return Err(ArrowError::SchemaError(format!(
                "{} columns do not match the shared API schema",
                self.name()
            ))
            .into());
        }
        Ok(batch)
    }
}

/// Every block decoded.
fn decode(blocks: &[Prepared]) -> Result<Vec<Cow<'_, DecodedBlock>>, FlightError> {
    blocks
        .iter()
        .map(Prepared::decoded)
        .collect::<Result<_, _>>()
        .map_err(|err| FlightError::ExternalError(Box::new(err)))
}

fn u64s(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(values))
}

fn opt_u64s(values: impl Iterator<Item = Option<u64>>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter(values))
}

fn u32s(values: impl Iterator<Item = usize>) -> ArrayRef {
    Arc::new(UInt32Array::from_iter_values(
        values.map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
    ))
}

fn bools(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values))
}

fn binary<'a>(values: impl Iterator<Item = &'a [u8]>) -> ArrayRef {
    Arc::new(BinaryArray::from_iter_values(values))
}

fn times(values: impl Iterator<Item = u64>) -> ArrayRef {
    let seconds = values.map(|value| i64::try_from(value).unwrap_or(i64::MAX));
    Arc::new(TimestampSecondArray::from_iter_values(seconds).with_timezone("UTC"))
}

/// A fixed-size binary column of `width` bytes; `None` is null.
fn fixed<T: AsRef<[u8]>>(
    values: impl Iterator<Item = Option<T>>,
    width: i32,
) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(values, width)?,
    ))
}

fn hashes(values: impl Iterator<Item = Option<B256>>) -> Result<ArrayRef, ArrowError> {
    fixed(values, 32)
}

fn addresses(values: impl Iterator<Item = Option<Address>>) -> Result<ArrayRef, ArrowError> {
    fixed(values, 20)
}

/// Wei amounts, 32 bytes big-endian each.
fn amounts(values: impl Iterator<Item = Option<U256>>) -> Result<ArrayRef, ArrowError> {
    fixed(
        values.map(|value| value.map(|value| value.to_be_bytes::<32>())),
        32,
    )
}

const fn status_name(status: proto::Status) -> &'static str {
    match status {
        proto::Status::Finalized => "finalized",
        proto::Status::Safe => "safe",
        proto::Status::Unsafe | proto::Status::Unspecified => "unsafe",
    }
}

/// The columns of the block each row belongs to, which the other tables start with.
fn block_columns(blocks: &[&DecodedBlock]) -> Result<Vec<Column>, ArrowError> {
    let headers = || blocks.iter().map(|block| &block.block.header);
    Ok(vec![
        (
            "block_number",
            u64s(headers().map(|header| header.number)),
            false,
        ),
        (
            "block_hash",
            hashes(blocks.iter().map(|block| Some(block.hash)))?,
            false,
        ),
        (
            "block_timestamp",
            times(headers().map(|header| header.timestamp)),
            false,
        ),
    ])
}

fn blocks_columns(blocks: &[Prepared], heads: &L1Heads) -> Result<Vec<Column>, ArrowError> {
    let headers = || blocks.iter().map(|block| &block.header);
    let roots = |root: fn(&alloy_consensus::Header) -> Option<B256>| hashes(headers().map(root));
    Ok(vec![
        ("number", u64s(headers().map(|header| header.number)), false),
        (
            "hash",
            hashes(blocks.iter().map(|block| Some(block.at.hash)))?,
            false,
        ),
        (
            "parent_hash",
            roots(|header| Some(header.parent_hash))?,
            false,
        ),
        (
            "timestamp",
            times(headers().map(|header| header.timestamp)),
            false,
        ),
        (
            "fee_recipient",
            addresses(headers().map(|header| Some(header.beneficiary)))?,
            false,
        ),
        (
            "state_root",
            roots(|header| Some(header.state_root))?,
            false,
        ),
        (
            "transactions_root",
            roots(|header| Some(header.transactions_root))?,
            false,
        ),
        (
            "receipts_root",
            roots(|header| Some(header.receipts_root))?,
            false,
        ),
        (
            "logs_bloom",
            fixed(headers().map(|header| Some(header.logs_bloom)), 256)?,
            false,
        ),
        ("prev_randao", roots(|header| Some(header.mix_hash))?, false),
        (
            "gas_limit",
            u64s(headers().map(|header| header.gas_limit)),
            false,
        ),
        (
            "gas_used",
            u64s(headers().map(|header| header.gas_used)),
            false,
        ),
        (
            "base_fee_per_gas",
            opt_u64s(headers().map(|header| header.base_fee_per_gas)),
            true,
        ),
        (
            "extra_data",
            binary(headers().map(|header| header.extra_data.as_ref())),
            false,
        ),
        (
            "tx_count",
            u32s(blocks.iter().map(|block| block.tx_count)),
            false,
        ),
        (
            "withdrawals_root",
            roots(|header| header.withdrawals_root)?,
            true,
        ),
        (
            "blob_gas_used",
            opt_u64s(headers().map(|header| header.blob_gas_used)),
            true,
        ),
        (
            "excess_blob_gas",
            opt_u64s(headers().map(|header| header.excess_blob_gas)),
            true,
        ),
        (
            "parent_beacon_block_root",
            roots(|header| header.parent_beacon_block_root)?,
            true,
        ),
        ("requests_hash", roots(|header| header.requests_hash)?, true),
        (
            "has_receipts",
            bools(blocks.iter().map(|block| Some(block.has_receipts()))),
            false,
        ),
        ("status", statuses(blocks, heads), false),
    ])
}

/// Each block's status under `heads`, by name.
fn statuses(blocks: &[Prepared], heads: &L1Heads) -> ArrayRef {
    let names = blocks
        .iter()
        .map(|block| status_name(status(block.at.number, heads)));
    Arc::new(StringArray::from_iter_values(names))
}

/// A transaction, with the block it is in.
struct Tx<'a> {
    block: &'a DecodedBlock,
    index: usize,
    transaction: &'a OpTxEnvelope,
    sender: Address,
}

fn transactions_columns(blocks: &[Cow<'_, DecodedBlock>]) -> Result<Vec<Column>, ArrowError> {
    let rows: Vec<Tx<'_>> = blocks
        .iter()
        .flat_map(|block| {
            let transactions = block.block.body.transactions.iter().zip(&block.senders);
            transactions
                .enumerate()
                .map(|(index, (transaction, sender))| Tx {
                    block,
                    index,
                    transaction,
                    sender: *sender,
                })
        })
        .collect();
    let txs = || rows.iter().map(|row| row.transaction);
    let deposits = || txs().map(OpTxEnvelope::as_deposit);
    let encodings: Vec<Vec<u8>> = txs().map(encoded).collect();
    let mut columns = block_columns(&rows.iter().map(|row| row.block).collect::<Vec<_>>())?;
    columns.extend([
        ("tx_index", u32s(rows.iter().map(|row| row.index)), false),
        ("hash", hashes(txs().map(|tx| Some(tx.tx_hash())))?, false),
        (
            "tx_type",
            Arc::new(UInt8Array::from_iter_values(txs().map(Typed2718::ty))) as ArrayRef,
            false,
        ),
        (
            "sender",
            addresses(rows.iter().map(|row| Some(row.sender)))?,
            false,
        ),
        ("to", addresses(txs().map(Transaction::to))?, true),
        ("nonce", opt_u64s(txs().map(nonce)), true),
        ("value", amounts(txs().map(|tx| Some(tx.value())))?, false),
        ("gas_limit", u64s(txs().map(Transaction::gas_limit)), false),
        (
            "gas_price",
            amounts(txs().map(|tx| tx.gas_price().map(U256::from)))?,
            true,
        ),
        (
            "max_fee_per_gas",
            amounts(txs().map(|tx| max_fee_per_gas(tx).map(U256::from)))?,
            true,
        ),
        (
            "max_priority_fee_per_gas",
            amounts(txs().map(|tx| tx.max_priority_fee_per_gas().map(U256::from)))?,
            true,
        ),
        ("input", binary(txs().map(|tx| tx.input().as_ref())), false),
        (
            "source_hash",
            hashes(deposits().map(|deposit| deposit.map(|d| d.source_hash)))?,
            true,
        ),
        (
            "mint",
            amounts(deposits().map(|deposit| deposit.map(|d| U256::from(d.mint))))?,
            true,
        ),
        (
            "is_system_tx",
            bools(deposits().map(|deposit| deposit.map(|d| d.is_system_transaction))),
            true,
        ),
        (
            "encoded",
            binary(encodings.iter().map(Vec::as_slice)),
            false,
        ),
    ]);
    Ok(columns)
}

/// A receipt, with its block, its transaction's index and hash, and the gas it used.
struct ReceiptRow<'a> {
    block: &'a DecodedBlock,
    index: usize,
    tx_hash: B256,
    receipt: &'a OpReceiptEnvelope,
    gas_used: u64,
}

/// The receipts of the blocks that have them, in order.
fn receipt_rows<'a>(blocks: &'a [Cow<'_, DecodedBlock>]) -> Vec<ReceiptRow<'a>> {
    let mut rows = Vec::new();
    for block in blocks {
        let Some(receipts) = &block.receipts else {
            continue;
        };
        // A receipt records the gas its block used so far: its own is the difference.
        let mut before = 0_u64;
        let transactions = block.block.body.transactions.iter();
        for (index, (receipt, transaction)) in receipts.iter().zip(transactions).enumerate() {
            let cumulative = receipt.cumulative_gas_used();
            rows.push(ReceiptRow {
                block,
                index,
                tx_hash: transaction.tx_hash(),
                receipt,
                gas_used: cumulative.saturating_sub(before),
            });
            before = cumulative;
        }
    }
    rows
}

fn receipts_columns(blocks: &[Cow<'_, DecodedBlock>]) -> Result<Vec<Column>, ArrowError> {
    let rows = receipt_rows(blocks);
    let receipts = || rows.iter().map(|row| row.receipt);
    let mut columns = block_columns(&rows.iter().map(|row| row.block).collect::<Vec<_>>())?;
    columns.extend([
        ("tx_index", u32s(rows.iter().map(|row| row.index)), false),
        (
            "tx_hash",
            hashes(rows.iter().map(|row| Some(row.tx_hash)))?,
            false,
        ),
        (
            "success",
            bools(receipts().map(|receipt| Some(receipt.status()))),
            false,
        ),
        (
            "cumulative_gas_used",
            u64s(receipts().map(OpReceiptEnvelope::cumulative_gas_used)),
            false,
        ),
        ("gas_used", u64s(rows.iter().map(|row| row.gas_used)), false),
        (
            "logs_count",
            u32s(receipts().map(|receipt| receipt.logs().len())),
            false,
        ),
        (
            "deposit_nonce",
            opt_u64s(receipts().map(OpReceiptEnvelope::deposit_nonce)),
            true,
        ),
        (
            "deposit_receipt_version",
            opt_u64s(receipts().map(OpReceiptEnvelope::deposit_receipt_version)),
            true,
        ),
    ]);
    Ok(columns)
}

fn logs_columns(blocks: &[Cow<'_, DecodedBlock>]) -> Result<Vec<Column>, ArrowError> {
    // Each log with its receipt's row, and its index within the block.
    let mut rows: Vec<(&ReceiptRow<'_>, usize, &Log)> = Vec::new();
    let receipts = receipt_rows(blocks);
    let mut in_block = 0_usize;
    let mut previous: Option<B256> = None;
    for receipt in &receipts {
        if previous != Some(receipt.block.hash) {
            in_block = 0;
            previous = Some(receipt.block.hash);
        }
        for log in receipt.receipt.logs() {
            rows.push((receipt, in_block, log));
            in_block = in_block.saturating_add(1);
        }
    }
    let logs = || rows.iter().map(|(_, _, log)| *log);
    let topic = |position: usize| hashes(logs().map(|log| log.topics().get(position).copied()));
    let mut columns = block_columns(
        &rows
            .iter()
            .map(|(receipt, _, _)| receipt.block)
            .collect::<Vec<_>>(),
    )?;
    columns.extend([
        (
            "log_index",
            u32s(rows.iter().map(|(_, index, _)| *index)),
            false,
        ),
        (
            "tx_index",
            u32s(rows.iter().map(|(receipt, _, _)| receipt.index)),
            false,
        ),
        (
            "tx_hash",
            hashes(rows.iter().map(|(receipt, _, _)| Some(receipt.tx_hash)))?,
            false,
        ),
        (
            "address",
            addresses(logs().map(|log| Some(log.address)))?,
            false,
        ),
        ("topic0", topic(0)?, true),
        ("topic1", topic(1)?, true),
        ("topic2", topic(2)?, true),
        ("topic3", topic(3)?, true),
        (
            "data",
            binary(logs().map(|log| log.data.data.as_ref())),
            false,
        ),
    ]);
    Ok(columns)
}
