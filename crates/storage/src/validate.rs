//! The checks a block and its receipts must pass before any store takes them.
//!
//! Every store calls [`validate_block`] first, and [`validate_receipts`] wherever receipts are
//! attached later, so what one store accepts the others accept too. Holds only what does not
//! depend on a store's state; checks against stored data (the parent's number, the stored
//! number of a block) stay with the store that makes them.

use alloy_primitives::BlockNumber;
use op_alloy_consensus::{OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_primitives::DecodedBlock;

use crate::{InvalidBlockReason, StorageError};

/// Highest block number the unsafe store's scripts hold exactly: Lua numbers are doubles.
const MAX_BLOCK_NUMBER: u64 = 1 << 53;
/// Most topics an EVM log can have; the committed store has one column per topic.
const MAX_LOG_TOPICS: usize = 4;

/// Checks that `block` fits every store.
///
/// # Errors
///
/// Returns [`StorageError::UnsupportedTransaction`] for a transaction type the stores have no
/// place for, and [`StorageError::InvalidBlock`] with the first [`InvalidBlockReason`] that
/// applies otherwise.
pub(crate) fn validate_block(block: &DecodedBlock) -> Result<(), StorageError> {
    let header = &block.block.header;
    let body = &block.block.body;
    let number = header.number;
    let transactions = &body.transactions;
    let invalid = |reason| StorageError::InvalidBlock { number, reason };

    if let Some(unsupported) = transactions
        .iter()
        .find(|transaction| matches!(transaction, OpTxEnvelope::PostExec(_)))
    {
        return Err(StorageError::UnsupportedTransaction {
            number,
            tx_type: u8::from(unsupported.tx_type()),
        });
    }
    if number > MAX_BLOCK_NUMBER {
        return Err(invalid(InvalidBlockReason::NumberRange));
    }
    // Timestamps, transaction indexes and log indexes are 32-bit columns in the committed store.
    if u32::try_from(header.timestamp).is_err() {
        return Err(invalid(InvalidBlockReason::TimestampRange));
    }
    if u32::try_from(transactions.len()).is_err() {
        return Err(invalid(InvalidBlockReason::TooManyTransactions));
    }
    if block.senders.len() != transactions.len() {
        return Err(invalid(InvalidBlockReason::SenderCount));
    }
    // OP blocks have no ommers, and an empty withdrawals list since Canyon: neither is stored.
    if !body.ommers.is_empty() {
        return Err(invalid(InvalidBlockReason::Ommers));
    }
    if body.withdrawals.as_ref().is_some_and(|w| !w.is_empty()) {
        return Err(invalid(InvalidBlockReason::Withdrawals));
    }

    let Some(receipts) = &block.receipts else {
        return Ok(());
    };
    if receipts.len() != transactions.len() {
        return Err(invalid(InvalidBlockReason::ReceiptCount));
    }
    validate_receipts(number, receipts)
}

/// Checks that `receipts` fit every store: at most four topics per log, and no more logs than a
/// log index can count. Their number against the block's transactions is the caller's check.
///
/// Runs wherever receipts enter a store, with a block or attached later, so both paths accept
/// the same receipts.
///
/// # Errors
///
/// Returns [`StorageError::InvalidBlock`] with [`InvalidBlockReason::TooManyTopics`] or
/// [`InvalidBlockReason::TooManyLogs`].
pub(crate) fn validate_receipts(
    number: BlockNumber,
    receipts: &[OpReceiptEnvelope],
) -> Result<(), StorageError> {
    let invalid = |reason| StorageError::InvalidBlock { number, reason };
    let mut logs: usize = 0;
    for receipt in receipts {
        let receipt_logs = receipt.logs();
        if receipt_logs
            .iter()
            .any(|log| log.data.topics().len() > MAX_LOG_TOPICS)
        {
            return Err(invalid(InvalidBlockReason::TooManyTopics));
        }
        logs = logs.saturating_add(receipt_logs.len());
    }
    if u32::try_from(logs).is_err() {
        return Err(invalid(InvalidBlockReason::TooManyLogs));
    }
    Ok(())
}
