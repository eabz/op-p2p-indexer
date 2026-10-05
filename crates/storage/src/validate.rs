//! The checks a decoded block must pass before the unsafe store takes it: what its layout can
//! hold. Holds only what does not depend on the store's state; checks against stored data
//! (the parent's number, the stored number of a block) and the roots stay with the store. The
//! archive takes blocks in their consensus encoding and checks those itself.

use op_alloy_consensus::OpTxEnvelope;
use op_indexer_primitives::DecodedBlock;

use crate::{InvalidBlockReason, StorageError};

/// Checks that `block` fits the unsafe store.
///
/// # Errors
///
/// Returns [`StorageError::UnsupportedTransaction`] for a transaction type the unsafe store has
/// no place for, and [`StorageError::InvalidBlock`] with the first [`InvalidBlockReason`] that
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

    if block
        .receipts
        .as_ref()
        .is_some_and(|receipts| receipts.len() != transactions.len())
    {
        return Err(invalid(InvalidBlockReason::ReceiptCount));
    }
    Ok(())
}
