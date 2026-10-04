//! Sender recovery: turns a gossiped block into the [`DecodedBlock`] storage takes.
//!
//! One secp256k1 recovery per signed transaction, so it runs on a blocking thread. It does not
//! decide what to do with a block whose senders cannot be recovered; ingest does.

use alloy_consensus::crypto::RecoveryError;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, BlockNumber};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_primitives::{BlockSource, DecodedBlock, UnsafeBlock};
use tokio::task::JoinError;

/// Why a gossiped block could not be turned into a [`DecodedBlock`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecoverError {
    /// A transaction's signature does not recover to a sender.
    #[error("transaction {index} of block {number} has no recoverable sender")]
    Sender {
        number: BlockNumber,
        /// Position of the transaction in the block.
        index: usize,
        #[source]
        source: RecoveryError,
    },
    /// The blocking task panicked or was aborted.
    #[error("sender recovery task failed")]
    Task(#[source] JoinError),
}

/// Recovers the sender of every transaction of `block` on a blocking thread.
///
/// Deposits carry their sender; every other transaction costs one signature recovery.
///
/// # Errors
///
/// Returns [`RecoverError::Sender`] for the first transaction without a recoverable sender,
/// and [`RecoverError::Task`] if the blocking task did not finish.
pub(crate) async fn recover(block: UnsafeBlock) -> Result<DecodedBlock, RecoverError> {
    tokio::task::spawn_blocking(move || recover_blocking(block))
        .await
        .map_err(RecoverError::Task)?
}

fn recover_blocking(block: UnsafeBlock) -> Result<DecodedBlock, RecoverError> {
    let number = block.number();
    let transactions: &[OpTxEnvelope] = &block.block.body.transactions;
    let senders = transactions
        .iter()
        .enumerate()
        .map(|(index, transaction)| {
            transaction
                .recover_signer()
                .map_err(|source| RecoverError::Sender {
                    number,
                    index,
                    source,
                })
        })
        .collect::<Result<Vec<Address>, _>>()?;
    Ok(DecodedBlock {
        block: block.block,
        hash: block.hash,
        senders,
        receipts: None,
        source: BlockSource::Gossip,
    })
}
