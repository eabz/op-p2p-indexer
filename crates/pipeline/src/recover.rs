//! Sender recovery: turns a gossiped block, or blocks fetched from peers, into the
//! [`DecodedBlock`] storage takes.
//!
//! One secp256k1 recovery per signed transaction, so it runs on a blocking thread. It does not
//! decide what to do with a block whose senders cannot be recovered; ingest does.

use alloy_consensus::crypto::RecoveryError;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{Address, BlockNumber};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_primitives::{
    BlockSource, DecodedBlock, EncodedBlock, SyncedBlock, UnsafeBlock, is_zero_signature,
};
use tokio::task::JoinError;

/// Why a block could not be turned into a [`DecodedBlock`].
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
    let senders = senders(block.number(), &block.block.body.transactions, false)?;
    Ok(DecodedBlock {
        block: block.block,
        hash: block.hash,
        senders,
        receipts: None,
        source: BlockSource::Gossip,
    })
}

/// Recovers the senders of a batch of fetched blocks on a blocking thread, and returns each
/// block as storage takes it next to its original encoding, in the same order.
///
/// A legacy transaction whose signature is all zero has no signer: before Bedrock these are
/// the messages sent from L1, and their sender is the zero address.
///
/// # Errors
///
/// Returns [`RecoverError::Sender`] for the first other transaction without a recoverable
/// sender, and [`RecoverError::Task`] if the blocking task did not finish.
pub(crate) async fn recover_synced(
    blocks: Vec<SyncedBlock>,
) -> Result<(Vec<DecodedBlock>, Vec<EncodedBlock>), RecoverError> {
    tokio::task::spawn_blocking(move || {
        blocks
            .into_iter()
            .map(|synced| {
                let number = synced.block.header.number;
                let senders = senders(number, &synced.block.body.transactions, true)?;
                let decoded = DecodedBlock {
                    block: synced.block,
                    hash: synced.encoded.hash,
                    senders,
                    receipts: Some(synced.receipts),
                    source: BlockSource::Sync,
                };
                Ok((decoded, synced.encoded))
            })
            .collect()
    })
    .await
    .map_err(RecoverError::Task)?
}

/// The sender of every transaction of block `number`, in order. With `zero_signatures`, a
/// legacy transaction signed with all zeros gets the zero address.
fn senders(
    number: BlockNumber,
    transactions: &[OpTxEnvelope],
    zero_signatures: bool,
) -> Result<Vec<Address>, RecoverError> {
    transactions
        .iter()
        .enumerate()
        .map(|(index, transaction)| {
            if zero_signatures && is_zero_signature(transaction) {
                return Ok(Address::ZERO);
            }
            transaction
                .recover_signer()
                .map_err(|source| RecoverError::Sender {
                    number,
                    index,
                    source,
                })
        })
        .collect()
}
