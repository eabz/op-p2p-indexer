//! Sender recovery: turns a gossiped block, or blocks fetched from peers, into the
//! [`DecodedBlock`] storage takes.
//!
//! One secp256k1 recovery per signed transaction, so it runs on blocking threads. It does not
//! decide what to do with a block whose senders cannot be recovered; its caller does.

use alloy_consensus::crypto::RecoveryError;
use alloy_consensus::transaction::SignerRecoverable;
use std::sync::Arc;

use alloy_primitives::{Address, BlockHash, BlockNumber};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_primitives::{
    BlockSource, DecodedBlock, EncodedBlock, UnsafeBlock, decode_block, is_zero_signature,
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
    /// A fetched block's bytes do not decode.
    #[error("block {hash} does not decode: {reason}")]
    Decode {
        hash: BlockHash,
        /// The decoder's error.
        reason: String,
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

/// Blocks decoded on one blocking thread. A batch is cut into this many blocks per thread, so
/// a batch of a few hundred blocks uses the cores there are: one signature recovery per
/// transaction is what bounds how fast a range is stored.
const BLOCKS_PER_THREAD: usize = 32;

/// Decodes a batch of fetched blocks and recovers their senders, on blocking threads, and
/// returns them as storage takes them, in the same order.
///
/// A legacy transaction whose signature is all zero has no signer: before Bedrock these are
/// the messages sent from L1, and their sender is the zero address.
///
/// # Errors
///
/// Returns [`RecoverError::Decode`] for the first block that does not decode,
/// [`RecoverError::Sender`] for the first other transaction without a recoverable sender,
/// and [`RecoverError::Task`] if a blocking task did not finish.
pub(crate) async fn recover_encoded(
    batch: Vec<EncodedBlock>,
) -> Result<Vec<DecodedBlock>, RecoverError> {
    let batch: Arc<[EncodedBlock]> = batch.into();
    let threads: Vec<_> = (0..batch.len())
        .step_by(BLOCKS_PER_THREAD)
        .map(|from| {
            let batch = Arc::clone(&batch);
            tokio::task::spawn_blocking(move || {
                batch
                    .iter()
                    .skip(from)
                    .take(BLOCKS_PER_THREAD)
                    .map(decode)
                    .collect::<Result<Vec<_>, _>>()
            })
        })
        .collect();
    let mut blocks = Vec::with_capacity(batch.len());
    for thread in threads {
        blocks.extend(thread.await.map_err(RecoverError::Task)??);
    }
    Ok(blocks)
}

fn decode(encoded: &EncodedBlock) -> Result<DecodedBlock, RecoverError> {
    let (block, receipts) = decode_block(encoded).map_err(|err| RecoverError::Decode {
        hash: encoded.hash,
        reason: err.to_string(),
    })?;
    let senders = senders(block.header.number, &block.body.transactions, true)?;
    Ok(DecodedBlock {
        block,
        hash: encoded.hash,
        senders,
        receipts,
        source: BlockSource::Sync,
    })
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
