//! Verification of a block's receipts against the receipts root of its header.
//!
//! The header is one we already trust (sequencer-signed on gossip); the receipts come from an
//! execution peer and are trusted only if they hash to that header's `receiptsRoot`. The root
//! commits to each receipt's status, cumulative gas, logs and bloom, in transaction order, so
//! a list that passes is exactly the block's receipts.
//!
//! The bloom is not sent on eth/69: our own decoding rebuilds it from the logs. That is safe
//! only because of this check: a bloom rebuilt from wrong logs gives a wrong root. Nothing the
//! root does not cover is taken from a peer.
//!
//! Does not fetch, decode, or decide what to do with a peer whose answer fails: it only says
//! whether a list is the block's receipts.
//!
//! # Rules
//!
//! - **One receipt per transaction.** An empty answer is not handled here: the caller treats
//!   it as "the peer does not hold them".
//! - **The root** is the root of the ordered trie of the receipts, each in its [EIP-2718]
//!   encoding with the bloom: the type byte, then the RLP list (none for legacy receipts).
//! - **Deposit receipts before Canyon.** A deposit receipt carries a deposit nonce (and, from
//!   Canyon, a version). They are part of the hashed receipt only from Canyon on; before it
//!   they are on the wire but not in the hash, so they are left out when the root is computed.
//!   See the [deposit receipt] section of the OP Stack specification.
//!
//! [EIP-2718]: https://eips.ethereum.org/EIPS/eip-2718
//! [deposit receipt]: https://specs.optimism.io/protocol/deposits.html#deposit-receipt

use std::borrow::Cow;

use alloy_consensus::proofs::calculate_receipt_root;
use alloy_primitives::B256;
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::ReceiptsRequest;

use crate::metrics::VerificationFailure;

/// Why a list of receipts is not the block's.
#[derive(Debug, thiserror::Error)]
pub(crate) enum VerifyError {
    /// Not one receipt per transaction.
    #[error("{got} receipts for a block with {expected} transactions")]
    Count {
        /// Transactions in the block.
        expected: usize,
        /// Receipts in the answer.
        got: usize,
    },
    /// The receipts do not hash to the header's receipts root.
    #[error("receipts root {computed} does not match the header's {expected}")]
    Root {
        /// The header's receipts root.
        expected: B256,
        /// The root of the receipts received.
        computed: B256,
    },
}

impl VerifyError {
    /// The failure as a metric label.
    pub(crate) const fn kind(&self) -> VerificationFailure {
        match self {
            Self::Count { .. } => VerificationFailure::Count,
            Self::Root { .. } => VerificationFailure::Root,
        }
    }
}

/// Checks that `receipts` are the receipts of the block in `request`: one per transaction,
/// hashing to its receipts root. `canyon_time` is the chain's Canyon activation, which selects
/// how deposit receipts are hashed.
///
/// CPU work proportional to the block's logs: call it from a blocking thread.
///
/// # Errors
///
/// Returns [`VerifyError::Count`] if the list does not have one receipt per transaction, and
/// [`VerifyError::Root`] if it does not hash to the header's receipts root.
pub(crate) fn verify_receipts(
    request: &ReceiptsRequest,
    receipts: &[OpReceiptEnvelope],
    canyon_time: u64,
) -> Result<(), VerifyError> {
    if receipts.len() != request.transaction_count {
        return Err(VerifyError::Count {
            expected: request.transaction_count,
            got: receipts.len(),
        });
    }
    let hashed = if request.timestamp_secs >= canyon_time {
        Cow::Borrowed(receipts)
    } else {
        Cow::Owned(receipts.iter().map(without_deposit_nonce).collect())
    };
    let computed = calculate_receipt_root(&hashed);
    if computed != request.receipts_root {
        return Err(VerifyError::Root {
            expected: request.receipts_root,
            computed,
        });
    }
    Ok(())
}

/// The receipt as it is hashed before Canyon: a deposit receipt without its nonce and version.
fn without_deposit_nonce(receipt: &OpReceiptEnvelope) -> OpReceiptEnvelope {
    let mut hashed = receipt.clone();
    if let OpReceiptEnvelope::Deposit(deposit) = &mut hashed {
        deposit.receipt.deposit_nonce = None;
        deposit.receipt.deposit_receipt_version = None;
    }
    hashed
}
