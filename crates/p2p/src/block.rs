//! OP Stack block gossip: topic names and unsafe-block validation.
//!
//! Implements the envelope layout and validation rules from
//! <https://specs.optimism.io/protocol/rollup-node-p2p.html#block-validation>, using op-alloy's
//! SSZ payload types and signing message. Receives messages already snappy-decompressed by
//! [`crate::gossip`]; op-alloy's own `OpNetworkPayloadEnvelope::decode_*` is not used because it
//! decompresses again and does not bound the decompressed size.

use std::collections::BTreeMap;

use alloy_eips::eip2718::{Decodable2718, Eip2718Error};
use alloy_primitives::{Address, B256, BlockHash, BlockNumber, ChainId, Signature, SignatureError};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3,
    PraguePayloadFields,
};
use bytes::Bytes;
use libp2p::gossipsub::MessageAcceptance;
use op_alloy_consensus::OpTxEnvelope;
use op_alloy_rpc_types_engine::{
    OpExecutionPayload, OpExecutionPayloadSidecar, OpExecutionPayloadV4, OpPayloadError,
    PayloadHash,
};
use op_indexer_primitives::{PayloadVersion, UnsafeBlock};
use ssz::Decode;

/// Every payload version, oldest first; we subscribe to all of their topics.
pub(crate) const VERSIONS: [PayloadVersion; 4] = [
    PayloadVersion::V1,
    PayloadVersion::V2,
    PayloadVersion::V3,
    PayloadVersion::V4,
];

/// Blocks older than this are rejected.
const MAX_AGE_SECS: u64 = 60;
/// Blocks further in the future than this are rejected.
const MAX_FUTURE_SECS: u64 = 5;

/// A block is rejected once more than this many distinct blocks were seen at its height (the
/// spec's wording), so a height holds at most one more than this.
const MAX_BLOCKS_PER_HEIGHT: usize = 5;
/// Heights tracked by [`SeenBlocks`] (op-node keeps 1000). Honest traffic spans about a minute
/// of heights, because older and newer blocks fail the timestamp window.
const MAX_SEEN_HEIGHTS: usize = 1000;

const SIGNATURE_LEN: usize = 65;
const PARENT_BEACON_BLOCK_ROOT_LEN: usize = 32;
/// Size of the `ExecutionPayloadV1` SSZ fixed part; anything shorter cannot be a payload.
const MIN_PAYLOAD_LEN: usize = 508;

/// Returns the gossip topic for blocks of `version` on `chain_id`.
pub(crate) fn topic(chain_id: ChainId, version: PayloadVersion) -> String {
    let index = match version {
        PayloadVersion::V1 => 0,
        PayloadVersion::V2 => 1,
        PayloadVersion::V3 => 2,
        PayloadVersion::V4 => 3,
    };
    format!("/optimism/{chain_id}/{index}/blocks")
}

/// Why a block message failed validation.
///
/// [`Self::acceptance`] says how gossipsub should treat the message. The spec makes every
/// validation failure a `REJECT`, which penalizes the peer that forwarded it; the one case
/// where the peer is probably not at fault is reported by [`Self::local_clock_lag_secs`].
/// [`Self::UndecodableTransaction`] is not a spec rule: the message is ignored, not rejected.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockError {
    #[error("message is {len} bytes, shorter than the minimum {min}")]
    TooShort { len: usize, min: usize },
    #[error("payload is not valid SSZ: {0:?}")]
    InvalidPayload(ssz::DecodeError),
    #[error("block {number} is {age_secs}s old")]
    Stale { number: BlockNumber, age_secs: u64 },
    #[error("block {number} is {ahead_secs}s in the future")]
    TooFarInFuture {
        number: BlockNumber,
        ahead_secs: u64,
        /// Whether the sequencer signed it. If so the block is genuine and the likelier cause is
        /// a local clock running behind, not a faulty peer.
        signed_by_sequencer: bool,
    },
    #[error("block {number} has signature recovery id {recovery_id}, expected 0 or 1")]
    InvalidRecoveryId {
        number: BlockNumber,
        recovery_id: u8,
    },
    #[error("block {number} has a malformed signature")]
    MalformedSignature {
        number: BlockNumber,
        #[source]
        source: SignatureError,
    },
    #[error("block {number} was signed by {signer}, not the sequencer")]
    WrongSigner {
        number: BlockNumber,
        signer: Address,
    },
    #[error("block {number} cannot be rebuilt from its payload")]
    InvalidBlock {
        number: BlockNumber,
        #[source]
        source: OpPayloadError,
    },
    #[error("block {number} claims hash {claimed} but hashes to {computed}")]
    HashMismatch {
        number: BlockNumber,
        claimed: BlockHash,
        computed: BlockHash,
    },
    #[error("block {number} is at a height with more than {MAX_BLOCKS_PER_HEIGHT} distinct blocks")]
    TooManyAtHeight { number: BlockNumber },
    /// The block is valid by every rule above, but holds a transaction this build cannot
    /// decode (a transaction type newer than it). Our limitation, not a peer fault.
    #[error("block {number} has a transaction this build cannot decode")]
    UndecodableTransaction {
        number: BlockNumber,
        #[source]
        source: Eip2718Error,
    },
}

impl BlockError {
    /// How gossipsub should treat the message that failed with this error. Every variant is
    /// listed so that a new one has to be classified.
    pub(crate) const fn acceptance(&self) -> MessageAcceptance {
        match self {
            Self::TooShort { .. }
            | Self::InvalidPayload(_)
            | Self::Stale { .. }
            | Self::TooFarInFuture { .. }
            | Self::InvalidRecoveryId { .. }
            | Self::MalformedSignature { .. }
            | Self::WrongSigner { .. }
            | Self::InvalidBlock { .. }
            | Self::HashMismatch { .. }
            | Self::TooManyAtHeight { .. } => MessageAcceptance::Reject,
            Self::UndecodableTransaction { .. } => MessageAcceptance::Ignore,
        }
    }

    /// Seconds the local clock appears to lag: set when the sequencer signed a block dated too
    /// far in our future, which points at our clock rather than at the peer.
    pub(crate) const fn local_clock_lag_secs(&self) -> Option<u64> {
        if let Self::TooFarInFuture {
            ahead_secs,
            signed_by_sequencer: true,
            ..
        } = *self
        {
            Some(ahead_secs)
        } else {
            None
        }
    }
}

/// Validates decompressed block messages against the chain's sequencer key.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockValidator {
    chain_id: ChainId,
    signer: Address,
}

impl BlockValidator {
    pub(crate) const fn new(chain_id: ChainId, signer: Address) -> Self {
        Self { chain_id, signer }
    }

    /// Cheap length check, run inline before a message is queued for full validation.
    pub(crate) fn precheck(version: PayloadVersion, message: &[u8]) -> Result<(), BlockError> {
        let min = header_len(version) + MIN_PAYLOAD_LEN;
        if message.len() < min {
            return Err(BlockError::TooShort {
                len: message.len(),
                min,
            });
        }
        Ok(())
    }

    /// Decodes and validates a decompressed block message received on a `version` topic.
    ///
    /// Checks run cheapest first: length, SSZ decoding, timestamp window, the sequencer signature,
    /// then the block hash. The hash check rebuilds the header (including the transactions root),
    /// so it only runs for messages the sequencer signed. A block dated in the future also has
    /// its signature checked, only to report whether the sequencer signed it. `now_secs` is
    /// the current Unix time. CPU-bound: run off the async runtime.
    ///
    /// Last, the transactions are decoded, so the block is emitted ready to use.
    ///
    /// The per-height limit needs state across messages; see [`SeenBlocks`].
    pub(crate) fn validate(
        &self,
        version: PayloadVersion,
        message: Vec<u8>,
        now_secs: u64,
    ) -> Result<UnsafeBlock, BlockError> {
        Self::precheck(version, &message)?;
        let mut message = Bytes::from(message);
        let signature_bytes = message.split_to(SIGNATURE_LEN);
        // The sequencer signs the parent beacon block root (from V3) followed by the payload.
        let signed = message.clone();
        let parent_beacon_block_root = has_parent_beacon_block_root(version)
            .then(|| B256::from_slice(&message.split_to(PARENT_BEACON_BLOCK_ROOT_LEN)));
        let payload = message;

        let decoded = decode_payload(version, &payload).map_err(BlockError::InvalidPayload)?;
        let number = decoded.block_number();
        let timestamp = decoded.timestamp();
        let hash = decoded.block_hash();

        let age_secs = now_secs.saturating_sub(timestamp);
        if age_secs > MAX_AGE_SECS {
            return Err(BlockError::Stale { number, age_secs });
        }
        let ahead_secs = timestamp.saturating_sub(now_secs);
        if ahead_secs > MAX_FUTURE_SECS {
            let signed_by_sequencer = self
                .verify_signature(number, &signature_bytes, &signed)
                .is_ok();
            return Err(BlockError::TooFarInFuture {
                number,
                ahead_secs,
                signed_by_sequencer,
            });
        }
        self.verify_signature(number, &signature_bytes, &signed)?;

        let block = decoded
            .into_block_with_sidecar_raw(&sidecar(version, parent_beacon_block_root))
            .map_err(|source| BlockError::InvalidBlock { number, source })?;
        let computed = block.header.hash_slow();
        if computed != hash {
            return Err(BlockError::HashMismatch {
                number,
                claimed: hash,
                computed,
            });
        }

        // Decoded last, once the block is known to be the sequencer's: the consumer gets the
        // block it would otherwise have to decode again.
        let block = block
            .try_map_transactions(|transaction| OpTxEnvelope::decode_2718_exact(&transaction))
            .map_err(|source| BlockError::UndecodableTransaction { number, source })?;
        Ok(UnsafeBlock {
            version,
            hash,
            block,
        })
    }

    /// Checks that the sequencer made `signature` over `signed`.
    ///
    /// The recovery id must be 0 or 1, as the sequencer writes it. alloy would also take 27, 28
    /// and EIP-155 values, giving one signature hundreds of encodings, each a distinct gossip
    /// message that passes validation. op-node (go-ethereum's `SigToPub`) rejects those; it reads
    /// 2 and 3 as a different public key, which the sequencer's signatures never recover to. Like
    /// op-node, a high `s` is accepted: rejecting it would penalize peers for relaying a message
    /// op-node forwards.
    fn verify_signature(
        &self,
        number: BlockNumber,
        signature: &[u8],
        signed: &[u8],
    ) -> Result<(), BlockError> {
        if let Some(&recovery_id) = signature.last()
            && recovery_id > 1
        {
            return Err(BlockError::InvalidRecoveryId {
                number,
                recovery_id,
            });
        }
        let signature = Signature::from_raw(signature)
            .map_err(|source| BlockError::MalformedSignature { number, source })?;
        let message_hash = PayloadHash::from(signed).signature_message(self.chain_id);
        let recovered = signature
            .recover_address_from_prehash(&message_hash)
            .map_err(|source| BlockError::MalformedSignature { number, source })?;
        if recovered != self.signer {
            return Err(BlockError::WrongSigner {
                number,
                signer: recovered,
            });
        }
        Ok(())
    }
}

/// Hashes of the validated blocks seen at each recent height.
#[derive(Debug, Default)]
pub(crate) struct SeenBlocks {
    heights: BTreeMap<BlockNumber, Vec<BlockHash>>,
}

impl SeenBlocks {
    /// Records a validated block and returns whether it is new (`false` for one already seen,
    /// which the spec ignores), or an error once its height has more than
    /// [`MAX_BLOCKS_PER_HEIGHT`] blocks.
    pub(crate) fn observe(
        &mut self,
        number: BlockNumber,
        hash: BlockHash,
    ) -> Result<bool, BlockError> {
        let hashes = self.heights.entry(number).or_default();
        if hashes.len() > MAX_BLOCKS_PER_HEIGHT {
            return Err(BlockError::TooManyAtHeight { number });
        }
        if hashes.contains(&hash) {
            return Ok(false);
        }
        hashes.push(hash);
        if self.heights.len() > MAX_SEEN_HEIGHTS {
            self.heights.pop_first();
        }
        Ok(true)
    }
}

/// Bytes before the payload: the signature, plus the parent beacon block root from V3.
const fn header_len(version: PayloadVersion) -> usize {
    if has_parent_beacon_block_root(version) {
        SIGNATURE_LEN + PARENT_BEACON_BLOCK_ROOT_LEN
    } else {
        SIGNATURE_LEN
    }
}

/// Whether messages of `version` carry a parent beacon block root before the payload.
const fn has_parent_beacon_block_root(version: PayloadVersion) -> bool {
    match version {
        PayloadVersion::V1 | PayloadVersion::V2 => false,
        PayloadVersion::V3 | PayloadVersion::V4 => true,
    }
}

/// The header fields that travel outside the payload: the parent beacon block root from V3, and
/// from V4 the requests hash, which is always the empty-requests hash on OP Stack.
fn sidecar(
    version: PayloadVersion,
    parent_beacon_block_root: Option<B256>,
) -> OpExecutionPayloadSidecar {
    let Some(root) = parent_beacon_block_root else {
        return OpExecutionPayloadSidecar::default();
    };
    let cancun = CancunPayloadFields::new(root, Vec::new());
    match version {
        PayloadVersion::V1 | PayloadVersion::V2 | PayloadVersion::V3 => {
            OpExecutionPayloadSidecar::v3(cancun)
        }
        PayloadVersion::V4 => OpExecutionPayloadSidecar::v4(cancun, PraguePayloadFields::default()),
    }
}

fn decode_payload(
    version: PayloadVersion,
    payload: &[u8],
) -> Result<OpExecutionPayload, ssz::DecodeError> {
    Ok(match version {
        PayloadVersion::V1 => OpExecutionPayload::V1(ExecutionPayloadV1::from_ssz_bytes(payload)?),
        PayloadVersion::V2 => OpExecutionPayload::V2(ExecutionPayloadV2::from_ssz_bytes(payload)?),
        PayloadVersion::V3 => OpExecutionPayload::V3(ExecutionPayloadV3::from_ssz_bytes(payload)?),
        PayloadVersion::V4 => {
            OpExecutionPayload::V4(OpExecutionPayloadV4::from_ssz_bytes(payload)?)
        }
    })
}
