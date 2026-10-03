//! Domain types shared by every op-p2p-indexer crate.
//!
//! Depends only on alloy types and `bytes`, never on networking or storage crates.

use alloy_primitives::{B256, BlockHash, BlockNumber, Signature};
use bytes::Bytes;

/// Execution payload version of a gossiped block, which determines how [`UnsafeBlock::payload`]
/// is SSZ-decoded.
///
/// See <https://specs.optimism.io/protocol/rollup-node-p2p.html#topic-validation>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PayloadVersion {
    /// Pre-Canyon: `ExecutionPayloadV1`.
    V1,
    /// Canyon/Delta: `ExecutionPayloadV2`.
    V2,
    /// Ecotone: `ExecutionPayloadV3`.
    V3,
    /// Isthmus: `OpExecutionPayloadV4` (`ExecutionPayloadV3` plus a withdrawals root).
    V4,
}

/// An L2 block received over gossip and signed by the sequencer, not yet derived from L1.
///
/// The fields needed for indexing are extracted; the full payload is kept as raw SSZ in
/// [`Self::payload`] so nothing is lost.
#[derive(Debug, Clone)]
pub struct UnsafeBlock {
    /// Payload version, which determines the SSZ layout of [`Self::payload`].
    pub version: PayloadVersion,
    /// L2 block number.
    pub number: BlockNumber,
    /// L2 block hash, verified against the header rebuilt from the payload.
    pub hash: BlockHash,
    /// Hash of the parent L2 block.
    pub parent_hash: BlockHash,
    /// Block timestamp, in seconds since the Unix epoch.
    pub timestamp: u64,
    /// Parent beacon block root (L1 origin), present from [`PayloadVersion::V3`].
    pub parent_beacon_block_root: Option<B256>,
    /// Sequencer signature over the payload.
    pub signature: Signature,
    /// SSZ-encoded execution payload.
    pub payload: Bytes,
}
