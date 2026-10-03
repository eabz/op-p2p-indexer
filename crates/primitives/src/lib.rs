//! Domain types shared by every op-p2p-indexer crate.
//!
//! Depends only on alloy and op-alloy types and `bytes`, never on networking or storage crates.
//! Blocks, transactions and receipts are the alloy / op-alloy consensus types; nothing here
//! redefines them.

use alloy_primitives::{Address, B256, BlockHash, BlockNumber, Signature};
use bytes::Bytes;
use op_alloy_consensus::{OpBlock, OpReceiptEnvelope};

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

/// A decoded L2 block, with its receipts once they are known. The input of storage.
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    /// Header and transactions.
    pub block: OpBlock,
    /// Hash of the block header.
    pub hash: BlockHash,
    /// Sender of each transaction, in block order, recovered by the caller.
    pub senders: Vec<Address>,
    /// Receipt of each transaction, in block order; `None` until they are known.
    pub receipts: Option<Vec<OpReceiptEnvelope>>,
    /// Where the block came from.
    pub source: BlockSource,
}

/// Where a block came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockSource {
    /// Received over gossip, signed by the sequencer.
    Gossip,
    /// Derived from batches committed to L1.
    L1,
}

/// A block identified by height and hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRef {
    /// Block number.
    pub number: BlockNumber,
    /// Block hash.
    pub hash: BlockHash,
}

/// Canonical entries of the unsafe store that were replaced or removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reorg {
    /// Last block shared by the old and the new canonical chain. `None` when it is not known:
    /// the replaced range ends in a gap.
    pub common_ancestor: Option<BlockRef>,
    /// Head before the reorg.
    pub old_head: BlockRef,
    /// Head after the reorg. Equal to [`Self::old_head`] when only entries below the head changed.
    pub new_head: BlockRef,
    /// Hashes of the blocks that stopped being canonical, newest first.
    pub replaced: Vec<BlockHash>,
}

/// What an unsafe-store write did. Also published to readers of the unsafe store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsafeEvent {
    /// The unsafe head moved to `head`.
    NewHead {
        /// The new head.
        head: BlockRef,
        /// Whether the heights between the previous head and this one are missing.
        gap: bool,
    },
    /// Canonical entries were replaced or removed.
    Reorg(Reorg),
    /// A block below the head became canonical: a gap was repaired.
    Filled(BlockRef),
    /// Receipts were attached to a stored block.
    Receipts(BlockRef),
    /// Every block at or below `up_to` was removed.
    Pruned {
        /// The highest removed block.
        up_to: BlockRef,
    },
}

/// The L1-derived heads of the L2 chain. `None` until an L1 source reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct L1Heads {
    /// Highest block whose batch is on L1.
    pub safe: Option<BlockRef>,
    /// Highest block whose batch is in a finalized L1 block.
    pub finalized: Option<BlockRef>,
}

/// Result of an unsafe-store insert.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InsertOutcome {
    /// Whether the block was stored. `false` for a block that was already stored or is at or
    /// below the safe head.
    pub stored: bool,
    /// What the insert changed, in order. Empty when `stored` is `false`.
    pub events: Vec<UnsafeEvent>,
}

/// A block as the archive holds it: RLP, decompressed, ready to be put on the wire by a caller
/// that knows the protocol version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedBlock {
    /// RLP of the header.
    pub header: alloy_primitives::Bytes,
    /// RLP of the body: transactions in network encoding, ommers, withdrawals.
    pub body: alloy_primitives::Bytes,
    /// RLP list of the consensus receipts; `None` until they are set.
    pub receipts: Option<alloy_primitives::Bytes>,
}
