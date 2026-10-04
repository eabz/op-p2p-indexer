//! Domain types shared by every op-p2p-indexer crate.
//!
//! Depends only on alloy and op-alloy types, never on networking or storage crates.
//! Blocks, transactions and receipts are the alloy / op-alloy consensus types; nothing here
//! redefines them.

use std::net::SocketAddr;

use alloy_consensus::transaction::{RlpEcdsaDecodableTx, RlpEcdsaEncodableTx};
use alloy_consensus::{Signed, TxLegacy};
use alloy_eips::eip2718::{Decodable2718, Eip2718Result, Encodable2718};
use alloy_primitives::{Address, B256, B512, BlockHash, BlockNumber, Signature, U256, keccak256};
use alloy_rlp::Header;
use op_alloy_consensus::{OpBlock, OpReceiptEnvelope, OpTxEnvelope};

/// Execution payload version a block was gossiped as, which is also the fork it belongs to.
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
/// The network has checked the sequencer's signature and that the header hashes to
/// [`Self::hash`], and has decoded the transactions; their senders are not recovered yet.
#[derive(Debug, Clone)]
pub struct UnsafeBlock {
    /// Payload version the block was gossiped as.
    pub version: PayloadVersion,
    /// Hash of the block header.
    pub hash: BlockHash,
    /// Header and transactions.
    pub block: OpBlock,
}

impl UnsafeBlock {
    /// Returns the block number.
    pub const fn number(&self) -> BlockNumber {
        self.block.header.number
    }

    /// Returns the block timestamp, in seconds since the Unix epoch.
    pub const fn timestamp_secs(&self) -> u64 {
        self.block.header.timestamp
    }
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
    /// Imported from an external archive by `op-indexer-import` and verified against the
    /// header chain down from a trusted block hash.
    Import,
    /// Fetched from execution peers and verified by the header chain from a trusted block.
    Sync,
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

/// A stored block whose receipts are wanted, sent to whoever fetches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptsRequest {
    /// The block: number and hash.
    pub block: BlockRef,
    /// `receiptsRoot` of its header, which the fetched receipts must hash to.
    pub receipts_root: B256,
    /// Header timestamp, in seconds since the Unix epoch: selects the fork's receipt encoding.
    pub timestamp_secs: u64,
    /// Number of transactions in the block, so the number of receipts expected.
    pub transaction_count: usize,
}

impl From<&DecodedBlock> for ReceiptsRequest {
    fn from(block: &DecodedBlock) -> Self {
        let header = &block.block.header;
        Self {
            block: BlockRef {
                number: header.number,
                hash: block.hash,
            },
            receipts_root: header.receipts_root,
            timestamp_secs: header.timestamp,
            transaction_count: block.block.body.transactions.len(),
        }
    }
}

/// Receipts of a block, verified against its header's receipts root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReceipts {
    /// The block they belong to, as in the request.
    pub block: BlockRef,
    /// One receipt per transaction, in block order.
    pub receipts: Vec<OpReceiptEnvelope>,
}

/// An execution peer that served us, kept so a restart can dial it without waiting for
/// discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionPeer {
    /// Node id: the peer's uncompressed secp256k1 public key without its `0x04` prefix.
    pub id: B512,
    /// TCP address its sessions are dialed at.
    pub addr: SocketAddr,
    /// When it last served a request, in seconds since the Unix epoch.
    pub last_served_secs: u64,
}

/// A range of blocks to fetch from execution peers: from `from` up to the anchor, a block
/// whose hash is trusted (configured, or verified on gossip) and that every fetched header
/// must chain to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncRange {
    /// First block of the range.
    pub from: BlockNumber,
    /// Last block of the range and its trusted hash.
    pub anchor: BlockRef,
}

/// How far a range sync got, kept so a restart resumes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncState {
    /// Highest block of the range that is stored, with every block of the range below it;
    /// `None` until the first batch is stored.
    pub stored_to: Option<BlockNumber>,
    /// Blocks above [`Self::stored_to`] whose hash was verified by the header chain down from
    /// the anchor, ascending. Fetching resumes from them without walking the chain again.
    pub checkpoints: Vec<BlockRef>,
}

/// A block fetched from execution peers and verified: its header by the hash chain from a
/// trusted block, its transactions and receipts against the roots in that header.
#[derive(Debug, Clone)]
pub struct SyncedBlock {
    /// Header and transactions, decoded from the bytes in [`Self::encoded`].
    pub block: OpBlock,
    /// The header and body exactly as received, and the receipts encoded with their blooms.
    pub encoded: EncodedBlock,
    /// Receipt of each transaction, in block order.
    pub receipts: Vec<OpReceiptEnvelope>,
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

/// A block in its original consensus encoding, as it is handed to the archive by a caller that
/// has verified it. The archive stores these bytes unchanged, so they must be the bytes that
/// were verified, never bytes encoded again from a decoded value: some blocks (those with a
/// legacy transaction whose signature is all zero) do not survive that round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedBlock {
    /// The block hash: the keccak of `header`.
    pub hash: BlockHash,
    /// RLP of the header.
    pub header: alloy_primitives::Bytes,
    /// RLP of the body (transactions in network encoding, ommers, optional withdrawals): one
    /// `BlockBodies` entry of the eth protocol.
    pub body: alloy_primitives::Bytes,
    /// RLP list of the receipts, each in network encoding with its bloom; `None` if unknown.
    pub receipts: Option<alloy_primitives::Bytes>,
}

/// Encodes `receipts` as the archive holds them and [`EncodedBlock::receipts`] carries them:
/// an RLP list of the receipts, each in network encoding with its bloom (one `Receipts` entry
/// of the eth protocol up to eth/68). A deposit receipt keeps its nonce and version as given.
#[must_use]
pub fn encode_receipts(receipts: &[OpReceiptEnvelope]) -> alloy_primitives::Bytes {
    let mut out = Vec::new();
    alloy_rlp::encode_list(receipts, &mut out);
    out.into()
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

/// RLP of the integer zero, which is how each of `v`, `r` and `s` of a zero signature is encoded.
const RLP_ZERO: u8 = alloy_rlp::EMPTY_STRING_CODE;

/// Decodes one transaction from its consensus encoding, as a block body and the transactions
/// trie hold it: the RLP list of a legacy transaction, or a type byte followed by the payload.
///
/// A legacy transaction whose signature is all zero (`v = r = s = 0`) decodes too. OP Mainnet's
/// client before Bedrock (l2geth) wrote L1-to-L2 messages that way, as observed on its blocks:
/// the transaction hash is the keccak of that encoding, zeros included. alloy's decoder rejects
/// `v = 0`, and its encoder would write `v = 27`, so such a transaction is built here with the
/// hash of the given bytes, and only [`encode_transaction`] gives those bytes back. It has no
/// signer: see [`is_zero_signature`].
///
/// # Errors
///
/// Returns the decoder's error if `leaf` is not exactly one transaction of a known type.
pub fn decode_transaction(leaf: &[u8]) -> Eip2718Result<OpTxEnvelope> {
    if let Some(message) = decode_zero_signature(leaf) {
        return Ok(OpTxEnvelope::Legacy(message));
    }
    let mut buf = leaf;
    let transaction = OpTxEnvelope::decode_2718(&mut buf)?;
    if !buf.is_empty() {
        return Err(alloy_rlp::Error::UnexpectedLength.into());
    }
    Ok(transaction)
}

/// Appends the consensus encoding of `transaction` to `out`: the inverse of
/// [`decode_transaction`], zero signatures included.
pub fn encode_transaction(transaction: &OpTxEnvelope, out: &mut Vec<u8>) {
    if let OpTxEnvelope::Legacy(signed) = transaction
        && is_zero(signed.signature())
    {
        let fields = signed.tx().rlp_encoded_fields_length();
        Header {
            list: true,
            payload_length: fields.saturating_add(3),
        }
        .encode(out);
        signed.tx().rlp_encode_fields(out);
        out.extend_from_slice(&[RLP_ZERO; 3]);
    } else {
        transaction.encode_2718(out);
    }
}

/// Whether `transaction` is a legacy transaction with an all-zero signature: an L1-to-L2
/// message of the client before Bedrock. It has no signer to recover; its sender is recorded
/// as the zero address.
#[must_use]
pub fn is_zero_signature(transaction: &OpTxEnvelope) -> bool {
    matches!(transaction, OpTxEnvelope::Legacy(signed) if is_zero(signed.signature()))
}

fn is_zero(signature: &Signature) -> bool {
    signature.r().is_zero() && signature.s().is_zero()
}

/// Decodes `leaf` if it is a legacy transaction with `v = r = s = 0`; `None` for anything
/// else, malformed input included, which the regular decoder then reports.
fn decode_zero_signature(leaf: &[u8]) -> Option<Signed<TxLegacy>> {
    let mut buf = leaf;
    let header = Header::decode(&mut buf).ok()?;
    if !header.list || header.payload_length != buf.len() {
        return None;
    }
    let transaction = TxLegacy::rlp_decode_fields(&mut buf).ok()?;
    if buf != [RLP_ZERO; 3] {
        return None;
    }
    let signature = Signature::new(U256::ZERO, U256::ZERO, false);
    Some(Signed::new_unchecked(
        transaction,
        signature,
        keccak256(leaf),
    ))
}
