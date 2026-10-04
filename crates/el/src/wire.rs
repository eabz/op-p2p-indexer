//! The eth wire messages this crate sends and reads, on reth's p2p stream.
//!
//! reth's typed eth stream is built for Ethereum's types, so OP messages are encoded and
//! decoded here on the raw stream: a message is its id byte followed by RLP. Request and
//! response structures are reth's; receipts are decoded by op-alloy. Requests and their answers are eth/69's; what is served is answered in
//! the session's version (eth/69 or eth/68), see `serve`.
//!
//! Does not open connections or track requests; see `session`.

use alloy_consensus::TxReceipt;
use alloy_primitives::{B256, Bytes};
use alloy_rlp::{Decodable, Encodable};
use op_alloy_consensus::{OpReceipt, OpReceiptEnvelope};
use op_indexer_primitives::rlp_list_items;
use reth_eth_wire_types::message::RequestPair;
use reth_eth_wire_types::{
    BlockRangeUpdate, GetBlockBodies, GetBlockHeaders, GetReceipts, HeadersDirection,
};

/// `GetBlockHeaders`.
pub(crate) const GET_BLOCK_HEADERS: u8 = 0x03;
/// `BlockHeaders`.
pub(crate) const BLOCK_HEADERS: u8 = 0x04;
/// `GetBlockBodies`.
pub(crate) const GET_BLOCK_BODIES: u8 = 0x05;
/// `BlockBodies`.
pub(crate) const BLOCK_BODIES: u8 = 0x06;
/// `GetPooledTransactions`.
pub(crate) const GET_POOLED_TRANSACTIONS: u8 = 0x09;
/// `PooledTransactions`.
pub(crate) const POOLED_TRANSACTIONS: u8 = 0x0a;
/// `GetReceipts`.
pub(crate) const GET_RECEIPTS: u8 = 0x0f;
/// `Receipts`.
pub(crate) const RECEIPTS: u8 = 0x10;
/// `BlockRangeUpdate` (eth/69): the range of blocks the peer serves.
pub(crate) const BLOCK_RANGE_UPDATE: u8 = 0x11;

/// A request this node makes of a peer.
#[derive(Debug)]
pub(crate) enum Request {
    /// `GetBlockHeaders`: up to `limit` headers going down from the block with hash `start`,
    /// inclusive.
    Headers {
        /// Hash of the highest header wanted.
        start: B256,
        /// Most headers wanted.
        limit: u64,
    },
    /// `GetBlockBodies` for these block hashes.
    Bodies(Vec<B256>),
    /// `GetReceipts` for these block hashes.
    Receipts(Vec<B256>),
}

impl Request {
    /// The id of the message that answers this request.
    pub(crate) const fn response_id(&self) -> u8 {
        match self {
            Self::Headers { .. } => BLOCK_HEADERS,
            Self::Bodies(_) => BLOCK_BODIES,
            Self::Receipts(_) => RECEIPTS,
        }
    }

    /// Encodes the request as a message: its id byte, then the request id and the request.
    pub(crate) fn encode(&self, request_id: u64) -> Bytes {
        match self {
            Self::Headers { start, limit } => encode(
                GET_BLOCK_HEADERS,
                request_id,
                &GetBlockHeaders {
                    start_block: (*start).into(),
                    limit: *limit,
                    skip: 0,
                    direction: HeadersDirection::Falling,
                },
            ),
            Self::Bodies(blocks) => encode(
                GET_BLOCK_BODIES,
                request_id,
                &GetBlockBodies(blocks.clone()),
            ),
            Self::Receipts(blocks) => {
                encode(GET_RECEIPTS, request_id, &GetReceipts(blocks.clone()))
            }
        }
    }
}

/// Cuts the body of an answer (`BlockHeaders`, `BlockBodies` or `Receipts`) into its items:
/// each header, body or block's receipts as the bytes the peer sent, sharing the buffer of
/// `body`. Nothing inside an item is decoded, so a caller can hash and store exactly what was
/// received.
pub(crate) fn decode_items(body: &Bytes) -> alloy_rlp::Result<Vec<Bytes>> {
    let mut buf: &[u8] = body;
    if !alloy_rlp::Header::decode(&mut buf)?.list {
        return Err(alloy_rlp::Error::UnexpectedString);
    }
    let _request_id = u64::decode(&mut buf)?;
    // What is left is the list of items.
    let items = rlp_list_items(buf).ok_or(alloy_rlp::Error::UnexpectedLength)?;
    Ok(items.into_iter().map(|item| body.slice_ref(item)).collect())
}

/// Returns the request id of a request or response body (the message without its id byte).
pub(crate) fn request_id(mut body: &[u8]) -> Option<u64> {
    let header = alloy_rlp::Header::decode(&mut body).ok()?;
    if !header.list {
        return None;
    }
    u64::decode(&mut body).ok()
}

/// Why one block's receipts in a `Receipts` answer cannot be used.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptsError {
    /// Not one receipt per transaction: no honest peer sends that.
    #[error("{got} receipts for a block with {expected} transactions")]
    Count { expected: usize, got: usize },
    /// The receipts cannot be decoded: malformed, or of a kind this build does not know.
    #[error(transparent)]
    Rlp(#[from] alloy_rlp::Error),
}

/// Decodes one block's receipts, an item of a `Receipts` answer, for a block with `expected`
/// transactions. An empty list is returned as it is: the peer does not hold them.
///
/// The receipts are counted over their RLP headers first, and nothing is decoded unless there
/// is one per transaction: an answer of millions of tiny receipts would otherwise be turned
/// into as many blooms, hundreds of megabytes, before anything is checked.
///
/// eth/69 ([EIP-7642]) sends each receipt as `[tx-type, status, cumulative-gas, logs]` without
/// the bloom; OP deposit receipts carry the deposit nonce and the deposit receipt version after
/// the logs when the block has them. op-alloy's `OpReceipt` decodes exactly that form. The
/// bloom is rebuilt from the logs here, so callers get the consensus form.
///
/// CPU work proportional to the block's logs: call it from a blocking thread.
///
/// # Errors
///
/// Returns [`ReceiptsError::Count`] if the list is not empty and does not have `expected`
/// receipts, and [`ReceiptsError::Rlp`] if it cannot be decoded.
///
/// [EIP-7642]: https://eips.ethereum.org/EIPS/eip-7642
pub(crate) fn decode_receipts(
    mut item: &[u8],
    expected: usize,
) -> Result<Vec<OpReceiptEnvelope>, ReceiptsError> {
    let got = rlp_list_items(item)
        .ok_or(alloy_rlp::Error::UnexpectedLength)?
        .len();
    if got == 0 {
        return Ok(Vec::new());
    }
    if got != expected {
        return Err(ReceiptsError::Count { expected, got });
    }
    let receipts = Vec::<OpReceipt>::decode(&mut item)?;
    Ok(receipts
        .into_iter()
        .map(|receipt| OpReceiptEnvelope::from(receipt.into_with_bloom()))
        .collect())
}

/// Decodes a `BlockRangeUpdate` body.
pub(crate) fn decode_block_range(body: &[u8]) -> alloy_rlp::Result<BlockRangeUpdate> {
    BlockRangeUpdate::decode(&mut &*body)
}

fn encode(message_id: u8, request_id: u64, message: &impl Encodable) -> Bytes {
    let pair = RequestPair {
        request_id,
        message,
    };
    let mut out = Vec::with_capacity(pair.length().saturating_add(1));
    out.push(message_id);
    pair.encode(&mut out);
    out.into()
}
