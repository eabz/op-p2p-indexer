//! The eth wire messages this crate sends and reads, on reth's p2p stream.
//!
//! reth's typed eth stream is built for Ethereum's types, so OP messages are encoded and
//! decoded here on the raw stream: a message is its id byte followed by RLP. Request and
//! response structures are reth's; receipts are decoded by op-alloy. Only eth/69 is spoken.
//!
//! Does not open connections or track requests; see `session`.

use alloy_consensus::TxReceipt;
use alloy_primitives::{B256, Bytes};
use alloy_rlp::{Decodable, Encodable};
use op_alloy_consensus::{OpReceipt, OpReceiptEnvelope};
use reth_eth_wire_types::message::RequestPair;
use reth_eth_wire_types::{BlockRangeUpdate, GetReceipts};

/// `GetBlockHeaders`.
const GET_BLOCK_HEADERS: u8 = 0x03;
/// `BlockHeaders`.
const BLOCK_HEADERS: u8 = 0x04;
/// `GetBlockBodies`.
const GET_BLOCK_BODIES: u8 = 0x05;
/// `BlockBodies`.
const BLOCK_BODIES: u8 = 0x06;
/// `GetPooledTransactions`.
const GET_POOLED_TRANSACTIONS: u8 = 0x09;
/// `PooledTransactions`.
const POOLED_TRANSACTIONS: u8 = 0x0a;
/// `GetReceipts`.
const GET_RECEIPTS: u8 = 0x0f;
/// `Receipts`.
pub(crate) const RECEIPTS: u8 = 0x10;
/// `BlockRangeUpdate` (eth/69): the range of blocks the peer serves.
pub(crate) const BLOCK_RANGE_UPDATE: u8 = 0x11;

/// Encodes `GetReceipts` for one block.
pub(crate) fn get_receipts(request_id: u64, block: B256) -> Bytes {
    encode(GET_RECEIPTS, request_id, &GetReceipts(vec![block]))
}

/// Returns the request id of a request or response body (the message without its id byte).
pub(crate) fn request_id(mut body: &[u8]) -> Option<u64> {
    let header = alloy_rlp::Header::decode(&mut body).ok()?;
    if !header.list {
        return None;
    }
    u64::decode(&mut body).ok()
}

/// Builds the empty answer to a peer's request, or `None` if the message is not a request we
/// answer. This node holds nothing to serve yet, and an empty answer is how a node says so.
pub(crate) fn empty_response(message_id: u8, body: &[u8]) -> Option<Bytes> {
    let response_id = match message_id {
        GET_BLOCK_HEADERS => BLOCK_HEADERS,
        GET_BLOCK_BODIES => BLOCK_BODIES,
        GET_POOLED_TRANSACTIONS => POOLED_TRANSACTIONS,
        GET_RECEIPTS => RECEIPTS,
        _ => return None,
    };
    Some(encode(response_id, request_id(body)?, &Vec::<B256>::new()))
}

/// Decodes a `Receipts` body answering a request for one block: the receipts of that block,
/// empty if the peer does not hold them.
///
/// eth/69 ([EIP-7642]) sends each receipt as `[tx-type, status, cumulative-gas, logs]` without
/// the bloom; OP deposit receipts carry the deposit nonce and the deposit receipt version after
/// the logs when the block has them. op-alloy's `OpReceipt` decodes exactly that form. The
/// bloom is rebuilt from the logs here, so callers get the consensus form.
///
/// [EIP-7642]: https://eips.ethereum.org/EIPS/eip-7642
pub(crate) fn decode_receipts(body: &[u8]) -> alloy_rlp::Result<Vec<OpReceiptEnvelope>> {
    let blocks = RequestPair::<Vec<Vec<OpReceipt>>>::decode(&mut &*body)?.message;
    Ok(blocks
        .into_iter()
        .next()
        .unwrap_or_default()
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
