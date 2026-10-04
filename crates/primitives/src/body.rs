//! Block bodies and roots as bytes: cutting a body into the parts its header commits to,
//! writing one from verified transactions, and the two roots a header carries for it.
//!
//! Everything here works on the encodings themselves, so what is hashed, stored and served is
//! what was received or built once; nothing is decoded into a typed value and encoded again.
//! Does not decode transactions (see [`decode_transaction`](crate::decode_transaction)) and
//! does not compare anything with a header: callers do.

use std::borrow::Cow;

use alloy_consensus::BlockBody;
use alloy_consensus::proofs::{calculate_receipt_root, ordered_trie_root_with_encoder};
use alloy_eips::eip2718::Eip2718Result;
use alloy_primitives::{B256, Bytes};
use alloy_rlp::{EMPTY_LIST_CODE, Encodable, Header};
use op_alloy_consensus::{OpBlock, OpReceiptEnvelope};

use crate::{EncodedBlock, decode_transaction};

/// A block body cut into the byte ranges its header commits to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyParts<'a> {
    /// Each transaction in its consensus encoding ([EIP-2718]), as the transactions trie holds
    /// it: the RLP list of a legacy transaction, or the type byte and payload of a typed one.
    ///
    /// [EIP-2718]: https://eips.ethereum.org/EIPS/eip-2718
    pub transactions: Vec<&'a [u8]>,
    /// The RLP list of ommers, whose keccak is the header's ommers hash.
    pub ommers: &'a [u8],
    /// Whether the body ends with a withdrawals list. The list is empty: an OP Stack block has
    /// no withdrawals.
    pub withdrawals: bool,
}

/// Cuts a body, `[transactions, ommers]` or `[transactions, ommers, withdrawals]` (one
/// `BlockBodies` entry of the eth protocol), without decoding what is inside.
///
/// Returns `None` if the bytes are not exactly one such body, or its withdrawals list is not
/// empty.
#[must_use]
pub fn split_body(body: &[u8]) -> Option<BodyParts<'_>> {
    let mut rest = body;
    let outer = item(&mut rest).filter(|outer| outer.list)?;
    if !rest.is_empty() {
        return None;
    }
    let mut fields = outer.payload;
    let list = item(&mut fields).filter(|list| list.list)?;
    let ommers = item(&mut fields).filter(|ommers| ommers.list)?;
    let withdrawals = if fields.is_empty() {
        false
    } else {
        let withdrawals = item(&mut fields)?;
        if !withdrawals.list || !withdrawals.payload.is_empty() || !fields.is_empty() {
            return None;
        }
        true
    };

    let mut transactions = Vec::new();
    let mut entries = list.payload;
    while !entries.is_empty() {
        let entry = item(&mut entries)?;
        // In a body a typed transaction is wrapped in an RLP string; the trie holds what is
        // inside. A legacy transaction is a list, held as it is.
        transactions.push(if entry.list {
            entry.whole
        } else {
            entry.payload
        });
    }
    Some(BodyParts {
        transactions,
        ommers: ommers.whole,
        withdrawals,
    })
}

/// Decodes a block from its consensus encoding into the typed block and its receipts, for
/// callers that need the fields. Transactions go through [`decode_transaction`], so a block
/// with a legacy transaction signed with all zeros decodes; its ommers are not decoded and
/// the typed block has none.
///
/// # Errors
///
/// Returns the decoder's error if the header, a transaction or the receipts do not decode,
/// or the body is not a block body.
pub fn decode_block(
    encoded: &EncodedBlock,
) -> Eip2718Result<(OpBlock, Option<Vec<OpReceiptEnvelope>>)> {
    let header = alloy_rlp::decode_exact(&encoded.header)?;
    let parts = split_body(&encoded.body).ok_or(alloy_rlp::Error::Custom("not a block body"))?;
    let transactions = parts
        .transactions
        .iter()
        .map(|transaction| decode_transaction(transaction))
        .collect::<Eip2718Result<_>>()?;
    let receipts = encoded
        .receipts
        .as_ref()
        .map(alloy_rlp::decode_exact)
        .transpose()?;
    let block = OpBlock {
        header,
        body: BlockBody {
            transactions,
            ommers: Vec::new(),
            withdrawals: parts.withdrawals.then(Default::default),
        },
    };
    Ok((block, receipts))
}

/// Encodes a body from its transactions, each in its consensus encoding (what
/// [`BodyParts::transactions`] holds): no ommers, and an empty withdrawals list when
/// `with_withdrawals`. The inverse of [`split_body`] for a body without ommers.
#[must_use]
pub fn encode_body(transactions: &[impl AsRef<[u8]>], with_withdrawals: bool) -> Bytes {
    let mut list = Vec::new();
    for transaction in transactions {
        let transaction = transaction.as_ref();
        // A legacy transaction is already an RLP list; a typed one is wrapped in a string.
        if transaction
            .first()
            .is_some_and(|byte| *byte >= EMPTY_LIST_CODE)
        {
            list.extend_from_slice(transaction);
        } else {
            transaction.encode(&mut list);
        }
    }
    let empty_lists = if with_withdrawals { 2 } else { 1 };
    let transactions = Header {
        list: true,
        payload_length: list.len(),
    };
    let mut out = Vec::new();
    Header {
        list: true,
        payload_length: transactions
            .length()
            .saturating_add(list.len())
            .saturating_add(empty_lists),
    }
    .encode(&mut out);
    transactions.encode(&mut out);
    out.extend_from_slice(&list);
    out.extend(std::iter::repeat_n(EMPTY_LIST_CODE, empty_lists));
    out.into()
}

/// The transactions root of a block whose transactions have these consensus encodings, in
/// block order.
#[must_use]
pub fn transactions_root(transactions: &[impl AsRef<[u8]>]) -> B256 {
    ordered_trie_root_with_encoder(transactions, |transaction, out| {
        out.extend_from_slice(transaction.as_ref());
    })
}

/// The receipts root of a block with these receipts, at `timestamp_secs`.
///
/// A deposit receipt carries a deposit nonce (and, from Canyon, a version). They are part of
/// the hashed receipt only from `canyon_time` on; before it they are stored and sent but not
/// hashed. See the [deposit receipt] section of the OP Stack specification.
///
/// [deposit receipt]: https://specs.optimism.io/protocol/deposits.html#deposit-receipt
#[must_use]
pub fn receipts_root(
    receipts: &[OpReceiptEnvelope],
    timestamp_secs: u64,
    canyon_time: u64,
) -> B256 {
    let hashed = if timestamp_secs >= canyon_time {
        Cow::Borrowed(receipts)
    } else {
        Cow::Owned(receipts.iter().map(without_deposit_nonce).collect())
    };
    calculate_receipt_root(&hashed)
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

/// One RLP item at the start of a buffer.
struct Item<'a> {
    list: bool,
    /// The item with its RLP header.
    whole: &'a [u8],
    payload: &'a [u8],
}

/// Takes the next RLP item off `buf`. `None` if `buf` does not start with a complete item.
fn item<'a>(buf: &mut &'a [u8]) -> Option<Item<'a>> {
    let start = *buf;
    let mut after_header = start;
    let header = Header::decode(&mut after_header).ok()?;
    let (payload, rest) = after_header.split_at_checked(header.payload_length)?;
    let (whole, _) = start.split_at_checked(start.len().checked_sub(rest.len())?)?;
    *buf = rest;
    Some(Item {
        list: header.list,
        whole,
        payload,
    })
}
