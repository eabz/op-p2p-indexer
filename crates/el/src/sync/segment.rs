//! One segment of the range sync: its headers, then its bodies, then its receipts, on one
//! session.
//!
//! Headers are verified by the hash chain down from the segment's checkpoint. Each item of a
//! bodies or receipts answer is accepted only if it belongs to the block it is for: a body by
//! the transactions root over its transaction bytes and the ommers hash over its ommers
//! bytes, a receipt list by the receipts root under the rule of the block's era. Receipts
//! come after the bodies because decoding them safely needs the block's transaction count.
//!
//! A peer may answer for fewer blocks than asked. What verifies is kept and the rest is asked
//! for again; an item that belongs to a later block means the peer left one out, which is
//! "not held", not bad data. Only data that is present and belongs to no block asked for
//! counts against the peer.
//!
//! Does not decide which segment a session fetches (the syncer does) or decode what it
//! verified beyond the receipts.

use std::sync::Arc;

use alloy_consensus::EMPTY_ROOT_HASH;
use alloy_primitives::{B256, Bytes, keccak256};
use op_indexer_primitives::{
    BlockRef, EncodedBlock, encode_receipts, receipts_root, split_body, transactions_root,
};
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use super::headers::{VerifiedHeader, verify_chain};
use super::{Failure, Segment};
use crate::pacing::REQUEST_SPACING;
use crate::session::SessionHandle;
use crate::wire::{ReceiptsError, decode_receipts};

/// A block whose body or receipts are asked for.
#[derive(Debug)]
struct Wanted {
    header: VerifiedHeader,
    /// Transactions in the block; known once its body is verified.
    transactions: usize,
}

/// Whether an item of an answer is the one asked for a block.
enum Check<T> {
    /// It is; here in the form it is kept in.
    Belongs(T),
    /// It is another block's, or nothing valid: what does not match, for the log.
    Other(String),
    /// It says the peer does not hold what was asked: an empty receipts list for a block with
    /// transactions (a node that pruned them answers so).
    NotHeld,
    /// It cannot be read at all.
    Undecodable(String),
}

/// Fetches and verifies the blocks of `segment`, ascending, as the bytes received (receipts
/// encoded once, with their blooms).
pub(super) async fn fetch(
    session: &SessionHandle,
    segment: Segment,
    canyon_time: u64,
) -> Result<Vec<EncodedBlock>, Failure> {
    let wanted: Arc<[Wanted]> = headers(session, segment)
        .await?
        .into_iter()
        .map(|header| Wanted {
            header,
            transactions: 0,
        })
        .collect();
    let hashes = wanted.iter().map(|block| block.header.hash).collect();
    let bodies = collect(&wanted, hashes, |hashes| session.bodies(hashes), body_of).await?;

    // A block without transactions has no receipts to ask for, and its entry in an answer
    // could not be told from "not held".
    let with_receipts: Arc<[Wanted]> = wanted
        .iter()
        .zip(&bodies)
        .filter(|(block, _)| block.header.header.receipts_root != EMPTY_ROOT_HASH)
        .map(|(block, (_, transactions))| Wanted {
            header: VerifiedHeader {
                hash: block.header.hash,
                raw: Bytes::new(),
                header: block.header.header.clone(),
            },
            transactions: *transactions,
        })
        .collect();
    let hashes = with_receipts
        .iter()
        .map(|block| block.header.hash)
        .collect();
    let receipts = collect(
        &with_receipts,
        hashes,
        |hashes| session.receipts(hashes),
        move |block, item| receipts_of(block, item, canyon_time),
    )
    .await?;

    let no_receipts = encode_receipts(&[]);
    let mut receipts = receipts.into_iter();
    let blocks = wanted
        .iter()
        .zip(bodies)
        .map(|(block, (body, _))| EncodedBlock {
            hash: block.header.hash,
            header: block.header.raw.clone(),
            body,
            receipts: if block.header.header.receipts_root == EMPTY_ROOT_HASH {
                Some(no_receipts.clone())
            } else {
                receipts.next()
            },
        })
        .collect();
    Ok(blocks)
}

/// The segment's headers, verified, ascending.
async fn headers(
    session: &SessionHandle,
    segment: Segment,
) -> Result<Vec<VerifiedHeader>, Failure> {
    let wanted = segment
        .top
        .number
        .saturating_sub(segment.first)
        .saturating_add(1);
    let mut headers: Vec<VerifiedHeader> = Vec::new();
    let mut next = segment.top;
    loop {
        let have = u64::try_from(headers.len()).unwrap_or(u64::MAX);
        if have >= wanted {
            break;
        }
        if have > 0 {
            sleep(REQUEST_SPACING).await;
        }
        let raw = session.headers(next.hash, wanted - have).await?;
        let page = verify_chain(raw, next)?;
        if let Some(last) = page.last() {
            next = BlockRef {
                number: last.header.number.saturating_sub(1),
                hash: last.header.parent_hash,
            };
        }
        headers.extend(page);
    }
    headers.reverse();
    Ok(headers)
}

/// Asks for one item per block of `wanted` (by `hashes`, in the same order) until every block
/// has its own, verifying each answer on a blocking thread, and returns the items in the form
/// `belongs` keeps them, in block order.
async fn collect<T, F, Fut, B>(
    wanted: &Arc<[Wanted]>,
    hashes: Vec<B256>,
    ask: F,
    belongs: B,
) -> Result<Vec<T>, Failure>
where
    T: Send + 'static,
    F: Fn(Vec<B256>) -> Fut,
    Fut: Future<Output = Result<Vec<Bytes>, crate::session::RequestError>>,
    B: Fn(&Wanted, &Bytes) -> Check<T> + Clone + Send + 'static,
{
    let mut items = Vec::with_capacity(wanted.len());
    while let Some(missing) = hashes.get(items.len()..).filter(|rest| !rest.is_empty()) {
        if !items.is_empty() {
            sleep(REQUEST_SPACING).await;
        }
        let answer = ask(missing.to_vec()).await?;
        let (wanted, from, belongs) = (Arc::clone(wanted), items.len(), belongs.clone());
        // One trie per item: off the runtime.
        let accepted = spawn_blocking(move || {
            accept(wanted.get(from..).unwrap_or_default(), &answer, belongs)
        });
        items.extend(accepted.await.map_err(Failure::Panicked)??);
    }
    Ok(items)
}

/// Takes from `answer` the leading items that belong, in order, to the blocks of `expected`.
/// Stops at the first item that does not belong to its block; what follows is asked for
/// again.
///
/// # Errors
///
/// Returns [`Failure::NotHeld`] if the answer is empty or its first item belongs to a later
/// block (the peer left the first one out), [`Failure::Invalid`] if the first item belongs to
/// no block asked for, and [`Failure::Undecodable`] if it cannot be read.
fn accept<T>(
    expected: &[Wanted],
    answer: &[Bytes],
    belongs: impl Fn(&Wanted, &Bytes) -> Check<T>,
) -> Result<Vec<T>, Failure> {
    let Some(first) = answer.first() else {
        return Err(Failure::NotHeld);
    };
    let mut accepted = Vec::with_capacity(answer.len());
    for (block, item) in expected.iter().zip(answer) {
        match belongs(block, item) {
            Check::Belongs(kept) => accepted.push(kept),
            Check::Undecodable(reason) if accepted.is_empty() => {
                return Err(Failure::Undecodable(reason));
            }
            Check::NotHeld if accepted.is_empty() => return Err(Failure::NotHeld),
            Check::Other(_) | Check::Undecodable(_) | Check::NotHeld => break,
        }
    }
    if !accepted.is_empty() {
        return Ok(accepted);
    }
    let mismatch = expected.first().map(|block| match belongs(block, first) {
        Check::Other(mismatch) => format!("block {}: {mismatch}", block.header.header.number),
        Check::Belongs(_) | Check::Undecodable(_) | Check::NotHeld => String::new(),
    });
    let later = expected
        .iter()
        .skip(1)
        .any(|block| matches!(belongs(block, first), Check::Belongs(_)));
    Err(if later {
        Failure::NotHeld
    } else {
        Failure::Invalid(format!(
            "an answer that belongs to none of the {} blocks asked for ({})",
            expected.len(),
            mismatch.unwrap_or_default()
        ))
    })
}

/// Checks that `body` is the body of `block`: its transactions hash to the transactions root,
/// its ommers to the ommers hash, and it has a withdrawals list exactly when the header has a
/// withdrawals root. Kept with its transaction count. (An OP Stack block has no withdrawals:
/// the list is absent before Canyon and empty after; from Isthmus the header's root is a
/// storage root, not the list's.)
fn body_of(block: &Wanted, body: &Bytes) -> Check<(Bytes, usize)> {
    let header = &block.header.header;
    let Some(parts) = split_body(body) else {
        return Check::Other("the body is not a block body".to_owned());
    };
    let root = transactions_root(&parts.transactions);
    if root != header.transactions_root {
        return Check::Other(format!(
            "transactions root {root} of {} transactions, the header's {}",
            parts.transactions.len(),
            header.transactions_root
        ));
    }
    if keccak256(parts.ommers) != header.ommers_hash {
        return Check::Other("ommers hash differs".to_owned());
    }
    if parts.withdrawals != header.withdrawals_root.is_some() {
        return Check::Other(format!(
            "withdrawals list {}, header withdrawals root {}",
            if parts.withdrawals {
                "present"
            } else {
                "absent"
            },
            if header.withdrawals_root.is_some() {
                "present"
            } else {
                "absent"
            },
        ));
    }
    Check::Belongs((body.clone(), parts.transactions.len()))
}

/// Checks that `item` holds the receipts of `block`: one per transaction, hashing to its
/// receipts root under the rule of its era. Kept encoded with their blooms.
fn receipts_of(block: &Wanted, item: &Bytes, canyon_time: u64) -> Check<Bytes> {
    let header = &block.header.header;
    match decode_receipts(item, block.transactions) {
        // Asked only for blocks with transactions: an empty list is "not from me".
        Ok(receipts) if receipts.is_empty() => Check::NotHeld,
        Ok(receipts) => {
            let root = receipts_root(&receipts, header.timestamp, canyon_time);
            if root == header.receipts_root {
                Check::Belongs(encode_receipts(&receipts))
            } else {
                Check::Other(format!(
                    "receipts root {root} of {} receipts (time {}, Canyon {canyon_time}), the \
                     header's {}",
                    receipts.len(),
                    header.timestamp,
                    header.receipts_root
                ))
            }
        }
        Err(err @ ReceiptsError::Count { .. }) => Check::Other(err.to_string()),
        Err(ReceiptsError::Rlp(err)) => Check::Undecodable(err.to_string()),
    }
}
