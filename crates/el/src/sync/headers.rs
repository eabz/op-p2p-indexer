//! Headers of the range sync: the hash chain down from a trusted block.
//!
//! A header is accepted only if its bytes hash to what the block above it names as parent
//! (or, for the first one, to the trusted hash). Nothing else about a header is checked: the
//! hash commits to all of it. Does not fetch bodies or receipts (`segment`).

use alloy_consensus::Header;
use alloy_primitives::{B256, BlockNumber, Bytes, keccak256};
use op_indexer_primitives::BlockRef;

use super::{Failure, SEGMENT_BLOCKS};
use crate::session::SessionHandle;

/// Headers asked for in one request of the walk. Peers answer with at most 1024.
pub(super) const HEADERS_PER_REQUEST: u64 = 1024;

/// A header whose bytes hash to what its child (or the anchor) names.
#[derive(Debug)]
pub(super) struct VerifiedHeader {
    pub(super) hash: B256,
    /// The header as received.
    pub(super) raw: Bytes,
    pub(super) header: Header,
}

/// Fetches one page of headers going down from `start` and returns the checkpoints in it,
/// highest first: every [`SEGMENT_BLOCKS`]-th block below `start`, and the parent of the
/// page's last header, where the next page starts. No header below `first` is asked for; when
/// the page reaches `first`, the parent that block names is returned too.
pub(super) async fn walk(
    session: &SessionHandle,
    start: BlockRef,
    first: BlockNumber,
) -> Result<(Vec<BlockRef>, Option<BlockRef>), Failure> {
    let limit = start
        .number
        .saturating_sub(first)
        .saturating_add(1)
        .min(HEADERS_PER_REQUEST);
    let raw = session.headers(start.hash, limit).await?;
    let headers = verify_chain(raw, start)?;
    let mut checkpoints: Vec<BlockRef> = headers
        .iter()
        .step_by(usize::try_from(SEGMENT_BLOCKS).unwrap_or(usize::MAX))
        .skip(1)
        .map(|header| BlockRef {
            number: header.header.number,
            hash: header.hash,
        })
        .collect();
    let below = headers.last().map(|last| BlockRef {
        number: last.header.number.saturating_sub(1),
        hash: last.header.parent_hash,
    });
    let reached = headers
        .last()
        .is_some_and(|last| last.header.number == first);
    if !reached && let Some(below) = below {
        checkpoints.push(below);
    }
    Ok((checkpoints, below.filter(|_| reached)))
}

/// Checks that `raw` are consecutive headers going down from `top`: the first hashes to
/// `top.hash`, each next one to the parent hash of the one before. Returns them as received,
/// highest first.
pub(super) fn verify_chain(raw: Vec<Bytes>, top: BlockRef) -> Result<Vec<VerifiedHeader>, Failure> {
    if raw.is_empty() {
        return Err(Failure::NotHeld);
    }
    let mut expected = top;
    let mut headers = Vec::with_capacity(raw.len());
    for bytes in raw {
        let hash = keccak256(&bytes);
        if hash != expected.hash {
            return Err(Failure::Invalid(format!(
                "header at block {} hashes to {hash}, not {}",
                expected.number, expected.hash
            )));
        }
        // From here the bytes are the chain's: what cannot be read is not the peer's fault.
        let header: Header = alloy_rlp::decode_exact(&bytes)
            .map_err(|err| Failure::Unsupported(format!("header {hash} does not decode: {err}")))?;
        if header.number != expected.number {
            return Err(Failure::Unsupported(format!(
                "block {hash} has number {}, not {}: the anchor's number is wrong",
                header.number, expected.number
            )));
        }
        expected = BlockRef {
            // Nothing lies below block 0; a header after it fails the hash check.
            number: header.number.saturating_sub(1),
            hash: header.parent_hash,
        };
        headers.push(VerifiedHeader {
            hash,
            raw: bytes,
            header,
        });
    }
    Ok(headers)
}
