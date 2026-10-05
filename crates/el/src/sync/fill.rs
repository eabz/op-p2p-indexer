//! Fetching what gossip missed: the spans of the unsafe chain the pipeline asks for
//! ([`FillRequest`]), within a short way of the head.
//!
//! A span is fetched from its top down, [`FILL_SEGMENT`] blocks at a time, each segment as range
//! sync fetches one: headers verified by the hash chain down from the segment's top, whose hash
//! the stored block above the span names, then bodies and receipts checked against them. Each
//! verified segment goes to the pipeline at once, so the gap closes from the head down. A
//! segment no peer serves is tried again with a growing pause, then left to range sync.

use std::time::Duration;

use alloy_consensus::Header;
use op_indexer_primitives::{BlockRef, EncodedBlock, FillRequest};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{Segment, segment};
use crate::ElError;
use crate::peers::Peers;

/// Blocks fetched in one go: a few requests' worth, small next to a range sync segment.
const FILL_SEGMENT: u64 = 64;
/// The pause before a segment is tried again, doubled each time up to [`MAX_RETRY_WAIT`].
const FIRST_RETRY_WAIT: Duration = Duration::from_secs(2);
const MAX_RETRY_WAIT: Duration = Duration::from_secs(30);
/// Tries of one segment before the span is left to range sync.
const MAX_ATTEMPTS: u32 = 6;

/// Fetches the spans asked for on `requests` and sends their blocks on `filled`, a verified
/// segment at a time (ascending), until `cancel` fires or either channel closes.
///
/// # Errors
///
/// Never fails; the result is that of the network's tasks.
pub(crate) async fn run(
    canyon_time: u64,
    peers: Peers,
    mut requests: mpsc::Receiver<FillRequest>,
    filled: mpsc::Sender<Vec<EncodedBlock>>,
    cancel: CancellationToken,
) -> Result<(), ElError> {
    loop {
        let request = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            request = requests.recv() => request,
        };
        let Some(request) = request else {
            return Ok(());
        };
        if !fill(canyon_time, &peers, request, &filled, &cancel).await {
            return Ok(());
        }
    }
}

/// Fetches one span. Returns `false` when the node is stopping.
async fn fill(
    canyon_time: u64,
    peers: &Peers,
    request: FillRequest,
    filled: &mpsc::Sender<Vec<EncodedBlock>>,
    cancel: &CancellationToken,
) -> bool {
    let mut top = request.top;
    let (mut attempts, mut wait) = (0_u32, FIRST_RETRY_WAIT);
    loop {
        let first = request
            .first
            .max(top.number.saturating_sub(FILL_SEGMENT - 1));
        let sessions = peers.sessions();
        let session = sessions.iter().find(|session| {
            let range = session.range();
            session.is_askable() && range.earliest <= first && top.number <= range.latest
        });
        let fetched = match session {
            Some(session) => segment::fetch(session, Segment { first, top }, canyon_time)
                .await
                .map_err(|failure| failure.to_string()),
            None => Err("no peer says it holds these blocks".to_owned()),
        };
        match fetched {
            Ok(blocks) => {
                let parent = blocks
                    .first()
                    .and_then(|block| alloy_rlp::decode_exact::<Header>(&block.header).ok())
                    .map(|header| header.parent_hash);
                if filled.send(blocks).await.is_err() {
                    return false;
                }
                let (Some(hash), Some(number)) = (parent, first.checked_sub(1)) else {
                    return true;
                };
                if first <= request.first {
                    return true;
                }
                top = BlockRef { number, hash };
                (attempts, wait) = (0, FIRST_RETRY_WAIT);
            }
            Err(reason) => {
                attempts = attempts.saturating_add(1);
                if attempts >= MAX_ATTEMPTS {
                    warn!(
                        from = request.first,
                        to = top.number,
                        %reason,
                        "missed unsafe blocks not fetched; range sync will"
                    );
                    return true;
                }
                debug!(from = first, to = top.number, %reason, "missed blocks: trying again");
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return false,
                    () = tokio::time::sleep(wait) => {}
                }
                wait = wait.saturating_mul(2).min(MAX_RETRY_WAIT);
            }
        }
    }
}
