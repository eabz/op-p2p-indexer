//! The receipts path: asks for the receipts gossip does not carry and attaches them when they
//! come back verified.
//!
//! The pipeline does not fetch or verify receipts; whoever holds the other ends of
//! [`ReceiptsChannels`] does. A request is not tracked: it may never be answered. Unsafe
//! blocks left without receipts are asked for again on the next start; archived ones (promoted
//! before their receipts arrived) are listed by the archive and asked for again every
//! [`ARCHIVE_RECEIPTS_INTERVAL`] until they are filled, so the archive never stays without
//! them.

use std::ops::ControlFlow;
use std::time::Duration;

use alloy_consensus::Header;
use alloy_primitives::BlockNumber;
use op_indexer_primitives::{
    ArchivedBlock, BlockRef, DecodedBlock, ReadLimits, ReceiptsRequest, VerifiedReceipts,
};
use op_indexer_storage::{ArchiveStore, StorageError, Store, UnsafeStore};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::PipelineError;
use crate::metrics::{self, RequestOutcome, UnmatchedReason};
use crate::retry::{RetryError, retry, settle};

/// Most stored blocks checked for missing receipts at startup, newest first. The walk also
/// ends when the request channel is full; older blocks wait for the next start.
const STARTUP_BLOCKS: usize = 1024;
/// How often the archived blocks without receipts are asked for: a low pace next to the
/// requests for new blocks, which they must not crowd out.
const ARCHIVE_RECEIPTS_INTERVAL: Duration = Duration::from_secs(30);
/// Archived blocks without receipts asked for per round, oldest first.
const ARCHIVE_RECEIPTS_PER_ROUND: usize = 64;

/// The pipeline's ends of the two channels to the receipts fetcher.
#[derive(Debug)]
pub struct ReceiptsChannels {
    /// Blocks whose receipts are wanted. Never waited on: a request that does not fit is
    /// dropped and counted.
    pub requests: mpsc::Sender<ReceiptsRequest>,
    /// Receipts the fetcher has verified against the block's receipts root.
    pub verified: mpsc::Receiver<VerifiedReceipts>,
}

/// Asks the fetcher for the receipts of `block`, without waiting. Returns whether the request
/// was handed over.
pub(crate) fn request(requests: &mpsc::Sender<ReceiptsRequest>, block: &DecodedBlock) -> bool {
    send(requests, ReceiptsRequest::from(block))
}

/// Hands `request` to the fetcher without waiting. Returns whether it was taken.
fn send(requests: &mpsc::Sender<ReceiptsRequest>, request: ReceiptsRequest) -> bool {
    let outcome = match requests.try_send(request) {
        Ok(()) => RequestOutcome::Sent,
        // A full channel means the fetcher is behind; a closed one that it has stopped.
        Err(TrySendError::Full(_) | TrySendError::Closed(_)) => RequestOutcome::Dropped,
    };
    metrics::receipts_requested(outcome);
    matches!(outcome, RequestOutcome::Sent)
}

/// Requests the receipts of stored blocks that lack them, then attaches every verified answer
/// until `cancel` fires or the fetcher closes its channel.
///
/// # Errors
///
/// Returns [`PipelineError::Storage`] if a store fails in a way that is neither transient nor
/// a refusal of one block's receipts.
pub(crate) async fn run<U: UnsafeStore, A: ArchiveStore>(
    unsafe_store: U,
    archive: A,
    channels: ReceiptsChannels,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    let ReceiptsChannels {
        requests,
        mut verified,
    } = channels;
    if request_missing(&unsafe_store, &requests, &cancel)
        .await?
        .is_break()
    {
        return Ok(());
    }
    // Where the next round of archived blocks starts: after the last one asked for.
    let mut next_archived = 0;
    let Some(pending) = request_archived(&archive, &requests, &mut next_archived, &cancel).await?
    else {
        return Ok(());
    };
    info!(pending, "archived blocks without receipts at startup");
    let mut archived = interval_at(
        Instant::now() + ARCHIVE_RECEIPTS_INTERVAL,
        ARCHIVE_RECEIPTS_INTERVAL,
    );
    archived.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            _ = archived.tick() => {
                let round = request_archived(&archive, &requests, &mut next_archived, &cancel);
                if round.await?.is_none() {
                    return Ok(());
                }
            }
            receipts = verified.recv() => {
                // A closed channel is the fetcher shutting down.
                let Some(receipts) = receipts else { return Ok(()) };
                if attach(&unsafe_store, &archive, &receipts, &cancel).await?.is_break() {
                    return Ok(());
                }
            }
        }
    }
}

/// Walks the unsafe chain back from its head and requests the receipts of every block without
/// them, at most [`STARTUP_BLOCKS`] blocks. Stops at the first block that is no longer stored,
/// or when the fetcher takes no more requests.
async fn request_missing<U: UnsafeStore>(
    store: &U,
    requests: &mpsc::Sender<ReceiptsRequest>,
    cancel: &CancellationToken,
) -> Result<ControlFlow<()>, PipelineError> {
    const HEAD: &str = "unsafe head";
    const BLOCK: &str = "unsafe block";

    let Some(head) = call(cancel, HEAD, || store.head()).await? else {
        return Ok(ControlFlow::Break(()));
    };
    let Some(head) = head else {
        return Ok(ControlFlow::Continue(()));
    };
    let (mut hash, mut requested) = (head.hash, 0_usize);
    for _ in 0..STARTUP_BLOCKS {
        let Some(block) = call(cancel, BLOCK, || store.block(hash)).await? else {
            return Ok(ControlFlow::Break(()));
        };
        let Some(block) = block else { break };
        if block.receipts.is_none() {
            if !request(requests, &block) {
                break;
            }
            requested += 1;
        }
        hash = block.block.header.parent_hash;
    }
    info!(
        head = head.number,
        requested, "requested receipts for stored blocks"
    );
    Ok(ControlFlow::Continue(()))
}

/// Asks for the receipts of archived blocks without them from `next` up (wrapping round to the
/// lowest), at most [`ARCHIVE_RECEIPTS_PER_ROUND`], stopping when the fetcher takes no more,
/// and moves `next` past the last one asked for: blocks whose receipts no peer serves do not
/// hold back the others. The request is built from the archived header, which the answer is
/// verified against. Returns how many archived blocks are without receipts, or `None` when
/// cancellation ended a read.
async fn request_archived<A: ArchiveStore>(
    archive: &A,
    requests: &mpsc::Sender<ReceiptsRequest>,
    next: &mut BlockNumber,
    cancel: &CancellationToken,
) -> Result<Option<u64>, PipelineError> {
    const PENDING: &str = "archive pending_receipts";
    const BLOCKS: &str = "archive blocks";
    let one = ReadLimits {
        items: 1,
        bytes: usize::MAX,
        lowest: 0,
    };
    let pending = retry(cancel, Store::Archive, PENDING, || {
        archive.pending_receipts(*next, ARCHIVE_RECEIPTS_PER_ROUND)
    });
    let Some((blocks, total)) = settle(pending.await, PENDING)? else {
        return Ok(None);
    };
    metrics::archive_pending_receipts(total);
    for block in blocks {
        *next = block.number.saturating_add(1);
        let read = retry(cancel, Store::Archive, BLOCKS, || {
            archive.blocks(block.number, one)
        });
        let Some(mut read) = settle(read.await, BLOCKS)? else {
            return Ok(None);
        };
        // Gone or replaced since it was listed: nothing to ask for.
        let Some(ArchivedBlock { encoded, senders }) = read
            .pop()
            .filter(|archived| archived.encoded.hash == block.hash)
        else {
            continue;
        };
        // The archive checked that these bytes hash to the block's hash when it stored them.
        let Ok(header) = alloy_rlp::decode_exact::<Header>(&encoded.header) else {
            warn!(number = block.number, "an archived header does not decode");
            continue;
        };
        let request = ReceiptsRequest {
            block,
            receipts_root: header.receipts_root,
            timestamp_secs: header.timestamp,
            // The archive keeps one sender per transaction.
            transaction_count: senders.len(),
        };
        if !send(requests, request) {
            // Not asked for: the next round starts with it.
            *next = block.number;
            break;
        }
    }
    Ok(Some(total))
}

/// Attaches verified receipts to their block: in the unsafe store, or in the archive if the
/// block has been promoted. Breaks when cancellation ended a retry.
async fn attach<U: UnsafeStore, A: ArchiveStore>(
    unsafe_store: &U,
    archive: &A,
    verified: &VerifiedReceipts,
    cancel: &CancellationToken,
) -> Result<ControlFlow<()>, PipelineError> {
    let VerifiedReceipts { block, receipts } = verified;
    let block = *block;
    let in_unsafe = set(cancel, Store::Unsafe, "unsafe set_receipts", block, || {
        unsafe_store.set_receipts(block, receipts)
    });
    let Some(held) = in_unsafe.await? else {
        return Ok(ControlFlow::Break(()));
    };
    if held {
        return Ok(ControlFlow::Continue(()));
    }

    // Not in the unsafe store any more: promoted to the archive.
    let in_archive = set(
        cancel,
        Store::Archive,
        "archive set_receipts",
        block,
        || archive.set_receipts(block, receipts),
    );
    let Some(held) = in_archive.await? else {
        return Ok(ControlFlow::Break(()));
    };
    if !held {
        // Pruned or expired in the meantime.
        debug!(number = block.number, hash = %block.hash, "dropped receipts for an unknown block");
        metrics::receipts_unmatched(UnmatchedReason::UnknownBlock);
    }
    Ok(ControlFlow::Continue(()))
}

/// Runs one `set_receipts` call on `store` through the retry helper and records what came of
/// it. Returns whether the store holds the block, or `None` if cancellation ended the retry.
///
/// A store that holds the block but refuses the receipts counts as holding it: the refusal is
/// logged and counted here. The fetcher verified the receipts against the receipts root, so a
/// wrong count or number means the block reference does not name the block it claims to.
async fn set<F, Fut>(
    cancel: &CancellationToken,
    store: Store,
    operation: &'static str,
    block: BlockRef,
    call: F,
) -> Result<Option<bool>, PipelineError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, StorageError>>,
{
    match retry(cancel, store, operation, call).await {
        Ok(true) => {
            debug!(number = block.number, hash = %block.hash, %store, "attached receipts");
            metrics::receipts_attached(store);
            Ok(Some(true))
        }
        Ok(false) => Ok(Some(false)),
        Err(RetryError::Cancelled) => Ok(None),
        Err(RetryError::Storage(err @ StorageError::InvalidBlock { .. })) => {
            warn!(number = block.number, hash = %block.hash, %err, "dropped receipts");
            metrics::receipts_unmatched(UnmatchedReason::Refused);
            Ok(Some(true))
        }
        Err(RetryError::Storage(source)) => Err(PipelineError::Storage { operation, source }),
    }
}

/// Runs one read of the unsafe store through the retry helper. `None` when cancelled.
async fn call<T, F, Fut>(
    cancel: &CancellationToken,
    operation: &'static str,
    read: F,
) -> Result<Option<T>, PipelineError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    settle(
        retry(cancel, Store::Unsafe, operation, read).await,
        operation,
    )
}
