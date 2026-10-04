//! The receipts path: asks for the receipts gossip does not carry and attaches them when they
//! come back verified.
//!
//! The pipeline does not fetch or verify receipts; whoever holds the other ends of
//! [`ReceiptsChannels`] does. A request is not tracked: it may never be answered, and blocks
//! left without receipts are asked for again on the next start.

use std::ops::ControlFlow;

use op_indexer_primitives::{BlockRef, DecodedBlock, ReceiptsRequest, VerifiedReceipts};
use op_indexer_storage::{ArchiveStore, StorageError, Store, UnsafeStore};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::PipelineError;
use crate::metrics::{self, RequestOutcome, UnmatchedReason};
use crate::retry::{RetryError, retry, settle};

/// Most stored blocks checked for missing receipts at startup, newest first. The walk also
/// ends when the request channel is full; older blocks wait for the next start.
const STARTUP_BLOCKS: usize = 1024;

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
    let outcome = match requests.try_send(ReceiptsRequest::from(block)) {
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
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
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
        // Pruned, expired or trimmed in the meantime.
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
