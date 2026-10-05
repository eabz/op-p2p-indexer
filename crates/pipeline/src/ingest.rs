//! The ingest task: gossiped blocks into the unsafe store, in arrival order.
//!
//! For each block: recover the senders, insert, log what the store's fork choice did
//! with it. It does not order or deduplicate blocks (the store does) and does not promote them
//! (the promotion task does).

use std::ops::ControlFlow;

use op_indexer_primitives::{BlockRef, ReceiptsRequest, UnsafeBlock, UnsafeEvent};
use op_indexer_storage::{StorageError, Store, UnsafeStore};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::PipelineError;
use crate::receipts;
use crate::recover::{RecoverError, recover};
use crate::retry::{RetryError, retry};

/// Name of the one store call ingest makes, for errors and retry logs.
const INSERT: &str = "unsafe insert";

/// Stores every block received on `blocks` until the channel closes or `cancel` fires, and
/// asks for the receipts of each block it stores on `receipts`, when there is a fetcher, and
/// publishes the unsafe head on `head` whenever it moves, when something follows it.
///
/// On cancellation the blocks already in the channel are still stored, without waiting for
/// more; a store that is failing then is not waited for.
///
/// # Errors
///
/// Returns [`PipelineError::Storage`] if the unsafe store fails with an error that is neither
/// transient nor a rejection of one block, and [`PipelineError::Task`] if sender recovery
/// panics.
pub(crate) async fn run<U: UnsafeStore>(
    store: U,
    mut blocks: mpsc::Receiver<UnsafeBlock>,
    receipts: Option<mpsc::Sender<ReceiptsRequest>>,
    head: Option<watch::Sender<Option<BlockRef>>>,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    let (receipts, head) = (receipts.as_ref(), head.as_ref());

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            block = blocks.recv() => {
                // A closed channel is the network shutting down.
                let Some(block) = block else { return Ok(()) };
                let ingested = ingest(&store, block, receipts, head, &cancel);
                if ingested.await?.is_break() {
                    return Ok(());
                }
            }
        }
    }
    while let Ok(block) = blocks.try_recv() {
        let ingested = ingest(&store, block, receipts, head, &cancel);
        if ingested.await?.is_break() {
            break;
        }
    }
    Ok(())
}

/// Stores one block. Breaks when cancellation ended a retry, so the caller stops.
async fn ingest<U: UnsafeStore>(
    store: &U,
    block: UnsafeBlock,
    receipts: Option<&mpsc::Sender<ReceiptsRequest>>,
    head: Option<&watch::Sender<Option<BlockRef>>>,
    cancel: &CancellationToken,
) -> Result<ControlFlow<()>, PipelineError> {
    let (number, hash) = (block.number(), block.hash);
    let block = match recover(block).await {
        Ok(block) => block,
        Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
        // The sequencer signed the block, so this is not expected.
        Err(err @ (RecoverError::Sender { .. } | RecoverError::Decode { .. })) => {
            warn!(number, %hash, %err, "dropped block");
            return Ok(ControlFlow::Continue(()));
        }
    };

    let outcome = match retry(cancel, Store::Unsafe, INSERT, || store.insert(&block)).await {
        Ok(outcome) => outcome,
        Err(RetryError::Cancelled) => return Ok(ControlFlow::Break(())),
        Err(RetryError::Storage(err)) => {
            // The store refuses this block, not the store itself: drop it and carry on.
            if !matches!(
                err,
                StorageError::InvalidBlock { .. } | StorageError::UnsupportedTransaction { .. }
            ) {
                return Err(PipelineError::Storage {
                    operation: INSERT,
                    source: err,
                });
            }
            warn!(number, %hash, %err, "dropped block");
            return Ok(ControlFlow::Continue(()));
        }
    };

    // Not stored: a retry of an insert that had been applied, or a block the store already
    // had or no longer wants. Its events, if any, went to the stream the first time.
    if outcome.stored
        && let Some(requests) = receipts
    {
        // Never waited on; a dropped request is asked again at the next start.
        let _sent = receipts::request(requests, &block);
    }
    debug!(number, %hash, stored = outcome.stored, "ingested block");
    for event in &outcome.events {
        record(event);
        let moved = match event {
            UnsafeEvent::NewHead { head, .. } => Some(*head),
            UnsafeEvent::Reorg(reorg) => Some(reorg.new_head),
            UnsafeEvent::Filled(_) | UnsafeEvent::Receipts(_) | UnsafeEvent::Pruned { .. } => None,
        };
        if let (Some(head), Some(moved)) = (head, moved) {
            head.send_replace(Some(moved));
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// Logs one thing fork choice did.
fn record(event: &UnsafeEvent) {
    match event {
        UnsafeEvent::NewHead { head, gap: true } => {
            info!(number = head.number, hash = %head.hash, "unsafe head moved past a gap");
        }
        UnsafeEvent::NewHead { head, gap: false } => {
            debug!(number = head.number, hash = %head.hash, "unsafe head moved");
        }
        UnsafeEvent::Reorg(reorg) => {
            info!(
                old_head = %reorg.old_head.hash,
                new_head = %reorg.new_head.hash,
                ancestor = ?reorg.common_ancestor.map(|ancestor| ancestor.number),
                depth = reorg.replaced.len(),
                "unsafe chain reorganized"
            );
        }
        UnsafeEvent::Filled(block) => {
            info!(number = block.number, hash = %block.hash, "gap in the unsafe chain filled");
        }
        // Insert does not produce these.
        UnsafeEvent::Receipts(_) | UnsafeEvent::Pruned { .. } => {}
    }
}
