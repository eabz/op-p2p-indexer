//! The range task: blocks fetched from execution peers into the committed store and the
//! archive, batch by batch, in block order.
//!
//! For each batch: recover the senders, insert the typed blocks into the committed store,
//! append the blocks to the archive in the encoding they were received in, then publish how
//! far the range is stored. It does not fetch or verify the blocks (the execution network
//! does, against a trusted hash) and does not save the progress (the binary does).
//!
//! A batch is stored whole or the task stops: the archive holds one contiguous range, so a
//! block that cannot be stored cannot be skipped. Stopping closes the channel, which stops
//! the fetch; the rest of the pipeline carries on.

use std::ops::ControlFlow;

use alloy_primitives::BlockNumber;
use op_indexer_primitives::SyncedBlock;
use op_indexer_storage::{ArchiveStore, CommittedStore, StorageError, Store};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::PipelineError;
use crate::metrics::{self, RangeStop};
use crate::recover::{RecoverError, recover_synced};
use crate::retry::{RetryError, retry};

const INSERT: &str = "committed insert (range)";
const APPEND: &str = "archive append_batch";

/// The pipeline's ends of the channels to whatever fetches a range of blocks.
#[derive(Debug)]
pub struct RangeChannels {
    /// Verified blocks, in ascending order, in batches of consecutive blocks. The range task
    /// stops when the channel closes.
    pub batches: mpsc::Receiver<Vec<SyncedBlock>>,
    /// Receives the number of the last block of each batch once the batch is stored: every
    /// block received up to it is in the committed store and the archive.
    pub stored: watch::Sender<Option<BlockNumber>>,
}

/// Stores every batch received until the channel closes or `cancel` fires. A batch being
/// stored when `cancel` fires is finished unless a store is failing.
///
/// # Errors
///
/// Returns [`PipelineError::Storage`] if a store fails with an error that is neither transient
/// nor a refusal of the batch, and [`PipelineError::Task`] if sender recovery panics.
pub(crate) async fn run<C: CommittedStore, A: ArchiveStore>(
    committed: C,
    archive: Option<A>,
    channels: RangeChannels,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    let RangeChannels {
        mut batches,
        stored,
    } = channels;
    loop {
        let batch = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            batch = batches.recv() => batch,
        };
        // A closed channel is the fetch ending.
        let Some(batch) = batch else { return Ok(()) };
        let Some(last) = store(&committed, archive.as_ref(), batch, &cancel).await? else {
            return Ok(());
        };
        stored.send_replace(Some(last));
    }
}

/// Stores one batch and returns the number of its last block. `None` when the task has to
/// stop: cancellation ended a retry, or the batch cannot be stored.
async fn store<C: CommittedStore, A: ArchiveStore>(
    committed: &C,
    archive: Option<&A>,
    batch: Vec<SyncedBlock>,
    cancel: &CancellationToken,
) -> Result<Option<BlockNumber>, PipelineError> {
    let (blocks, encoded) = match recover_synced(batch).await {
        Ok(recovered) => recovered,
        Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
        // The block was verified against the chain, so this build cannot read it.
        Err(err @ RecoverError::Sender { .. }) => {
            error!(%err, "range sync stopped: a verified block cannot be stored");
            metrics::range_stopped(RangeStop::SenderRecovery);
            return Ok(None);
        }
    };
    let (Some(first), Some(last)) = (blocks.first(), blocks.last()) else {
        return Ok(None);
    };
    let (first, last) = (first.block.header.number, last.block.header.number);

    let insert = retry(cancel, Store::Committed, INSERT, || {
        committed.insert(&blocks)
    });
    if refused(insert.await, INSERT, first, last)?.is_break() {
        return Ok(None);
    }
    if let Some(archive) = archive {
        // The clone is of reference-counted buffers; the bytes are not copied.
        let append = retry(cancel, Store::Archive, APPEND, || {
            archive.append_batch(encoded.clone())
        });
        if refused(append.await, APPEND, first, last)?.is_break() {
            return Ok(None);
        }
    }
    debug!(first, last, "stored a batch of the range sync");
    metrics::range_stored(blocks.len(), last);
    Ok(Some(last))
}

/// Sorts the end of a store call: done, stop the task (cancelled, or the store refuses the
/// batch), or a failure of the store itself.
fn refused(
    result: Result<(), RetryError>,
    operation: &'static str,
    first: BlockNumber,
    last: BlockNumber,
) -> Result<ControlFlow<()>, PipelineError> {
    match result {
        Ok(()) => Ok(ControlFlow::Continue(())),
        Err(RetryError::Cancelled) => Ok(ControlFlow::Break(())),
        // The store refuses these blocks, not the store itself. For the archive, "not
        // contiguous" means the range does not start where the archive ends.
        Err(RetryError::Storage(
            err @ (StorageError::InvalidBlock { .. }
            | StorageError::UnsupportedTransaction { .. }
            | StorageError::NotContiguous { .. }),
        )) => {
            error!(first, last, operation, %err, "range sync stopped: a store refuses the batch");
            metrics::range_stopped(RangeStop::Refused);
            Ok(ControlFlow::Break(()))
        }
        Err(RetryError::Storage(source)) => Err(PipelineError::Storage { operation, source }),
    }
}
