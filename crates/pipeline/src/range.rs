//! The range task: blocks fetched from execution peers into the committed store and the
//! archive, batch by batch, in block order.
//!
//! For each batch: decode the blocks and recover their senders, insert the typed blocks into
//! the committed store, then append the blocks to the archive in the encoding they were
//! received in. It does not fetch or verify the blocks (the execution network does, against a
//! trusted hash) and keeps no progress of its own: the archive's last block is where a
//! restart resumes.
//!
//! A batch is stored whole or the pipeline stops with the error: the archive holds one
//! contiguous range, so a block that cannot be stored cannot be skipped, and a sync that
//! cannot continue must not look like it is running.

use op_indexer_primitives::EncodedBlock;
use op_indexer_storage::{ArchiveStore, CommittedStore, Store};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::recover::{RecoverError, recover_encoded};
use crate::retry::{RetryError, retry};
use crate::{PipelineError, metrics};

const INSERT: &str = "committed insert (range)";
const APPEND: &str = "archive append_batch";

/// Stores every batch received on `batches` (verified blocks in ascending order, each batch
/// consecutive) until the channel closes or `cancel` fires. A batch being stored when
/// `cancel` fires is finished unless a store is failing.
///
/// # Errors
///
/// Returns [`PipelineError::RangeBlock`] if a block cannot be decoded or a sender recovered,
/// [`PipelineError::Storage`] if a store refuses a batch (for the archive: it does not end
/// where the batch starts) or fails in a way retrying cannot fix, and
/// [`PipelineError::Task`] if decoding panics.
pub(crate) async fn run<C: CommittedStore, A: ArchiveStore>(
    committed: C,
    archive: Option<A>,
    mut batches: mpsc::Receiver<Vec<EncodedBlock>>,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    loop {
        let batch = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            batch = batches.recv() => batch,
        };
        // A closed channel is the fetch ending.
        let Some(batch) = batch else { return Ok(()) };
        let blocks = match recover_encoded(batch.clone()).await {
            Ok(blocks) => blocks,
            Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
            // The block was verified against the chain, so this build cannot read it.
            Err(err) => return Err(PipelineError::RangeBlock(err.to_string())),
        };
        let Some(last) = blocks.last().map(|block| block.block.header.number) else {
            continue;
        };

        let insert = retry(&cancel, Store::Committed, INSERT, || {
            committed.insert(&blocks)
        });
        match insert.await {
            Ok(()) => {}
            Err(RetryError::Cancelled) => return Ok(()),
            Err(RetryError::Storage(source)) => {
                return Err(PipelineError::Storage {
                    operation: INSERT,
                    source,
                });
            }
        }
        if let Some(archive) = &archive {
            // The clone is of reference-counted buffers; the bytes are not copied.
            let append = retry(&cancel, Store::Archive, APPEND, || {
                archive.append_batch(batch.clone())
            });
            match append.await {
                Ok(()) => {}
                Err(RetryError::Cancelled) => return Ok(()),
                Err(RetryError::Storage(source)) => {
                    return Err(PipelineError::Storage {
                        operation: APPEND,
                        source,
                    });
                }
            }
        }
        debug!(
            blocks = blocks.len(),
            last, "stored a batch of the range sync"
        );
        metrics::range_stored(blocks.len(), last);
    }
}
