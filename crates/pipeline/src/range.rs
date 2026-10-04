//! The range task: blocks fetched from execution peers into the archive, batch by batch, in
//! block order.
//!
//! For each batch: decode the blocks and recover their senders, then append them to the
//! archive in the encoding they were received in, with their senders. The archive leaves out
//! the blocks it already holds (promotion appends to it too). It does not fetch or verify the
//! blocks (the execution network does, against a trusted hash) and keeps no progress of its
//! own: the archive's last block is where a restart resumes.
//!
//! A batch that extends the archive is stored whole or the pipeline stops with the error: the
//! archive holds one contiguous range, so a block that cannot be stored cannot be skipped,
//! and a sync that cannot continue must not look like it is running. One that does not extend
//! it (fetched towards another chain than the archive's) is skipped with a warning, and the
//! planner gives its round up.

use op_indexer_primitives::{ArchivedBlock, EncodedBlock};
use op_indexer_storage::{ArchiveStore, RetryError, StorageError, Store};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::recover::{RecoverError, recover_encoded};
use crate::retry::retry;
use crate::{PipelineError, metrics};

const APPEND: &str = "archive append_batch";

/// Stores every batch received on `batches` (verified blocks in ascending order, each batch
/// consecutive) until the channel closes or `cancel` fires. A batch being stored when
/// `cancel` fires is finished unless a store is failing.
///
/// # Errors
///
/// Returns [`PipelineError::RangeBlock`] if a block cannot be decoded or a sender recovered,
/// [`PipelineError::Storage`] if a store fails in a way retrying cannot fix, and
/// [`PipelineError::Task`] if decoding panics.
pub(crate) async fn run<A: ArchiveStore>(
    archive: A,
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
        let mut blocks = match recover_encoded(batch.clone()).await {
            Ok(blocks) => blocks,
            Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
            // The block was verified against the chain, so this build cannot read it.
            Err(err) => return Err(PipelineError::RangeBlock(err.to_string())),
        };
        let Some(last) = blocks.last().map(|block| block.block.header.number) else {
            continue;
        };
        // The bytes as received, with the senders recovered from them.
        let archived: Vec<ArchivedBlock> = batch
            .into_iter()
            .zip(&mut blocks)
            .map(|(encoded, block)| ArchivedBlock {
                encoded,
                senders: std::mem::take(&mut block.senders),
            })
            .collect();
        let count = archived.len();
        let append = retry(&cancel, Store::Archive, APPEND, || {
            archive.append_batch(archived.clone())
        });
        match append.await {
            Ok(()) => {
                debug!(blocks = count, last, "stored a batch of the range sync");
                metrics::range_stored(count, last);
            }
            Err(RetryError::Cancelled) => return Ok(()),
            Err(RetryError::Storage(StorageError::NotContiguous { expected, got })) => {
                warn!(
                    archive_tip = ?expected,
                    block = ?got,
                    "a range sync batch does not extend the archive; it is not stored"
                );
            }
            Err(RetryError::Storage(source)) => {
                return Err(PipelineError::Storage {
                    operation: APPEND,
                    source,
                });
            }
        }
    }
}
