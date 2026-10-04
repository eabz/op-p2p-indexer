//! The range task: blocks fetched from execution peers into the archive, batch by batch, in
//! block order.
//!
//! For each batch: decode the blocks and recover their senders, leave out the blocks the
//! archive already holds (promotion appends to it too), check that the rest extend the
//! archive's last block, then append them to the archive in the encoding they were received
//! in, with their senders. A batch that does not extend the archive is left out with a
//! warning, not an error. It does not fetch or verify the blocks (the execution network does,
//! against a trusted hash) and keeps no progress of its own: the archive's last block is where
//! a restart resumes.
//!
//! A batch is stored whole or the pipeline stops with the error: the archive holds one
//! contiguous range, so a block that cannot be stored cannot be skipped, and a sync that
//! cannot continue must not look like it is running.

use op_indexer_primitives::{ArchivedBlock, BlockRef, DecodedBlock, EncodedBlock};
use op_indexer_storage::{ArchiveStore, StorageError, Store};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::recover::{RecoverError, recover_encoded};
use crate::retry::{retry, settle};
use crate::{PipelineError, metrics};
use op_indexer_storage::RetryError;

const APPEND: &str = "archive append_batch";
const RANGE: &str = "archive range";

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
        let blocks = match recover_encoded(batch.clone()).await {
            Ok(blocks) => blocks,
            Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
            // The block was verified against the chain, so this build cannot read it.
            Err(err) => return Err(PipelineError::RangeBlock(err.to_string())),
        };
        // The bytes as received, with the senders recovered from them, moved: `blocks` is used
        // for numbers and hashes only from here on.
        let mut blocks = blocks;
        let mut archived: Vec<ArchivedBlock> = batch
            .into_iter()
            .zip(&mut blocks)
            .map(|(encoded, block)| ArchivedBlock {
                encoded,
                senders: std::mem::take(&mut block.senders),
            })
            .collect();
        // Promotion appends to the archive too: blocks it already holds are skipped.
        let Some(tip) = archive_tip(&archive, &cancel).await? else {
            return Ok(());
        };
        let Some(skip) = above(tip, &blocks) else {
            continue;
        };
        archived.drain(..skip);
        blocks.drain(..skip);
        let Some(last) = blocks.last().map(|block| block.block.header.number) else {
            continue;
        };
        let append = retry(&cancel, Store::Archive, APPEND, || {
            archive.append_batch(archived.clone())
        });
        match append.await {
            Ok(()) => {}
            Err(RetryError::Cancelled) => return Ok(()),
            // Promotion appended in between: looked at once more against the new tip.
            Err(RetryError::Storage(StorageError::NotContiguous { .. })) => {
                let Some(tip) = archive_tip(&archive, &cancel).await? else {
                    return Ok(());
                };
                let Some(skip) = above(tip, &blocks) else {
                    continue;
                };
                archived.drain(..skip);
                if !archived.is_empty() {
                    let append = retry(&cancel, Store::Archive, APPEND, || {
                        archive.append_batch(archived.clone())
                    });
                    if settle(append.await, APPEND)?.is_none() {
                        return Ok(());
                    }
                }
            }
            Err(RetryError::Storage(source)) => {
                return Err(PipelineError::Storage {
                    operation: APPEND,
                    source,
                });
            }
        }
        debug!(
            blocks = blocks.len(),
            last, "stored a batch of the range sync"
        );
        metrics::range_stored(blocks.len(), last);
    }
}

/// The archive's last block, retried; `None` if `cancel` fired.
async fn archive_tip<A: ArchiveStore>(
    archive: &A,
    cancel: &CancellationToken,
) -> Result<Option<Option<BlockRef>>, PipelineError> {
    let range = retry(cancel, Store::Archive, RANGE, || archive.range());
    Ok(settle(range.await, RANGE)?.map(|range| range.map(|(_, tip)| tip)))
}

/// How many leading blocks of `blocks` (consecutive) the archive, ending at `tip`, already
/// holds, if the rest extend it. `None` if they do not: the round was fetched towards
/// another chain than the archive's. The batch is then left out, with a warning, and the
/// round never reaches its anchor, which the planner gives up after a while; the pipeline
/// carries on.
fn above(tip: Option<BlockRef>, blocks: &[DecodedBlock]) -> Option<usize> {
    let Some(tip) = tip else {
        // An empty archive takes any first block.
        return Some(0);
    };
    let skip = blocks
        .iter()
        .take_while(|block| block.block.header.number <= tip.number)
        .count();
    let Some(first) = blocks.get(skip) else {
        return Some(skip);
    };
    let header = &first.block.header;
    let got = BlockRef {
        number: header.number.saturating_sub(1),
        hash: header.parent_hash,
    };
    if got == tip {
        return Some(skip);
    }
    warn!(
        archive_tip = ?tip,
        block = ?got,
        "a range sync batch does not extend the archive; it is not stored"
    );
    None
}
