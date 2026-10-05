//! Missed gossip blocks: finding the spans of the unsafe chain gossip skipped, and storing the
//! blocks peers sent for them.
//!
//! When the unsafe store's head jumps over heights it does not hold, [`missing_below`] finds
//! the span under the head: from the parent of the lowest stored block above the hole (its hash
//! is what that block names, so the span is trusted through it) down to the next stored block,
//! within [`MAX_FILL_DEPTH`] of the head. Deeper holes are range sync's. The fill task then
//! stores what the fetcher returns, highest block first, so each closes the gap below the
//! canonical block above it (the store's fork choice, "closes a gap below the head").
//!
//! It does not fetch (the fetcher does, verified by the hash chain down from the span's top)
//! and does not decide which spans are worth asking for again.

use op_indexer_primitives::{BlockRef, EncodedBlock, FillRequest};
use op_indexer_storage::{StorageError, Store, UnsafeStore};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::PipelineError;
use crate::ingest::INSERT;
use crate::recover::{RecoverError, recover_encoded};
use crate::retry::{RetryError, retry};

/// Heights below the unsafe head a missed span may reach to be fetched: about what promotion
/// reads back from a head. Deeper holes are left to range sync.
pub(crate) const MAX_FILL_DEPTH: u64 = 1024;

/// The span of missing heights right below `head`, if its parent is not stored: up to the
/// parent of the lowest stored block above it, down to the next stored height, the store's
/// lowest, or [`MAX_FILL_DEPTH`] below the head. `None` if nothing is missing or the store
/// cannot be read.
pub(crate) async fn missing_below<U: UnsafeStore>(
    store: &U,
    head: BlockRef,
) -> Option<FillRequest> {
    let lowest = store.lowest().await.ok()??;
    let floor = head.number.saturating_sub(MAX_FILL_DEPTH).max(lowest);
    // The run of canonical blocks down from the head, by their headers: it ends where a
    // height is missing (or no longer links).
    let count = usize::try_from(head.number.saturating_sub(floor))
        .ok()?
        .saturating_add(1);
    let run = store
        .canonical_headers(head.number, count, false)
        .await
        .ok()?;
    let lowest_held = run.last()?;
    let top = BlockRef {
        number: lowest_held.block.number.checked_sub(1)?,
        hash: lowest_held.parent_hash,
    };
    if top.number < floor || store.canonical(top.number).await.ok()?.is_some() {
        return None;
    }
    let mut first = top.number;
    while let Some(below) = first.checked_sub(1).filter(|below| *below >= floor) {
        if store.canonical(below).await.ok()?.is_some() {
            break;
        }
        first = below;
    }
    Some(FillRequest { top, first })
}

/// Stores the blocks received on `filled` (each batch a consecutive span, ascending, verified
/// by the fetcher) until the channel closes or `cancel` fires.
///
/// # Errors
///
/// Returns [`PipelineError::Storage`] if the unsafe store fails in a way retrying cannot fix,
/// and [`PipelineError::Task`] if sender recovery panics.
pub(crate) async fn run<U: UnsafeStore>(
    store: U,
    mut filled: mpsc::Receiver<Vec<EncodedBlock>>,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    loop {
        let batch = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            batch = filled.recv() => batch,
        };
        // Closed: the fetcher has stopped.
        let Some(batch) = batch else { return Ok(()) };
        let blocks = match recover_encoded(batch).await {
            Ok(blocks) => blocks,
            Err(RecoverError::Task(err)) => return Err(PipelineError::Task(err)),
            Err(err) => {
                warn!(%err, "blocks fetched for a missed span dropped");
                continue;
            }
        };
        let (Some(lowest), Some(highest)) = (blocks.first(), blocks.last()) else {
            continue;
        };
        let (from, to) = (lowest.block.header.number, highest.block.header.number);
        let mut stored = 0_usize;
        // Highest first: each fills the height below a canonical block.
        for block in blocks.iter().rev() {
            match retry(&cancel, Store::Unsafe, "unsafe insert", || {
                store.insert(block)
            })
            .await
            {
                Ok(outcome) => stored = stored.saturating_add(usize::from(outcome.stored)),
                Err(RetryError::Cancelled) => return Ok(()),
                Err(RetryError::Storage(
                    err @ (StorageError::InvalidBlock { .. }
                    | StorageError::UnsupportedTransaction { .. }),
                )) => {
                    warn!(number = block.block.header.number, %err, "fetched block dropped");
                    break;
                }
                Err(RetryError::Storage(source)) => {
                    return Err(PipelineError::Storage {
                        operation: INSERT,
                        source,
                    });
                }
            }
        }
        info!(
            from,
            to, stored, "missed unsafe blocks fetched from execution peers"
        );
    }
}
