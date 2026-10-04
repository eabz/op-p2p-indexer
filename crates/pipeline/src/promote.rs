//! Promotion: moves blocks from the unsafe store to the committed store and the archive when
//! the L1 safe head passes them, and the reconciliation that runs before it at startup.
//!
//! Does not decide what is safe (the L1 heads arrive on a channel), does not fill holes
//! (backfill belongs to the `el` crate), and does not retry by itself: every store call goes
//! through [`retry`].
//!
//! # One promotion
//!
//! `C` is the safe head recorded in the committed store, `S` the new one.
//!
//! 1. If `S` is below `C`, or at its height with another hash (an L1 reorg): roll the
//!    committed store back to `S` and truncate the archive above it.
//! 2. Record the heads in the unsafe store.
//! 3. Read the blocks above `C` up to `S` from the unsafe store.
//! 4. Insert them into the committed store, append them to the archive, trim the archive.
//! 5. Record the heads in the committed store: the marker that the range is committed.
//! 6. Prune the unsafe store up to `S` and publish `S`'s number.
//!
//! Before the first safe head is recorded there is no `C`: block `S` alone is promoted, and the
//! committed store begins there.
//!
//! # Crash safety
//!
//! The order is data, then the marker, then pruning, so a crash never loses a block: what the
//! marker covers is stored, and what is not yet covered is still in the unsafe store. Every
//! step is idempotent. After a crash, [`Promoter::reconcile`] runs and the next change of the
//! heads repeats the promotion from `C`:
//!
//! | Crash after | What is left | Why repeating is safe |
//! |---|---|---|
//! | 1, the rollback | The committed store is at `S`; the archive may still hold blocks above it. | The rollback records `S` before it deletes, so `C` is `S`; startup truncates the archive to `C`. |
//! | 2 | The unsafe store knows `S`; nothing is committed. | Startup writes `C` back; the range is still stored. |
//! | 4, part or all of it | Rows or archived blocks above `C`; the marker still says `C`. | Startup deletes the committed rows above `C` and truncates the archive to `C`, so nothing of the stopped attempt survives, even if the next safe chain is another one. |
//! | 5 | The range is committed; the unsafe store still holds it. | Startup prunes up to `C` and publishes it. |
//!
//! # Limits, for the L1 and backfill work
//!
//! - **Holes.** When the whole range cannot be read, the readable part next to `S` is promoted
//!   and the rest, next to `C`, is left out: below a block missing from the unsafe store, or
//!   beyond what one ancestry call returns. Everything from `S` down to the break is on `S`'s
//!   chain, so it is safe. The hole is logged and counted with the blocks left out, `S` is
//!   recorded, and promotion continues from there; backfill finds the hole by block number.
//!   If `S` itself is missing, nothing is promoted. The archive starts again at the first
//!   promoted block, because its range must be contiguous.
//! - **A range that does not build on `C`.** It is `S`'s chain, so it is promoted; the
//!   committed block at `C`'s height then belongs to another chain, as in the next point.
//! - **A reorg that changes the block at `S`'s height.** The rollback deletes above `S`'s
//!   height, so the committed row at that height is still the old chain's block. The pipeline
//!   cannot rewrite it: the unsafe store was pruned up to the old `C`.

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, DecodedBlock, L1Heads};
use op_indexer_storage::{
    ArchiveRetention, ArchiveStore, CommittedStore, StorageError, Store, UnsafeStore,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::PipelineError;
use crate::metrics::{self, HoleReason};
use crate::retry::{RetryError, retry};

/// Most ancestry calls one promotion makes: one for the whole range and up to three narrower
/// ones (as much as one call returns, then above each missing block found). Past that nothing
/// of the range is promoted and all of it is the hole.
const MAX_RANGE_READS: usize = 4;

/// The promotion task and its startup reconciliation.
#[derive(Debug)]
pub(crate) struct Promoter<U, C, A> {
    unsafe_store: U,
    committed_store: C,
    /// The archive and how much it keeps; `None` when it is disabled.
    archive: Option<(A, ArchiveRetention)>,
    l1_heads: watch::Receiver<L1Heads>,
    safe_number: watch::Sender<BlockNumber>,
    /// The heads recorded in the committed store, as of the last write this task made.
    committed: L1Heads,
}

/// What could be read of the range above the committed safe head.
#[derive(Debug)]
struct RangeRead {
    /// The blocks read, oldest first, ending at the safe head; empty if none could be read.
    blocks: Vec<DecodedBlock>,
    /// The height the blocks start above; blocks up to it that are not committed are a hole.
    above: BlockNumber,
    /// Why the read does not start above the committed safe head, if it does not.
    hole: Option<HoleReason>,
}

/// Why a promotion stopped before its end.
#[derive(Debug)]
enum Stop {
    /// The pipeline was cancelled while a store call waited to be retried.
    Cancelled,
    /// A store failed in a way retrying cannot fix.
    Fatal(PipelineError),
}

impl<U, C, A> Promoter<U, C, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    C: CommittedStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
{
    /// Creates the task. Makes no store call: [`Self::reconcile`] must run before
    /// [`Self::run`].
    pub(crate) fn new(
        unsafe_store: U,
        committed_store: C,
        archive: Option<(A, ArchiveRetention)>,
        l1_heads: watch::Receiver<L1Heads>,
        safe_number: watch::Sender<BlockNumber>,
    ) -> Self {
        Self {
            unsafe_store,
            committed_store,
            archive,
            l1_heads,
            safe_number,
            committed: L1Heads::default(),
        }
    }

    /// Brings the stores in line with the committed store's heads, which are the truth after a
    /// restart: deletes committed rows above the safe head (left by a promotion that stopped
    /// before its marker), writes the heads to the unsafe store (it may have been wiped), prunes
    /// it up to the safe head, publishes the safe number, and truncates an archive that is ahead
    /// of it.
    ///
    /// An archive that is behind is left alone: the first block that does not extend it empties
    /// it (see [`Self::run`]). Returns `Ok(())` if cancelled part-way.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Storage`] if a store fails in a way retrying cannot fix.
    pub(crate) async fn reconcile(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<(), PipelineError> {
        finish(self.reconcile_stores(cancel).await)
    }

    /// Promotes on every change of the L1 heads until `cancel` fires or the heads' sender is
    /// dropped. A promotion in progress is finished first, except that a store call waiting to
    /// be retried gives up when cancelled; the next start repeats it.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Storage`] if a store fails in a way retrying cannot fix.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), PipelineError> {
        loop {
            // The value present at start counts too: it may have been sent before this ran.
            let heads = *self.l1_heads.borrow_and_update();
            // A promotion stopped by the cancellation ends the loop at the select below.
            finish(self.promote(heads, &cancel).await)?;
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                changed = self.l1_heads.changed() => {
                    if changed.is_err() {
                        debug!("L1 heads sender dropped; promotion stops");
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn reconcile_stores(&mut self, cancel: &CancellationToken) -> Result<(), Stop> {
        self.committed = call(cancel, Store::Committed, "committed l1_heads", || {
            self.committed_store.l1_heads()
        })
        .await?;
        let heads = self.committed;
        info!(safe = ?heads.safe, finalized = ?heads.finalized, "committed heads at startup");
        if heads == L1Heads::default() {
            return Ok(());
        }
        if let Some(safe) = heads.safe {
            // Rows above `C` can only come from a promotion that stopped before its marker.
            // Usually there are none and the deletes match nothing.
            call(cancel, Store::Committed, "committed rollback_to", || {
                self.committed_store.rollback_to(safe)
            })
            .await?;
        }
        self.set_unsafe_heads(heads, cancel).await?;
        let Some(safe) = heads.safe else {
            return Ok(());
        };
        // Repeats step 6, in case the last run stopped between the marker and the prune.
        self.prune_and_publish(safe, cancel).await?;

        let Some((archive, _)) = &self.archive else {
            return Ok(());
        };
        let range = call(cancel, Store::Archive, "archive range", || archive.range()).await?;
        if let Some((_, tip)) = range
            && tip.number > safe.number
        {
            // Left by a promotion that stopped before its marker; that range is repeated.
            info!(
                tip = tip.number,
                safe = safe.number,
                "archive is ahead of the committed safe head; truncating"
            );
            Self::truncate_archive(archive, safe.number, cancel).await?;
        }
        Ok(())
    }

    /// One promotion to `heads`; a no-op when they change nothing.
    async fn promote(&mut self, heads: L1Heads, cancel: &CancellationToken) -> Result<(), Stop> {
        if self.merged(heads) == self.committed {
            return Ok(());
        }
        let committed_safe = self.committed.safe;
        let safe = match heads.safe {
            Some(safe) if Some(safe) != committed_safe => safe,
            // Only the finalized head changed.
            Some(_) | None => {
                self.set_unsafe_heads(heads, cancel).await?;
                return self.set_committed_heads(heads, cancel).await;
            }
        };

        // Not above `C` and not `C`: the safe head moved back, or changed hash at its height.
        let reorged = committed_safe.is_some_and(|committed| safe.number <= committed.number);
        if reorged {
            self.roll_back(safe, cancel).await?;
        }
        self.set_unsafe_heads(heads, cancel).await?;
        if !reorged {
            self.commit_range(committed_safe, safe, cancel).await?;
        }
        self.set_committed_heads(heads, cancel).await?;
        self.prune_and_publish(safe, cancel).await
    }

    /// What the committed store holds once `heads` are recorded: a `None` head means "unknown"
    /// and leaves the recorded one in place.
    fn merged(&self, heads: L1Heads) -> L1Heads {
        L1Heads {
            safe: heads.safe.or(self.committed.safe),
            finalized: heads.finalized.or(self.committed.finalized),
        }
    }

    /// Step 1: rolls the committed store back to `safe` and truncates the archive above it.
    async fn roll_back(&mut self, safe: BlockRef, cancel: &CancellationToken) -> Result<(), Stop> {
        warn!(
            committed = ?self.committed.safe,
            safe = ?safe,
            "L1 reorg: the safe head moved back; rolling the committed store back"
        );
        call(cancel, Store::Committed, "committed rollback_to", || {
            self.committed_store.rollback_to(safe)
        })
        .await?;
        // The rollback recorded `safe` as the committed safe head.
        self.committed.safe = Some(safe);
        metrics::l1_reorg();
        if let Some((archive, _)) = &self.archive {
            Self::truncate_archive(archive, safe.number, cancel).await?;
        }
        Ok(())
    }

    /// Steps 3 and 4: reads the blocks above `committed` up to `safe` and writes them to the
    /// committed store and the archive. When only the part next to `safe` can be read, that
    /// part is written and the rest is a hole.
    async fn commit_range(
        &self,
        committed: Option<BlockRef>,
        safe: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        // Without a committed safe head the committed store begins at `safe`.
        let floor = committed.map_or_else(|| safe.number.saturating_sub(1), |c| c.number);
        let RangeRead {
            blocks,
            above: stop_at,
            hole,
        } = self.read_range(floor, safe, cancel).await?;
        let builds_on_committed = committed
            .zip(blocks.first())
            .is_none_or(|(committed, first)| first.block.header.parent_hash == committed.hash);
        let hole = hole.or((!builds_on_committed).then_some(HoleReason::ParentMismatch));
        match (hole, committed) {
            (Some(reason), Some(committed)) => report_hole(reason, committed, stop_at, safe),
            (Some(_), None) => info!(
                safe = ?safe,
                "the first safe head is not in the unsafe store; the committed store begins after it"
            ),
            (None, _) => {}
        }
        if blocks.is_empty() {
            return Ok(());
        }

        call(cancel, Store::Committed, "committed insert", || {
            self.committed_store.insert(&blocks)
        })
        .await?;
        // After a hole the first block does not extend the archive, which starts it again.
        self.archive_range(&blocks, cancel).await?;
        metrics::blocks_promoted(blocks.len());
        info!(
            from = stop_at.saturating_add(1),
            to = safe.number,
            blocks = blocks.len(),
            "promoted blocks to the committed store"
        );
        Ok(())
    }

    /// Reads the blocks above `floor` up to `safe`. If that range cannot be read, reads the part
    /// of it next to `safe` instead: above a block missing from the unsafe store, or as much as
    /// one call returns. Makes at most [`MAX_RANGE_READS`] calls; if none succeeds, or `safe`
    /// itself is missing, the result is empty.
    ///
    /// With more than one reason for a narrower read, the last one is reported.
    async fn read_range(
        &self,
        floor: BlockNumber,
        safe: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<RangeRead, Stop> {
        const ANCESTRY: &str = "unsafe ancestry";
        let mut above = floor;
        let mut hole = None;
        for _ in 0..MAX_RANGE_READS {
            let ancestry = retry(cancel, Store::Unsafe, ANCESTRY, || {
                self.unsafe_store.ancestry(safe, above)
            })
            .await;
            // Each narrower read must start higher, or the loop would repeat the same call.
            let narrowed = match ancestry {
                Ok(blocks) => {
                    return Ok(RangeRead {
                        blocks,
                        above,
                        hole,
                    });
                }
                Err(RetryError::Storage(StorageError::MissingAncestor { hash, number })) => {
                    debug!(%hash, number, "a block of the range is not in the unsafe store");
                    hole = Some(HoleReason::MissingAncestor);
                    number
                }
                Err(RetryError::Storage(StorageError::AncestryTooLong { max, .. })) => {
                    hole = Some(HoleReason::TooLong);
                    safe.number.saturating_sub(max)
                }
                Err(other) => return Err(stop(ANCESTRY)(other)),
            };
            if narrowed <= above || narrowed >= safe.number {
                break;
            }
            above = narrowed;
        }
        Ok(RangeRead {
            blocks: Vec::new(),
            above: safe.number,
            hole,
        })
    }

    /// Appends `blocks` to the archive and trims it to its retention. A block that does not
    /// extend the archive (it is behind, or holds another chain) empties it and starts it again
    /// at that block.
    async fn archive_range(
        &self,
        blocks: &[DecodedBlock],
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        const APPEND: &str = "archive append";
        let Some((archive, retention)) = &self.archive else {
            return Ok(());
        };
        for block in blocks {
            let append = || retry(cancel, Store::Archive, APPEND, || archive.append(block));
            match append().await {
                Ok(()) => {}
                Err(RetryError::Storage(StorageError::NotContiguous { expected, got })) => {
                    warn!(
                        tip = ?expected,
                        parent = ?got,
                        "a promoted block does not extend the archive; restarting the archive at it"
                    );
                    Self::restart_archive(archive, cancel).await?;
                    append().await.map_err(stop(APPEND))?;
                }
                Err(other) => return Err(stop(APPEND)(other)),
            }
        }
        match *retention {
            ArchiveRetention::Blocks(retain) => {
                call(cancel, Store::Archive, "archive trim", || {
                    archive.trim(retain)
                })
                .await?;
            }
            ArchiveRetention::All => {}
        }
        Ok(())
    }

    /// Empties the archive. `trim(0)` stops at its deadline with a transient timeout when there
    /// is more to remove, so retrying it finishes the job.
    async fn restart_archive(archive: &A, cancel: &CancellationToken) -> Result<(), Stop> {
        call(cancel, Store::Archive, "archive trim", || archive.trim(0)).await?;
        metrics::archive_restart();
        Ok(())
    }

    /// Removes the archive's blocks above `number`; retried like [`Self::restart_archive`].
    async fn truncate_archive(
        archive: &A,
        number: BlockNumber,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        call(cancel, Store::Archive, "archive truncate_above", || {
            archive.truncate_above(number)
        })
        .await
    }

    /// Step 2: records `heads` in the unsafe store.
    async fn set_unsafe_heads(
        &self,
        heads: L1Heads,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        call(cancel, Store::Unsafe, "unsafe set_l1_heads", || {
            self.unsafe_store.set_l1_heads(heads)
        })
        .await
    }

    /// Step 5: records `heads` in the committed store.
    async fn set_committed_heads(
        &mut self,
        heads: L1Heads,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        call(cancel, Store::Committed, "committed set_l1_heads", || {
            self.committed_store.set_l1_heads(heads)
        })
        .await?;
        self.committed = self.merged(heads);
        Ok(())
    }

    /// Step 6: prunes the unsafe store up to `safe` and publishes its number.
    async fn prune_and_publish(
        &self,
        safe: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        call(cancel, Store::Unsafe, "unsafe prune", || {
            self.unsafe_store.prune(safe)
        })
        .await?;
        // No receiver is not an error: the value is kept for one that subscribes later.
        self.safe_number.send_replace(safe.number);
        metrics::safe_block_number(safe.number);
        Ok(())
    }
}

/// Promote now, backfill later: logs and counts the blocks above `committed` up to
/// `left_out_to` that are not promoted. With a parent mismatch none are left out, but the
/// committed block at `committed`'s height is another chain's.
fn report_hole(reason: HoleReason, committed: BlockRef, left_out_to: BlockNumber, safe: BlockRef) {
    let missing = left_out_to.saturating_sub(committed.number);
    metrics::promotion_hole(reason, missing);
    let why = match reason {
        HoleReason::MissingAncestor => "a block of the range is not in the unsafe store",
        HoleReason::TooLong => {
            "the range is longer than one read returns; the blocks left out may be in the \
             unsafe store but are not promoted"
        }
        HoleReason::ParentMismatch => {
            warn!(
                committed = ?committed,
                safe = ?safe,
                "the promoted range does not build on the committed safe head: the committed \
                 block at that height is another chain's, left for backfill to repair"
            );
            return;
        }
    };
    warn!(
        from = committed.number.saturating_add(1),
        to = left_out_to,
        missing,
        committed = ?committed,
        safe = ?safe,
        why,
        "promotion hole: blocks are left for backfill"
    );
}

/// Runs a store call through [`retry`]. `operation` names it in the retry log and in the error.
async fn call<T, F, Fut>(
    cancel: &CancellationToken,
    store: Store,
    operation: &'static str,
    f: F,
) -> Result<T, Stop>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    retry(cancel, store, operation, f)
        .await
        .map_err(stop(operation))
}

/// Maps the end of a retried call to why the promotion stops; `operation` names the call in
/// the error.
fn stop(operation: &'static str) -> impl FnOnce(RetryError) -> Stop {
    move |err| match err {
        RetryError::Cancelled => Stop::Cancelled,
        RetryError::Storage(source) => Stop::Fatal(PipelineError::Storage { operation, source }),
    }
}

/// A cancelled step is a clean stop; only a fatal error is returned.
fn finish(result: Result<(), Stop>) -> Result<(), PipelineError> {
    match result {
        Ok(()) | Err(Stop::Cancelled) => Ok(()),
        Err(Stop::Fatal(err)) => Err(err),
    }
}
