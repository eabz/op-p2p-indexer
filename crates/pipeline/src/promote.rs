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
//! 1. If `S` is at `C`'s height with another hash, or below `C` with the archive holding
//!    another block at `S`'s height (an L1 reorg): roll the committed store back to `S`, and
//!    truncate the archive above it if the archive ends at or below `C`. A block of the
//!    committed chain below `C`, or one the archive cannot tell about, is nothing to do: a
//!    rollback deletes committed blocks and is only done on evidence.
//! 2. Record the heads in the unsafe store.
//! 3. Read the blocks above `C` up to `S` from the unsafe store.
//! 4. Insert them into the committed store; append them to the archive if they extend it, and
//!    trim it to its window.
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
//! | 1, the rollback | The committed store is at `S`; the archive may still hold blocks above it. | The rollback records `S` before it deletes, so `C` is `S`. The archive's blocks above `S` are of the old chain: the next promoted range does not extend them and is not archived (below). |
//! | 2 | The unsafe store knows `S`; nothing is committed. | Startup writes `C` back; the range is still stored. |
//! | 4, part or all of it | Rows or archived blocks above `C`; the marker still says `C`. | The repeat inserts the same rows again and finds the same blocks in the archive, which skips them. Only if the safe chain differs after the restart do rows and archived blocks of the stopped attempt stay: see the limits. |
//! | 5 | The range is committed; the unsafe store still holds it. | Startup prunes up to `C` and publishes it. |
//!
//! # The archive is never emptied here
//!
//! The archive holds one contiguous range and other writers fill it too: the importer and
//! range sync, below and above `C`. Promotion therefore only ever **adds at the tip**:
//!
//! - A promoted range is appended only if it extends the archive's tip (blocks already held
//!   are skipped). Otherwise it is not archived: the archive is behind (a gap that range sync
//!   fills) or holds another chain at that height. This is logged, at most once per
//!   [`ARCHIVE_WARN_INTERVAL`], and the blocks are counted.
//! - Startup removes nothing from the archive or the committed store: blocks above `C` may
//!   be an import or a sync that reached further, which cannot be told from a stopped
//!   promotion.
//! - Trimming to the retention window happens only while the archive is no larger than the
//!   window plus what was just appended, so it removes at most as many blocks as were
//!   appended. An archive that already holds more than the window (an import) is not trimmed.
//! - Blocks are removed only by an L1 reorg of the safe head (step 1), and only when the
//!   archive ends at or below `C`: at most `C - S` blocks, the depth of the reorg. An archive
//!   that reaches above `C` was not written by promotion alone and is left as it is.
//!
//! # Limits, for the L1 and backfill work
//!
//! - **Holes.** When the whole range cannot be read, the readable part next to `S` is promoted
//!   and the rest, next to `C`, is left out: below a block missing from the unsafe store, or
//!   beyond what one ancestry call returns. Everything from `S` down to the break is on `S`'s
//!   chain, so it is safe. The hole is logged and counted with the blocks left out, `S` is
//!   recorded, and promotion continues from there; backfill finds the hole by block number.
//!   If `S` itself is missing, nothing is promoted. The blocks promoted after a hole do not
//!   extend the archive and are not archived until range sync has filled the hole.
//! - **A promotion stopped before its marker, followed by another safe chain.** Rows above
//!   `C` of the stopped attempt stay in the committed store (the insert replaces a row only by
//!   the same block), and its blocks stay at the archive's tip, so later ranges are not
//!   archived. Both need a crash between steps 4 and 5 and an L1 reorg of the safe head
//!   before the restart; backfill repairs the rows, and the operator the archive.
//! - **A range that does not build on `C`.** It is `S`'s chain, so it is promoted; the
//!   committed block at `C`'s height then belongs to another chain, as in the next point.
//! - **A reorg that changes the block at `S`'s height.** The rollback deletes above `S`'s
//!   height, so the committed row at that height is still the old chain's block. The pipeline
//!   cannot rewrite it: the unsafe store was pruned up to the old `C`.

use std::time::{Duration, Instant};

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, DecodedBlock, EncodedBlock, L1Heads};
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

/// Shortest time between two warnings that promoted blocks are not archived, or that the
/// archive is not trimmed. Either state lasts until something else changes the archive.
const ARCHIVE_WARN_INTERVAL: Duration = Duration::from_mins(10);

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
    /// When an archive warning was last logged.
    archive_warned: Option<Instant>,
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
            archive_warned: None,
        }
    }

    /// Brings the unsafe store in line with the committed store's heads, which are the truth
    /// after a restart: writes the heads to it (it may have been wiped), prunes it up to the
    /// safe head and publishes the safe number. Removes nothing from the committed store or
    /// the archive (see the module documentation). Returns `Ok(())` if cancelled part-way.
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

    /// The heads the committed store recorded, as read by [`Self::reconcile`]: where the heads
    /// promotion acts on start, which nothing may publish below.
    pub(crate) const fn committed_heads(&self) -> L1Heads {
        self.committed
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
        self.set_unsafe_heads(heads, cancel).await?;
        let Some(safe) = heads.safe else {
            return Ok(());
        };
        // Repeats step 6, in case the last run stopped between the marker and the prune.
        self.prune_and_publish(safe, cancel).await
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

        // Not above `C` and not `C`. Only a different block at that height is an L1 reorg; a
        // block of the committed chain below `C` is a head that is behind, and nothing to do.
        // The commitment task never publishes such a head; this guards the committed store
        // against any other source of heads.
        let behind = committed_safe.filter(|committed| safe.number <= committed.number);
        let reorged = behind.is_some();
        if let Some(committed) = behind {
            if !self.replaced(safe, committed, cancel).await? {
                debug!(
                    ?safe,
                    ?committed,
                    "a safe head behind the committed one; nothing to do"
                );
                return Ok(());
            }
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

    /// Whether `safe`, at or below the committed safe head `committed`, is a block of another
    /// chain than the committed one: at the committed head's height with another hash, or below it with
    /// the archive holding another block at that height. When the archive cannot tell (it
    /// does not reach that height, or there is none), the answer is no: a rollback deletes
    /// committed blocks, and is only done on evidence.
    async fn replaced(
        &self,
        safe: BlockRef,
        committed: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<bool, Stop> {
        if safe.number == committed.number {
            return Ok(safe.hash != committed.hash);
        }
        let Some((archive, _)) = &self.archive else {
            warn!(
                ?safe,
                ?committed,
                "a safe head below the committed one cannot be checked without the archive; not rolling back"
            );
            return Ok(false);
        };
        let held = call(cancel, Store::Archive, "archive number_of", || {
            archive.number_of(safe.hash)
        })
        .await?;
        if held == Some(safe.number) {
            return Ok(false);
        }
        let range = call(cancel, Store::Archive, "archive range", || archive.range()).await?;
        let covers =
            range.is_some_and(|(first, tip)| (first.number..=tip.number).contains(&safe.number));
        if !covers {
            warn!(
                ?safe,
                ?committed,
                "a safe head below the committed one is outside the archive; not rolling back"
            );
        }
        Ok(covers)
    }

    /// Step 1: rolls the committed store back to `safe`, and truncates the archive above it if
    /// the archive ends at or below the committed safe head, which bounds the removal by the
    /// depth of the reorg.
    async fn roll_back(&mut self, safe: BlockRef, cancel: &CancellationToken) -> Result<(), Stop> {
        let committed = self.committed.safe;
        warn!(
            ?committed,
            ?safe,
            "L1 reorg: the safe head moved back; rolling the committed store back"
        );
        call(cancel, Store::Committed, "committed rollback_to", || {
            self.committed_store.rollback_to(safe)
        })
        .await?;
        // The rollback recorded `safe` as the committed safe head.
        self.committed.safe = Some(safe);

        let Some((archive, _)) = &self.archive else {
            return Ok(());
        };
        let range = call(cancel, Store::Archive, "archive range", || archive.range()).await?;
        let Some((_, tip)) = range.filter(|(_, tip)| tip.number > safe.number) else {
            return Ok(());
        };
        if committed.is_some_and(|committed| tip.number <= committed.number) {
            return call(cancel, Store::Archive, "archive truncate_above", || {
                archive.truncate_above(safe.number)
            })
            .await;
        }
        // Blocks above the committed safe head were not all written by promotion.
        warn!(
            ?tip,
            ?committed,
            ?safe,
            "the archive reaches above the committed safe head; not truncating it after the reorg"
        );
        Ok(())
    }

    /// Steps 3 and 4: reads the blocks above `committed`, or above the archive's last block
    /// when that is higher and below `safe`, up to `safe` and writes them to the committed
    /// store and the archive. When only the part next to `safe` can be read, that part is
    /// written and the rest is a hole.
    async fn commit_range(
        &mut self,
        committed: Option<BlockRef>,
        safe: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        // The block the range is read above: the archive's last block when it is above the
        // committed safe head and below `safe`, so the archive is extended from where it ends
        // and blocks it holds are not read again; else the committed safe head. Without
        // either the committed store begins at `safe`.
        let tip = match &self.archive {
            Some((archive, _)) => {
                let range = call(cancel, Store::Archive, "archive range", || archive.range());
                range.await?.map(|(_, tip)| tip)
            }
            None => None,
        };
        let base = match (committed, tip) {
            (committed, Some(tip))
                if tip.number < safe.number
                    && committed.is_none_or(|committed| tip.number > committed.number) =>
            {
                Some(tip)
            }
            (committed, _) => committed,
        };
        let floor = base.map_or_else(|| safe.number.saturating_sub(1), |base| base.number);
        let RangeRead {
            blocks,
            above: stop_at,
            hole,
        } = self.read_range(floor, safe, cancel).await?;
        let builds_on_base = base
            .zip(blocks.first())
            .is_none_or(|(base, first)| first.block.header.parent_hash == base.hash);
        let hole = hole.or((!builds_on_base).then_some(HoleReason::ParentMismatch));
        match (hole, base) {
            (Some(reason), Some(base)) => report_hole(reason, base, stop_at, safe),
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
        self.archive_range(&blocks, cancel).await?;
        let without_receipts = blocks
            .iter()
            .filter(|block| block.receipts.is_none())
            .count();
        metrics::blocks_promoted(blocks.len(), without_receipts);
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

    /// Appends `blocks` to the archive if they extend its tip, and trims it to its retention
    /// window. Blocks that do not extend it are not archived; nothing is removed to make room
    /// for them.
    async fn archive_range(
        &mut self,
        blocks: &[DecodedBlock],
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        const APPEND: &str = "archive append_batch";
        let Some((archive, retention)) = self.archive.clone() else {
            return Ok(());
        };
        // The bytes archived for a promoted block are encoded here, from the gossip block.
        let encoded: Vec<EncodedBlock> = blocks.iter().map(EncodedBlock::from).collect();
        let appended = retry(cancel, Store::Archive, APPEND, || {
            archive.append_batch(encoded.clone())
        });
        match appended.await {
            Ok(()) => {}
            Err(RetryError::Storage(StorageError::NotContiguous { expected, got })) => {
                metrics::archive_skipped(blocks.len());
                if self.archive_warning_due() {
                    warn!(
                        archive_tip = ?expected,
                        block = ?got,
                        blocks = blocks.len(),
                        "promoted blocks do not extend the archive and are not archived"
                    );
                }
                return Ok(());
            }
            Err(other) => return Err(stop(APPEND)(other)),
        }

        let ArchiveRetention::Blocks(retain) = retention else {
            return Ok(());
        };
        let range = call(cancel, Store::Archive, "archive range", || archive.range()).await?;
        let held = range.map_or(0, |(first, last)| {
            last.number.saturating_sub(first.number).saturating_add(1)
        });
        let appended = u64::try_from(blocks.len()).unwrap_or(u64::MAX);
        // More than the window before this append: not an archive promotion filled alone.
        if held > retain.saturating_add(appended) {
            if self.archive_warning_due() {
                warn!(
                    held,
                    retain,
                    "the archive holds more blocks than its retention window; not trimming it"
                );
            }
            return Ok(());
        }
        call(cancel, Store::Archive, "archive trim", || {
            archive.trim(retain)
        })
        .await
        .map(|_removed| ())
    }

    /// Whether an archive warning may be logged now; at most one per
    /// [`ARCHIVE_WARN_INTERVAL`].
    fn archive_warning_due(&mut self) -> bool {
        let now = Instant::now();
        let due = self
            .archive_warned
            .is_none_or(|at| now.duration_since(at) >= ARCHIVE_WARN_INTERVAL);
        if due {
            self.archive_warned = Some(now);
        }
        due
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

/// Promote now, backfill later: logs and counts the blocks above `base` (the block the range
/// was read above) up to `left_out_to` that are not promoted. With a parent mismatch none are
/// left out, but the block at `base`'s height is another chain's.
fn report_hole(reason: HoleReason, base: BlockRef, left_out_to: BlockNumber, safe: BlockRef) {
    let missing = left_out_to.saturating_sub(base.number);
    metrics::promotion_hole(reason, missing);
    let why = match reason {
        HoleReason::MissingAncestor => "a block of the range is not in the unsafe store",
        HoleReason::TooLong => {
            "the range is longer than one read returns; the blocks left out may be in the \
             unsafe store but are not promoted"
        }
        HoleReason::ParentMismatch => {
            warn!(
                above = ?base,
                safe = ?safe,
                "the promoted range does not build on the block it was read above (the \
                 committed safe head, or the archive's last block): that block is another \
                 chain's, left for backfill to repair"
            );
            return;
        }
    };
    warn!(
        from = base.number.saturating_add(1),
        to = left_out_to,
        missing,
        above = ?base,
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
