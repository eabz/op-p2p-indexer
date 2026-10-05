//! Promotion: moves blocks from the unsafe store to the archive (the committed store) when the
//! L1 safe head passes them, and the reconciliation that runs before it at startup.
//!
//! Does not decide what is safe (the L1 heads arrive on a channel), does not fill holes
//! (backfill belongs to the `el` crate), and does not retry by itself: every store call goes
//! through [`retry`].
//!
//! # One promotion
//!
//! `C` is the safe head recorded in the archive, `S` the new one.
//!
//! 1. If `S` is at or below `C`, nothing is done: the heads only rise (the commitment task
//!    never publishes a lower safe head), so there is no rollback.
//! 2. Record the heads in the unsafe store.
//! 3. Read the blocks above `C` up to `S` from the unsafe store, walking down from `S` one
//!    ancestry call (1,024 blocks) at a time.
//! 4. Part by part, oldest first: append them to the archive if they extend it, each block's
//!    transactions and receipts roots checked first over exactly the bytes written; the part
//!    ends before a block that does not match, which is logged as an error.
//! 5. Record the heads in the archive: the marker that the range is committed. The heads never
//!    name a block the archive lacks: the safe head recorded is the newest block of `S`'s chain
//!    the archive holds (`S` when the whole range went in), and the finalized head only if it
//!    is not above it. When nothing was appended but the archive holds `S` (range sync stored
//!    it), `S` is recorded. When the archive holds nothing of `S`'s chain above `C`, only the
//!    finalized head may change; range sync, required with the L1 side, fills the gap.
//! 6. Prune the unsafe store up to the recorded safe head and publish its number.
//!
//! Before the first safe head is recorded there is no `C`: block `S` alone is promoted.
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
//! | 2 | The unsafe store knows `S`; nothing is committed. | Startup writes `C` back; the range is still stored. |
//! | 4, part or all of it | Archived blocks above `C`; the marker still says `C`. | The repeat finds the same blocks in the archive, which skips them. Only if the safe chain differs after the restart do archived blocks of the stopped attempt stay: see the limits. |
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
//!   [`ARCHIVE_WARN_INTERVAL`], with the number of blocks.
//! - Startup removes nothing from the archive: blocks above `C` may be an import or a sync
//!   that reached further, which cannot be told from a stopped promotion.
//! - Nothing removes blocks: the archive keeps every block, and the heads only rise.
//!
//! # Limits, for the L1 and backfill work
//!
//! - **Holes.** When the whole range cannot be read, the readable part next to `S` is promoted
//!   and the rest, next to `C`, is left out: below a block missing from the unsafe store, or
//!   beyond [`MAX_PROMOTED_PARTS`]. Everything from `S` down to the break is on `S`'s
//!   chain, so it is safe. The hole is logged, with the number of blocks left out. The blocks
//!   after a hole do not extend the archive, so they are not archived and the recorded safe
//!   head stays below the hole (step 5); the unsafe store keeps them until range sync has
//!   filled the hole and a later promotion appends them, or until they expire. If `S` itself
//!   is missing, nothing is promoted.
//! - **A promotion stopped before its marker, followed by another safe chain.** The blocks
//!   of the stopped attempt stay at the archive's tip, so later ranges are not archived. It
//!   needs a crash between steps 4 and 5 and another safe chain before the restart; the
//!   operator repairs the archive.
//! - **A range that does not build on `C`.** It is `S`'s chain, so it is promoted if it
//!   extends the archive; the archived block at `C`'s height then belongs to another chain,
//!   which the pipeline cannot rewrite (the unsafe store was pruned up to `C`): the operator
//!   repairs the archive.

use std::time::{Duration, Instant};

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{
    ArchivedBlock, BlockRef, DecodedBlock, L1Heads, receipts_root, split_body, transactions_root,
};
use op_indexer_storage::{ArchiveStore, StorageError, Store, UnsafeStore};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::PipelineError;
use crate::retry::{RetryError, retry};

/// Most ancestry calls one part of the range takes: one for all of what is left and up to three
/// narrower ones (as much as one call returns, then above each missing block found). Past that
/// nothing more of the range is read and the rest of it is the hole.
const MAX_RANGE_READS: usize = 4;

/// Most parts one promotion reads, which bounds its work. A part is what one ancestry call
/// returns (1,024 blocks), so the cap is a count of blocks and its span depends on the block
/// time: about 9 hours on OP Mainnet and 4.5 hours on Unichain, either several times the
/// interval between the dispute games that move the safe head. Past it the oldest part of the
/// range is a hole.
const MAX_PROMOTED_PARTS: usize = 16;

/// Shortest time between two warnings that promoted blocks are not archived. The state lasts
/// until something else changes the archive.
const ARCHIVE_WARN_INTERVAL: Duration = Duration::from_mins(10);

/// The promotion task and its startup reconciliation.
#[derive(Debug)]
pub(crate) struct Promoter<U, A> {
    unsafe_store: U,
    /// The archive, the committed store.
    archive: A,
    /// The chain's Canyon time, for receipts roots.
    canyon_time: u64,
    l1_heads: watch::Receiver<L1Heads>,
    safe_number: watch::Sender<BlockNumber>,
    /// The heads recorded in the archive, as of the last write this task made.
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

impl<U, A> Promoter<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
{
    /// Creates the task. Makes no store call: [`Self::reconcile`] must run before
    /// [`Self::run`].
    pub(crate) fn new(
        unsafe_store: U,
        archive: A,
        canyon_time: u64,
        l1_heads: watch::Receiver<L1Heads>,
        safe_number: watch::Sender<BlockNumber>,
    ) -> Self {
        Self {
            unsafe_store,
            archive,
            canyon_time,
            l1_heads,
            safe_number,
            committed: L1Heads::default(),
            archive_warned: None,
        }
    }

    /// Brings the unsafe store in line with the archive's heads, which are the truth after a
    /// restart: writes the heads to it (it may have been wiped), prunes it up to the safe head
    /// and publishes the safe number. Removes nothing from the archive (see the module
    /// documentation). Returns `Ok(())` if cancelled part-way.
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

    /// The heads the archive recorded, as read by [`Self::reconcile`]: where the heads
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
        self.committed = call(cancel, Store::Archive, "archive heads", || {
            self.archive.heads()
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

        // Heads only rise: the commitment task never publishes a safe head at or below the
        // committed one. One from anywhere else is behind, and nothing to do.
        if committed_safe.is_some_and(|committed| safe.number <= committed.number) {
            debug!(
                ?safe,
                committed = ?committed_safe,
                "a safe head at or below the committed one; nothing to do"
            );
            return Ok(());
        }
        self.set_unsafe_heads(heads, cancel).await?;
        // The heads recorded never name a block the archive lacks: the safe head is the newest
        // block of `S`'s chain it appended, or `S` itself when the archive already holds it
        // (range sync stored it, and the unsafe store may not have it).
        let appended = self.commit_range(committed_safe, safe, cancel).await?;
        let held = if appended.is_some() {
            appended
        } else {
            let archive = &self.archive;
            let number = call(cancel, Store::Archive, "archive number_of", || {
                archive.number_of(safe.hash)
            })
            .await?;
            (number == Some(safe.number)).then_some(safe)
        };
        let Some(held) = held else {
            if self.archive_warning_due() {
                warn!(
                    ?safe,
                    committed = ?committed_safe,
                    "the archive holds nothing of the safe chain above the committed safe head; \
                     the committed heads stay until range sync fills the gap"
                );
            }
            return self
                .set_committed_heads(
                    L1Heads {
                        safe: None,
                        ..heads
                    },
                    cancel,
                )
                .await;
        };
        if held != safe {
            info!(
                ?safe,
                recorded = ?held,
                "the archive ends below the safe head; recorded its tip"
            );
        }
        let recorded = L1Heads {
            safe: Some(held),
            finalized: heads.finalized,
        };
        self.set_committed_heads(recorded, cancel).await?;
        self.prune_and_publish(held, cancel).await
    }

    /// What the archive holds once `heads` are recorded: a `None` head means "unknown"
    /// and leaves the recorded one in place.
    fn merged(&self, heads: L1Heads) -> L1Heads {
        L1Heads {
            safe: heads.safe.or(self.committed.safe),
            finalized: heads.finalized.or(self.committed.finalized),
        }
    }

    /// Steps 3 and 4: reads the blocks above `committed`, or above the archive's last block
    /// when that is higher and below `safe`, up to `safe` and appends them to the archive,
    /// oldest part first. When only the part next to `safe` can be read, that part is written
    /// and the rest is a hole. Returns the newest block it appended (all of `safe`'s chain,
    /// above `committed`), which is what may be recorded as committed; `None` if none.
    async fn commit_range(
        &mut self,
        committed: Option<BlockRef>,
        safe: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<Option<BlockRef>, Stop> {
        // The block the range is read above: the archive's last block when it is above the
        // committed safe head and below `safe`, so the archive is extended from where it ends
        // and blocks it holds are not read again; else the committed safe head. Without
        // either the committed chain begins at `safe`.
        let range = call(cancel, Store::Archive, "archive range", || {
            self.archive.range()
        });
        let tip = range.await?.map(|(_, tip)| tip);
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
        let (
            RangeRead {
                blocks,
                above: stop_at,
                hole,
            },
            heads,
        ) = self.walk_range(floor, safe, cancel).await?;
        let builds_on_base = base
            .zip(blocks.first())
            .is_none_or(|(base, first)| first.block.header.parent_hash == base.hash);
        let hole = hole.or((!builds_on_base).then_some(HoleReason::ParentMismatch));
        match (hole, base) {
            (Some(reason), Some(base)) => report_hole(reason, base, stop_at, safe),
            (Some(_), None) => info!(
                safe = ?safe,
                "the first safe head is not in the unsafe store; the committed chain begins after it"
            ),
            (None, _) => {}
        }
        let mut promoted = blocks.len();
        let mut reached = self.write_blocks(&blocks, cancel).await?;
        drop(blocks);
        for pair in heads.windows(2) {
            let [above, head] = *pair else { continue };
            // Read moments ago, so a hole here means the unsafe store changed in between.
            let read = self.read_range(above.number, head, cancel).await?;
            let builds_on_previous = read
                .blocks
                .first()
                .is_some_and(|first| first.block.header.parent_hash == above.hash);
            match read.hole {
                Some(reason) => report_hole(reason, above, read.above, safe),
                // The walk linked the parts by parent hash, so only a store that changed under
                // it gets here: nothing above a broken link is written.
                None if !builds_on_previous => {
                    report_hole(HoleReason::ParentMismatch, above, read.above, safe);
                    break;
                }
                None => {}
            }
            promoted = promoted.saturating_add(read.blocks.len());
            match self.write_blocks(&read.blocks, cancel).await? {
                Some(newest) => reached = Some(newest),
                // The parts above build on this one: they cannot extend the archive either.
                None => break,
            }
        }
        if promoted > 0 {
            info!(
                from = stop_at.saturating_add(1),
                to = safe.number,
                blocks = promoted,
                "promoted blocks to the archive"
            );
        }
        Ok(reached)
    }

    /// Walks the range above `floor` up to `safe` downwards, one [`Self::read_range`] at a
    /// time, until it reaches `floor`, finds a block missing from the unsafe store, or has read
    /// [`MAX_PROMOTED_PARTS`]. Returns the oldest part read, with the reason for the hole below
    /// it if any, and the head of every part, oldest first, ending at `safe`: each part after
    /// the oldest is the blocks above the previous head up to its own, read again when it is
    /// promoted, so only one part's blocks are held at a time.
    async fn walk_range(
        &self,
        floor: BlockNumber,
        safe: BlockRef,
        cancel: &CancellationToken,
    ) -> Result<(RangeRead, Vec<BlockRef>), Stop> {
        let mut heads = vec![safe];
        loop {
            let head = heads.last().copied().unwrap_or(safe);
            let part = self.read_range(floor, head, cancel).await?;
            // The parent of the part's oldest block heads the next part down.
            let parent = part.blocks.first().map(|oldest| BlockRef {
                number: oldest.block.header.number.saturating_sub(1),
                hash: oldest.block.header.parent_hash,
            });
            match (part.hole, parent) {
                // A part cut short at one call's worth: more of the range is below it.
                (Some(HoleReason::TooLong), Some(parent)) if heads.len() < MAX_PROMOTED_PARTS => {
                    heads.push(parent);
                }
                _ => {
                    heads.reverse();
                    return Ok((part, heads));
                }
            }
        }
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

    /// Step 4 for one part of the range: appends `blocks` to the archive if they extend its
    /// tip and match their header's roots. Blocks that do not are not archived, nor any after
    /// them. Returns the newest of them if the
    /// archive now holds them.
    async fn write_blocks(
        &mut self,
        blocks: &[DecodedBlock],
        cancel: &CancellationToken,
    ) -> Result<Option<BlockRef>, Stop> {
        const APPEND: &str = "archive append_batch";
        let archive = self.archive.clone();
        // The bytes archived for a promoted block are encoded here, from the gossip block, and
        // checked against its header: the archive checks only the header's hash.
        let mut encoded: Vec<ArchivedBlock> = Vec::with_capacity(blocks.len());
        for block in blocks {
            let archived = ArchivedBlock::from(block);
            if !roots_match(block, &archived, self.canyon_time) {
                error!(
                    number = block.block.header.number,
                    hash = %block.hash,
                    "a promoted block's transactions or receipts do not match its header's roots; \
                     it and the blocks after it are not archived"
                );
                break;
            }
            encoded.push(archived);
        }
        let blocks = blocks.get(..encoded.len()).unwrap_or_default();
        let Some(newest) = blocks.last().map(|block| BlockRef {
            number: block.block.header.number,
            hash: block.hash,
        }) else {
            return Ok(None);
        };
        let appended = retry(cancel, Store::Archive, APPEND, || {
            archive.append_batch(encoded.clone())
        });
        match appended.await {
            Ok(()) => {}
            Err(RetryError::Storage(StorageError::NotContiguous { expected, got })) => {
                if self.archive_warning_due() {
                    warn!(
                        archive_tip = ?expected,
                        block = ?got,
                        blocks = blocks.len(),
                        "promoted blocks do not extend the archive and are not archived"
                    );
                }
                return Ok(None);
            }
            Err(other) => return Err(stop(APPEND)(other)),
        }
        Ok(Some(newest))
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

    /// Step 5: records `heads` in the archive.
    async fn set_committed_heads(
        &mut self,
        heads: L1Heads,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        // A finalized head above the recorded safe head may be a block the archive lacks.
        let safe = self.merged(heads).safe;
        let heads = L1Heads {
            finalized: heads
                .finalized
                .filter(|finalized| safe.is_some_and(|safe| finalized.number <= safe.number)),
            ..heads
        };
        call(cancel, Store::Archive, "archive set_heads", || {
            self.archive.set_heads(heads)
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
        Ok(())
    }
}

/// Whether the transactions root and, when the block has receipts, the receipts root over
/// exactly the bytes about to be archived are the header's.
fn roots_match(block: &DecodedBlock, archived: &ArchivedBlock, canyon_time: u64) -> bool {
    let header = &block.block.header;
    let transactions = split_body(&archived.encoded.body)
        .is_some_and(|body| transactions_root(&body.transactions) == header.transactions_root);
    transactions
        && block.receipts.as_ref().is_none_or(|receipts| {
            receipts_root(receipts, header.timestamp, canyon_time) == header.receipts_root
        })
}

/// Why a promotion could not read its whole range.
#[derive(Debug, Clone, Copy)]
enum HoleReason {
    /// A block of the range is not in the unsafe store: never received, or expired.
    MissingAncestor,
    /// The range is longer than one ancestry call returns. The blocks may all be stored.
    TooLong,
    /// The oldest block of the range does not build on the committed safe head.
    ParentMismatch,
}

/// Promote now, backfill later: logs the blocks above `base` (the block the range
/// was read above) up to `left_out_to` that are not promoted. With a parent mismatch none are
/// left out, but the block at `base`'s height is another chain's.
fn report_hole(reason: HoleReason, base: BlockRef, left_out_to: BlockNumber, safe: BlockRef) {
    let missing = left_out_to.saturating_sub(base.number);
    let why = match reason {
        HoleReason::MissingAncestor => "a block of the range is not in the unsafe store",
        HoleReason::TooLong => {
            "the range is longer than one promotion reads; the blocks left out may be in \
             the unsafe store but are not promoted"
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
