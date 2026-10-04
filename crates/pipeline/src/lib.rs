//! Indexing pipeline: the connection between the network and storage.
//!
//! ```text
//! network ─▶ [ingest]  recover senders ─▶ UnsafeStore::insert
//! L1      ─▶ [promote] UnsafeStore::ancestry ─▶ ArchiveStore::append_batch
//!                      ─▶ ArchiveStore::set_heads ─▶ UnsafeStore::prune ─▶ safe number
//! peers   ─▶ [range]   recover senders ─▶ ArchiveStore::append_batch
//! peers   ─▶ [receipts] UnsafeStore / ArchiveStore::set_receipts
//! L1      ─▶ [commit]  dispute games checked against our blocks ─▶ L1 heads
//! ```
//!
//! - [`Pipeline`] owns separate tasks, so a slow archive never delays a gossiped block: ingest
//!   (`ingest`, `recover`), promotion (`promote`) and, when something fetches receipts, the task
//!   that attaches them (`receipts`); and, when a range of blocks is fetched from peers, the task
//!   that stores it (`range`); and, when the L1 side runs, the task that turns its dispute games
//!   into heads (`commit`).
//! - `retry` is how store calls are made: transient store errors are retried with backoff
//!   (storage's helper, without a time limit), everything else is decided by the task that
//!   made the call.
//! - [`metrics`] names and records what the tasks do.
//!
//! Generic over the two store traits, so it does not know about Redis or fjall,
//! and it talks to the networks through channels, so it depends on neither. It does not fetch
//! or verify receipts, it asks for them and stores the answers; it does not fetch missing
//! blocks. The design is in `docs/pipeline.md`.

mod commit;
mod error;
mod ingest;
pub mod metrics;
mod promote;
mod range;
mod receipts;
mod recover;
mod retry;

use std::fmt;

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, EncodedBlock, L1Games, L1Heads, UnsafeBlock};
use op_indexer_storage::{ArchiveRetention, ArchiveStore, UnsafeStore};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub use error::PipelineError;
use promote::Promoter;
pub use receipts::ReceiptsChannels;

/// Writes gossiped blocks to the unsafe store and moves them to the archive, the committed
/// store, once L1 commits them.
pub struct Pipeline<U, A> {
    unsafe_store: U,
    /// The archive, also for receipts that arrive after their block was promoted, the
    /// commitment task and the range task.
    archive: A,
    promoter: Promoter<U, A>,
    blocks: mpsc::Receiver<UnsafeBlock>,
    receipts: Option<ReceiptsChannels>,
    range: Option<mpsc::Receiver<Vec<EncodedBlock>>>,
    head: Option<watch::Sender<Option<BlockRef>>>,
    /// The dispute games, the chain's Isthmus time, and where the heads they give go.
    games: Option<(watch::Receiver<L1Games>, u64, watch::Sender<L1Heads>)>,
}

/// One of the pipeline's tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Task {
    Ingest,
    Promote,
    Receipts,
    Range,
    Commit,
}

impl<U, A> Pipeline<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
{
    /// Creates a pipeline over the two stores.
    ///
    /// - `archive` is the local block archive, the committed store, with how much it keeps.
    /// - `blocks` are the gossiped blocks; the pipeline stops when the channel closes.
    /// - `l1_heads` are the safe and finalized heads; each change starts a promotion.
    /// - `safe_number` receives the number of the safe head once its blocks are committed.
    /// - `receipts` are the channels to whatever fetches receipts, or `None` when nothing
    ///   does: then no receipts are asked for and blocks stay without them.
    pub fn new(
        unsafe_store: U,
        archive: (A, ArchiveRetention),
        blocks: mpsc::Receiver<UnsafeBlock>,
        l1_heads: watch::Receiver<L1Heads>,
        safe_number: watch::Sender<BlockNumber>,
        receipts: Option<ReceiptsChannels>,
    ) -> Self {
        let (archive, retention) = archive;
        let promoter = Promoter::new(
            unsafe_store.clone(),
            archive.clone(),
            retention,
            l1_heads,
            safe_number,
        );
        Self {
            unsafe_store,
            archive,
            promoter,
            blocks,
            receipts,
            range: None,
            head: None,
            games: None,
        }
    }

    /// Publishes the unsafe head on `head` whenever ingest moves it, for whatever has to know
    /// the newest block the node holds.
    #[must_use]
    pub fn with_head(mut self, head: watch::Sender<Option<BlockRef>>) -> Self {
        self.head = Some(head);
        self
    }

    /// Adds the dispute games verified on L1 (the recent ones, and how far L1 is finalized):
    /// each is checked against our own block at its height, and the highest match is published
    /// on `heads` as the safe head, the highest in a finalized L1 block as the finalized head.
    /// The heads start at what the archive recorded and never go below it. `heads` is
    /// what feeds the `l1_heads` given to [`Self::new`], directly or through whatever decides
    /// when promotion may act on them. `isthmus_time` is the chain's Isthmus activation: a
    /// claim about a block before it cannot be checked.
    #[must_use]
    pub fn with_l1_games(
        mut self,
        games: watch::Receiver<L1Games>,
        heads: watch::Sender<L1Heads>,
        isthmus_time: u64,
    ) -> Self {
        self.games = Some((games, isthmus_time, heads));
        self
    }

    /// Adds a range of blocks fetched from peers: `batches` are verified blocks in ascending
    /// order, each batch consecutive, appended to the archive with their recovered senders.
    /// The range task ends when the channel closes.
    ///
    /// The archive must be empty or end at the block before the first batch, and keep every
    /// block: it holds one contiguous range, which the range task extends. A batch the archive
    /// refuses stops the pipeline with the error.
    #[must_use]
    pub fn with_range(mut self, batches: mpsc::Receiver<Vec<EncodedBlock>>) -> Self {
        self.range = Some(batches);
        self
    }

    /// Reconciles the stores with each other, then runs ingest, promotion, the receipts task
    /// and the range task until `cancel` fires or the block channel closes.
    ///
    /// Every task finishes the write in progress before it stops; every store write is
    /// idempotent, so one cut short is repeated on the next start. Ingest also stores the
    /// blocks already in the channel. If one task fails, the others are stopped.
    ///
    /// # Errors
    ///
    /// Returns the first [`PipelineError`] of any task: a store failed in a way retrying
    /// cannot fix, or a task panicked.
    pub async fn run(mut self, cancel: CancellationToken) -> Result<(), PipelineError> {
        metrics::describe();
        self.promoter.reconcile(&cancel).await?;

        // A child token, so a task that ends can stop the others without stopping the caller.
        let stop = cancel.child_token();
        let mut tasks = JoinSet::new();
        let requests = self
            .receipts
            .as_ref()
            .map(|channels| channels.requests.clone());
        let ingest = ingest::run(
            self.unsafe_store.clone(),
            self.blocks,
            requests,
            self.head,
            stop.clone(),
        );
        tasks.spawn(async move { (Task::Ingest, ingest.await) });
        if let Some((games, isthmus_time, heads)) = self.games {
            let commit = commit::run(
                self.unsafe_store.clone(),
                self.archive.clone(),
                games,
                isthmus_time,
                heads,
                self.promoter.committed_heads(),
                stop.clone(),
            );
            tasks.spawn(async move { (Task::Commit, commit.await) });
        }
        if let Some(channels) = self.range {
            let range = range::run(self.archive.clone(), channels, stop.clone());
            tasks.spawn(async move { (Task::Range, range.await) });
        }
        if let Some(channels) = self.receipts {
            let receipts = receipts::run(self.unsafe_store, self.archive, channels, stop.clone());
            tasks.spawn(async move { (Task::Receipts, receipts.await) });
        }
        let promote = self.promoter.run(stop.clone());
        tasks.spawn(async move { (Task::Promote, promote.await) });

        let mut first_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                // Ingest ends when the network does; the others have nothing left to follow.
                Ok((Task::Ingest, Ok(()))) => stop.cancel(),
                // Promotion ends alone only when nothing sends L1 heads any more, the receipts
                // task when the fetcher has stopped, and the range task when its range is
                // done or cannot be stored; ingest carries on without them.
                Ok((Task::Promote | Task::Receipts | Task::Range | Task::Commit, Ok(()))) => {}
                Ok((_, Err(err))) => {
                    stop.cancel();
                    first_error.get_or_insert(err);
                }
                Err(err) => {
                    stop.cancel();
                    first_error.get_or_insert(PipelineError::Task(err));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

// The stores and channel ends have nothing useful to print.
impl<U, A> fmt::Debug for Pipeline<U, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipeline").finish_non_exhaustive()
    }
}
