//! Indexing pipeline: the connection between the network and storage.
//!
//! ```text
//! network ─▶ [ingest]  recover senders ─▶ UnsafeStore::insert
//! L1      ─▶ [promote] UnsafeStore::ancestry ─▶ CommittedStore::insert ─▶ ArchiveStore::append
//!                      ─▶ CommittedStore::set_l1_heads ─▶ UnsafeStore::prune ─▶ safe number
//! ```
//!
//! - [`Pipeline`] owns separate tasks, so a slow committed store never delays a gossiped
//!   block: ingest (`ingest`, `recover`), promotion (`promote`) and, when something fetches
//!   receipts, the task that attaches them (`receipts`).
//! - `retry` is the retry policy storage deliberately does not have: transient store errors are
//!   retried with backoff, everything else is decided by the task that made the call.
//! - [`metrics`] names and records what both tasks do.
//!
//! Generic over the three store traits, so it does not know about Redis, ClickHouse or fjall,
//! and it talks to the networks through channels, so it depends on neither. It does not fetch
//! or verify receipts, it asks for them and stores the answers; it does not fetch missing
//! blocks; and nothing produces the L1 heads yet. The design is in `docs/pipeline.md`.

mod error;
mod ingest;
pub mod metrics;
mod promote;
mod receipts;
mod recover;
mod retry;

use std::fmt;

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{L1Heads, UnsafeBlock};
use op_indexer_storage::{ArchiveRetention, ArchiveStore, CommittedStore, UnsafeStore};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub use error::PipelineError;
use promote::Promoter;
pub use receipts::ReceiptsChannels;

/// Writes gossiped blocks to the unsafe store and moves them to the committed store and the
/// archive once L1 commits them.
pub struct Pipeline<U, C, A> {
    unsafe_store: U,
    /// The archive, for receipts that arrive after their block was promoted.
    archive: Option<A>,
    promoter: Promoter<U, C, A>,
    blocks: mpsc::Receiver<UnsafeBlock>,
    receipts: Option<ReceiptsChannels>,
}

/// One of the pipeline's tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Task {
    Ingest,
    Promote,
    Receipts,
}

impl<U, C, A> Pipeline<U, C, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    C: CommittedStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
{
    /// Creates a pipeline over the three stores.
    ///
    /// - `archive` is the local block archive with how much it keeps, or `None` when it is
    ///   disabled.
    /// - `blocks` are the gossiped blocks; the pipeline stops when the channel closes.
    /// - `l1_heads` are the safe and finalized heads; each change starts a promotion.
    /// - `safe_number` receives the number of the safe head once its blocks are committed.
    /// - `receipts` are the channels to whatever fetches receipts, or `None` when nothing
    ///   does: then no receipts are asked for and blocks stay without them.
    pub fn new(
        unsafe_store: U,
        committed: C,
        archive: Option<(A, ArchiveRetention)>,
        blocks: mpsc::Receiver<UnsafeBlock>,
        l1_heads: watch::Receiver<L1Heads>,
        safe_number: watch::Sender<BlockNumber>,
        receipts: Option<ReceiptsChannels>,
    ) -> Self {
        let receipts_archive = archive.as_ref().map(|(archive, _)| archive.clone());
        let promoter = Promoter::new(
            unsafe_store.clone(),
            committed,
            archive,
            l1_heads,
            safe_number,
        );
        Self {
            unsafe_store,
            archive: receipts_archive,
            promoter,
            blocks,
            receipts,
        }
    }

    /// Reconciles the stores with each other, then runs ingest, promotion and the receipts
    /// task until `cancel` fires or the block channel closes.
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
            stop.clone(),
        );
        tasks.spawn(async move { (Task::Ingest, ingest.await) });
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
                // Promotion ends alone only when nothing sends L1 heads any more, and the
                // receipts task when the fetcher has stopped; ingest carries on without them.
                Ok((Task::Promote | Task::Receipts, Ok(()))) => {}
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
impl<U, C, A> fmt::Debug for Pipeline<U, C, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipeline").finish_non_exhaustive()
    }
}
