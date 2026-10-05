//! The follower: one task that follows the canonical chain for every subscription, from the
//! unsafe store's event stream, and the bounded window of numbered events it publishes.
//!
//! The event stream (`docs/storage.md` section 3.3) records what each write did: a new head,
//! a reorg with the blocks it replaced, a gap filled, receipts attached. The follower reads it
//! from where it left off and publishes the same facts for the canonical chain: a `Block` for
//! each block on top of the last one published, a `Reorg` when a reorg removes published
//! blocks, `Receipts` when a published block gets its receipts.
//!
//! It reads the stores themselves only where the events do not say enough:
//!
//! - **At start**, and when the stream has trimmed events it had not read: it takes the
//!   stream's position, then reads the heads and walks up from the last block published (or
//!   starts at the head). A block whose parent is not the last one published is a reorg, found
//!   by reading which published blocks are still canonical.
//! - **For a gap** (a head two or more heights above the last): it walks up one height at a
//!   time and stops below a height not held, until a `fill` event comes.
//!
//! The committed safe and finalized heads come from the archive (promotion records them
//! there), read after every batch of events; a change is published as `Heads`. Then the
//! published blocks still without receipts are looked up once more: the pipeline attaches
//! late receipts to archived blocks, which has no event.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, L1Heads, UnsafeEvent};
use op_indexer_storage::{ArchiveStore, EventId, Store, UnsafeStore};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::convert::Prepared;
use crate::source::{ReadError, Source, push_bounded};

/// Events kept for subscriptions to join and to catch up from: about two minutes of blocks
/// at one a second. A subscription that falls further behind reads from the stores again.
const WINDOW_EVENTS: usize = 128;
/// Blocks remembered as published: the deepest reorg found without starting over from the
/// head.
pub(crate) const CHAIN_WINDOW: usize = 256;
/// Most heights walked for one event, so a long walk lets the next events in.
const MAX_STEPS: usize = 1024;
/// Events read from the stream at once.
const EVENTS_PER_READ: usize = 256;
/// How long one read of the stream waits for an event; the heads are read again after it.
const EVENT_WAIT: Duration = Duration::from_secs(5);

/// One thing that happened to the canonical chain, as the follower saw it.
#[derive(Debug)]
pub(crate) enum ChainEvent {
    /// A block became canonical on top of the previous one.
    Block(Arc<Prepared>),
    /// The receipts of a block published without them were attached.
    Receipts(Arc<Prepared>),
    /// The blocks `removed`, lowest first, are no longer canonical; `last` is the last block
    /// published that still is.
    Reorg {
        removed: Vec<BlockRef>,
        last: Option<BlockRef>,
    },
    /// The committed safe or finalized head moved; `unsafe_head` is the last block published.
    Heads {
        unsafe_head: Option<BlockRef>,
        heads: L1Heads,
    },
}

/// The events the follower published, numbered contiguously, the oldest dropped first.
#[derive(Debug, Default)]
struct Window {
    events: VecDeque<Arc<ChainEvent>>,
    /// The number of the next event.
    next: u64,
    /// The last block published.
    last_block: Option<BlockRef>,
    /// The heads last published.
    heads: Option<L1Heads>,
}

impl Window {
    fn oldest(&self) -> u64 {
        let len = u64::try_from(self.events.len()).unwrap_or(u64::MAX);
        self.next.saturating_sub(len)
    }

    fn numbered(&self) -> impl DoubleEndedIterator<Item = (u64, &Arc<ChainEvent>)> {
        let oldest = self.oldest();
        self.events.iter().enumerate().map(move |(index, event)| {
            let index = u64::try_from(index).unwrap_or(u64::MAX);
            (oldest.saturating_add(index), event)
        })
    }
}

/// The follower's output, shared with every subscription.
#[derive(Debug)]
pub(crate) struct Live {
    window: Mutex<Window>,
    /// Sent after each publish.
    published: watch::Sender<()>,
}

impl Live {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            window: Mutex::default(),
            published: watch::Sender::new(()),
        })
    }

    fn window(&self) -> MutexGuard<'_, Window> {
        self.window.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wakes on every publish.
    pub(crate) fn subscribe(&self) -> watch::Receiver<()> {
        self.published.subscribe()
    }

    /// The number of the next event and the last block published: where a subscription that
    /// starts at the head begins.
    pub(crate) fn tail(&self) -> (u64, Option<BlockRef>) {
        let window = self.window();
        (window.next, window.last_block)
    }

    /// The last block and the heads published; `None` before the first heads.
    pub(crate) fn heads(&self) -> Option<(Option<BlockRef>, L1Heads)> {
        let window = self.window();
        window.heads.map(|heads| (window.last_block, heads))
    }

    /// Where a subscription whose last block is `block` continues, if the follower's last
    /// block is that one too: the follower's chain is then the subscription's, and every event
    /// from the next one on applies to it. With the receipts in the window, for the
    /// subscription to pick those it lacks.
    pub(crate) fn join(&self, block: BlockRef) -> Option<(u64, Vec<Arc<Prepared>>)> {
        let window = self.window();
        if window.last_block != Some(block) {
            return None;
        }
        let receipts = window
            .events
            .iter()
            .filter_map(|event| match &**event {
                ChainEvent::Receipts(block) => Some(Arc::clone(block)),
                ChainEvent::Block(_) | ChainEvent::Reorg { .. } | ChainEvent::Heads { .. } => None,
            })
            .collect();
        Some((window.next, receipts))
    }

    /// The events from number `from` on; `None` if the window no longer holds them.
    pub(crate) fn after(&self, from: u64) -> Option<Vec<(u64, Arc<ChainEvent>)>> {
        let window = self.window();
        if from < window.oldest() {
            return None;
        }
        Some(
            window
                .numbered()
                .filter(|(seq, _)| *seq >= from)
                .map(|(seq, event)| (seq, Arc::clone(event)))
                .collect(),
        )
    }

    fn publish(&self, event: ChainEvent) {
        {
            let mut window = self.window();
            match &event {
                ChainEvent::Block(block) => window.last_block = Some(block.at),
                ChainEvent::Reorg { last, .. } => window.last_block = *last,
                ChainEvent::Heads { heads, .. } => window.heads = Some(*heads),
                ChainEvent::Receipts(_) => {}
            }
            window.events.push_back(Arc::new(event));
            if window.events.len() > WINDOW_EVENTS {
                window.events.pop_front();
            }
            window.next = window.next.saturating_add(1);
        }
        self.published.send_replace(());
    }
}

/// Follows the canonical chain and publishes it on [`Live`].
#[derive(Debug)]
pub(crate) struct Follower<U, A> {
    pub(crate) source: Source<U, A>,
    pub(crate) live: Arc<Live>,
    /// How long to wait after a store failed before reading again.
    pub(crate) retry_after: Duration,
    /// Whether receipts are fetched at all: without, no block is looked up for them.
    pub(crate) receipts: bool,
}

/// What the follower remembers between events.
#[derive(Debug, Default)]
struct State {
    /// The blocks published, oldest first, at most [`CHAIN_WINDOW`].
    chain: VecDeque<BlockRef>,
    /// The newest head the events named.
    target: Option<BlockRef>,
    heads: Option<L1Heads>,
    /// The published blocks without receipts, oldest first.
    without_receipts: Vec<BlockRef>,
    /// After a reorg removed every block remembered: the height to publish from again, so the
    /// chain published has no gap.
    resume_at: Option<BlockNumber>,
}

impl<U: UnsafeStore, A: ArchiveStore> Follower<U, A> {
    /// Runs until `cancel` fires. A store that fails is logged, and the follower reads the
    /// state again after [`Self::retry_after`].
    pub(crate) async fn run(self, cancel: CancellationToken) {
        let mut state = State::default();
        // Where to read the stream from; `None` reads the state first.
        let mut after = None;
        loop {
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                result = self.step(&mut state, after) => result,
            };
            after = match result {
                Ok(next) => next,
                Err(ReadError::Cancelled) => return,
                Err(err) => {
                    warn!(%err, "the stream's follower cannot read the stores; reading again");
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        () = tokio::time::sleep(self.retry_after) => {}
                    }
                    None
                }
            };
        }
    }

    /// Reads the state when `after` is `None`, else the events after it. Returns where to
    /// read the stream from next; `None` when the stream lost events.
    async fn step(
        &self,
        state: &mut State,
        after: Option<EventId>,
    ) -> Result<Option<EventId>, ReadError> {
        let Some(after) = after else {
            // The position first: what happens while the state is read is read again.
            let position = self.last_event_id().await?;
            let (unsafe_head, heads) = self.source.heads().await?;
            let tracked = state.without_receipts.clone();
            // A head at or below the last block published, and not it: the store went back
            // (a reorg to a shorter chain) under the published tail.
            let went_back = match (state.chain.back(), unsafe_head) {
                (Some(last), Some(head)) => last.number >= head.number && *last != head,
                (Some(_), None) => true,
                (None, _) => false,
            };
            if went_back {
                let removed = self.source.rewind(&mut state.chain).await?;
                self.publish_reorg(state, removed);
            }
            if let Some(head) = unsafe_head {
                state.target = Some(head);
                self.extend(state, head).await?;
            }
            self.publish_heads(state, heads);
            // Receipts events may have been missed while the state was lost.
            self.recheck_missed_receipts(state, tracked).await?;
            return Ok(Some(position));
        };
        let batch = self
            .source
            .call(Store::Unsafe, "unsafe events", || {
                self.source
                    .unsafe_store
                    .events(after, EVENTS_PER_READ, EVENT_WAIT)
            })
            .await?;
        if batch.missed {
            warn!("the stream's follower fell behind the unsafe store's events; reading state");
            return Ok(None);
        }
        let mut position = after;
        for (id, event) in batch.events {
            self.apply(state, event).await?;
            position = id;
        }
        let heads = self
            .source
            .call(Store::Archive, "archive heads", || {
                self.source.archive.heads()
            })
            .await?;
        self.publish_heads(state, heads);
        self.recheck_receipts(state).await?;
        Ok(Some(position))
    }

    /// Publishes the receipts the archive got for published blocks: those at or below the
    /// safe head are archived, and the unsafe store's `receipts` events cover the others.
    /// The archive's list of blocks still without receipts is read once; only the blocks
    /// that left it are read.
    async fn recheck_receipts(&self, state: &mut State) -> Result<(), ReadError> {
        let chain = &state.chain;
        state.without_receipts.retain(|at| chain.contains(at));
        let Some(safe) = state.heads.and_then(|heads| heads.safe) else {
            return Ok(());
        };
        let archived: Vec<BlockRef> = state
            .without_receipts
            .iter()
            .filter(|at| at.number <= safe.number)
            .copied()
            .collect();
        let Some(lowest) = archived.first() else {
            return Ok(());
        };
        let (pending, _) = self
            .source
            .call(Store::Archive, "archive pending_receipts", || {
                self.source
                    .archive
                    .pending_receipts(lowest.number, CHAIN_WINDOW)
            })
            .await?;
        for at in archived.into_iter().filter(|at| !pending.contains(at)) {
            if let Some(block) = self.source.archived_with_receipts(at).await? {
                self.publish_receipts(state, block);
            }
        }
        Ok(())
    }

    /// Publishes the receipts either store has for `tracked` (blocks tracked before the state
    /// was read again, whose events may have been missed), looked up one by one.
    async fn recheck_missed_receipts(
        &self,
        state: &mut State,
        tracked: Vec<BlockRef>,
    ) -> Result<(), ReadError> {
        for at in tracked {
            if state.chain.contains(&at)
                && let Some(block) = self.source.with_receipts(at).await?
            {
                self.publish_receipts(state, block);
            }
        }
        Ok(())
    }

    /// Publishes `block`'s receipts, and stops tracking it.
    fn publish_receipts(&self, state: &mut State, block: Prepared) {
        state
            .without_receipts
            .retain(|pending| *pending != block.at);
        self.live.publish(ChainEvent::Receipts(Arc::new(block)));
    }

    async fn last_event_id(&self) -> Result<EventId, ReadError> {
        Ok(self
            .source
            .call(Store::Unsafe, "unsafe last_event_id", || {
                self.source.unsafe_store.last_event_id()
            })
            .await?)
    }

    async fn apply(&self, state: &mut State, event: UnsafeEvent) -> Result<(), ReadError> {
        match event {
            UnsafeEvent::NewHead { head, .. } => {
                state.target = Some(head);
                self.extend(state, head).await
            }
            UnsafeEvent::Filled(_) => match state.target {
                Some(target) => self.extend(state, target).await,
                None => Ok(()),
            },
            UnsafeEvent::Reorg(reorg) => {
                // Only from the first block it replaced: an earlier event of the batch may
                // have read the new chain already, and its blocks stay.
                let first = state
                    .chain
                    .iter()
                    .position(|block| reorg.replaced.contains(&block.hash));
                if let Some(first) = first {
                    let removed = state.chain.drain(first..).collect();
                    self.publish_reorg(state, removed);
                }
                Ok(())
            }
            UnsafeEvent::Receipts(at) => {
                if state.without_receipts.contains(&at)
                    && let Some(block) = self.source.with_receipts(at).await?
                {
                    self.publish_receipts(state, block);
                }
                Ok(())
            }
            UnsafeEvent::Pruned { .. } => Ok(()),
        }
    }

    /// Publishes the canonical blocks above the last one published, up to `head`.
    async fn extend(&self, state: &mut State, head: BlockRef) -> Result<(), ReadError> {
        for _ in 0..MAX_STEPS {
            let Some(last) = state.chain.back().copied() else {
                // A start: the head is the first block, and subscriptions read what is below
                // it from the stores. After a reorg that removed every block remembered: from
                // the first removed height on, so nothing is skipped.
                let first = state.resume_at.unwrap_or(head.number).min(head.number);
                let Some(block) = self.source.block_at(first).await? else {
                    debug!(
                        number = first,
                        "the stream's follower waits for a block it lacks"
                    );
                    return Ok(());
                };
                state.resume_at = None;
                self.publish_block(state, block);
                continue;
            };
            if last.number >= head.number {
                return Ok(());
            }
            let next = last.number.saturating_add(1);
            let Some(block) = self.source.block_at(next).await? else {
                debug!(
                    number = next,
                    "the unsafe chain has a gap; the stream waits at it"
                );
                return Ok(());
            };
            if block.parent == last.hash {
                self.publish_block(state, block);
            } else {
                let removed = self.source.rewind(&mut state.chain).await?;
                if removed.is_empty() {
                    // The stores disagree for a moment (a block promoted between two reads).
                    return Ok(());
                }
                self.publish_reorg(state, removed);
            }
        }
        Ok(())
    }

    fn publish_block(&self, state: &mut State, block: Prepared) {
        push_bounded(&mut state.chain, block.at, CHAIN_WINDOW);
        if self.receipts && !block.has_receipts() {
            state.without_receipts.push(block.at);
        }
        self.live.publish(ChainEvent::Block(Arc::new(block)));
    }

    fn publish_reorg(&self, state: &mut State, removed: Vec<BlockRef>) {
        let Some(first) = removed.first() else {
            return;
        };
        if state.chain.is_empty() {
            state.resume_at = Some(first.number);
        }
        warn!(
            from = first.number,
            depth = removed.len(),
            "reorg in the streamed chain"
        );
        self.live.publish(ChainEvent::Reorg {
            removed,
            last: state.chain.back().copied(),
        });
    }

    fn publish_heads(&self, state: &mut State, heads: L1Heads) {
        if state.heads != Some(heads) {
            state.heads = Some(heads);
            self.live.publish(ChainEvent::Heads {
                unsafe_head: state.chain.back().copied(),
                heads,
            });
        }
    }
}
