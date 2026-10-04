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
//! there), read after every batch of events; a change is published as `Heads`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

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
    /// These blocks, lowest first, are no longer canonical.
    Reorg(Vec<BlockRef>),
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

    /// Where a subscription whose last block is `block` continues: after the newest
    /// publication of that block, where the follower's chain was the subscription's. With the
    /// receipts published before it, for the subscription to pick those it lacks.
    pub(crate) fn join(&self, block: BlockRef) -> Option<(u64, Vec<Arc<Prepared>>)> {
        let window = self.window();
        let joined = window.numbered().rev().find_map(|(seq, event)| {
            matches!(&**event, ChainEvent::Block(published) if published.at == block).then_some(seq)
        })?;
        let receipts = window
            .numbered()
            .take_while(|(seq, _)| *seq < joined)
            .filter_map(|(_, event)| match &**event {
                ChainEvent::Receipts(block) => Some(Arc::clone(block)),
                ChainEvent::Block(_) | ChainEvent::Reorg(_) | ChainEvent::Heads { .. } => None,
            })
            .collect();
        Some((joined.saturating_add(1), receipts))
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
            if let ChainEvent::Block(block) = &event {
                window.last_block = Some(block.at);
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
}

/// What the follower remembers between events.
#[derive(Debug, Default)]
struct State {
    /// The blocks published, oldest first, at most [`CHAIN_WINDOW`].
    chain: VecDeque<BlockRef>,
    /// The newest head the events named.
    target: Option<BlockRef>,
    heads: Option<L1Heads>,
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
            let position = self
                .source
                .call(Store::Unsafe, "unsafe last_event_id", || {
                    self.source.unsafe_store.last_event_id()
                })
                .await?;
            let (unsafe_head, heads) = self.source.heads().await?;
            if let Some(head) = unsafe_head {
                state.target = Some(head);
                self.extend(state, head).await?;
            }
            self.publish_heads(state, heads);
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
        Ok(Some(position))
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
                let kept = reorg
                    .common_ancestor
                    .and_then(|ancestor| state.chain.iter().position(|block| *block == ancestor));
                let removed = match kept {
                    Some(index) => state.chain.drain(index.saturating_add(1)..).collect(),
                    None => self.source.rewind(&mut state.chain).await?,
                };
                self.publish_reorg(removed);
                Ok(())
            }
            UnsafeEvent::Receipts(at) => {
                if state.chain.contains(&at)
                    && let Some(block) = self.source.block_by_hash(at.hash).await?
                    && block.has_receipts()
                {
                    self.live.publish(ChainEvent::Receipts(Arc::new(block)));
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
                // A start, or a reorg deeper than the blocks remembered: the head is the first
                // block; subscriptions read what is below it from the stores.
                if let Some(block) = self.source.block_at(head.number).await? {
                    self.publish_block(state, block);
                }
                return Ok(());
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
                self.publish_reorg(removed);
            }
        }
        Ok(())
    }

    fn publish_block(&self, state: &mut State, block: Prepared) {
        push_bounded(&mut state.chain, block.at, CHAIN_WINDOW);
        self.live.publish(ChainEvent::Block(Arc::new(block)));
    }

    fn publish_reorg(&self, removed: Vec<BlockRef>) {
        let Some(first) = removed.first() else {
            return;
        };
        warn!(
            from = first.number,
            depth = removed.len(),
            "reorg in the streamed chain"
        );
        self.live.publish(ChainEvent::Reorg(removed));
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
