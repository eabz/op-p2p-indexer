//! One subscription: catches up by reading the stores, then follows the follower's window.
//!
//! **Reading**: from its next height, it reads batches from the archive and then the unsafe
//! store's canonical blocks, checking that each block's parent is the last one sent; one that
//! is not means a reorg, found by reading which blocks it sent are still canonical. Before each
//! batch it sends the follower's heads if they moved, and every few seconds it looks up the
//! blocks it sent without receipts again. It also tries to join the window: when the
//! follower's last block is its own, the follower's chain is its own, so from the window's
//! next event every event applies as it is. Receipts in the window, for blocks it sent without
//! them, are sent at the join. A read that finds nothing below what the stores hold, and never
//! will, ends the subscription with `OUT_OF_RANGE`.
//!
//! **Following**: blocks, reorgs (restricted to the blocks it sent), receipts (for blocks it
//! sent without them) and heads are sent in the window's order. A block that does not build on
//! its last one, or a window that moved past it, sends it back to reading.
//!
//! So, per subscription, every height is sent once per canonical chain, in chain order, and no
//! event is applied twice.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, L1Heads};
use op_indexer_storage::{ArchiveStore, UnsafeStore};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::warn;

use crate::Sent;
use crate::convert::{Payload, Prepared, heads_message};
use crate::follower::{CHAIN_WINDOW, ChainEvent, Live};
use crate::proto;
use crate::sink::{Ended, Sink};
use crate::source::{History, ReadError, Source, push_bounded, read_status};

/// How often, while reading, the blocks sent without receipts are looked up again.
const RECEIPTS_RECHECK: Duration = Duration::from_secs(5);

/// The events a subscription sends, and the status it ends with.
pub(crate) type Item = Result<proto::Event, Status>;

/// Where a subscription starts.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Start {
    /// At this height.
    Number(BlockNumber),
    /// After the follower's last block.
    Head,
}

/// Why a subscription stops.
#[derive(Debug)]
enum Stop {
    /// The consumer left, or was told why.
    Ended,
    Read(ReadError),
}

impl From<ReadError> for Stop {
    fn from(err: ReadError) -> Self {
        Self::Read(err)
    }
}

impl From<Ended> for Stop {
    fn from(Ended: Ended) -> Self {
        Self::Ended
    }
}

/// What a subscription does next.
#[derive(Debug, Clone, Copy)]
enum Mode {
    /// Read from the stores.
    Read,
    /// Follow the window from this event.
    Follow(u64),
}

/// One subscription's state.
#[derive(Debug)]
pub(crate) struct Subscription<U, A> {
    source: Source<U, A>,
    live: Arc<Live>,
    payload: Payload,
    sink: Sink<proto::Event, Status>,
    /// Held while the subscription runs: the server's limit on subscriptions.
    _permit: OwnedSemaphorePermit,
    /// Whether receipts are fetched at all, for the `Heads` messages.
    receipts: bool,
    /// The server's count of bytes sent, which every event adds to.
    traffic: Sent,
    /// The blocks sent, oldest first, at most [`CHAIN_WINDOW`].
    sent: VecDeque<BlockRef>,
    /// Blocks sent without receipts.
    /// With whether the window covers it: a block sent from the window while following gets
    /// its `Receipts` event there; one sent from the stores, or left behind by the window, is
    /// looked up again.
    without_receipts: HashMap<BlockRef, bool>,
    heads: L1Heads,
    /// The next height to read, or to accept from the window when nothing was sent yet (or
    /// a reorg removed all that was).
    next: BlockNumber,
    /// Whether the window's first block is accepted whatever its height: a subscription from
    /// the head of a node that holds nothing yet.
    any_first: bool,
    /// Whether `next` is in a gap range sync is filling: only the archive's tip is watched
    /// until it reaches `next`.
    in_gap: bool,
    /// This subscription's archive position and read-ahead.
    history: History,
    /// When the blocks sent without receipts were last looked up.
    receipts_checked: Instant,
}

impl<U: UnsafeStore, A: ArchiveStore> Subscription<U, A> {
    pub(crate) fn new(
        source: Source<U, A>,
        live: Arc<Live>,
        payload: Payload,
        sink: Sink<proto::Event, Status>,
        permit: OwnedSemaphorePermit,
        receipts: bool,
        traffic: Sent,
    ) -> Self {
        Self {
            source,
            live,
            payload,
            sink,
            _permit: permit,
            receipts,
            traffic,
            sent: VecDeque::new(),
            without_receipts: HashMap::new(),
            heads: L1Heads::default(),
            next: 0,
            any_first: false,
            in_gap: false,
            history: History::default(),
            receipts_checked: Instant::now(),
        }
    }

    /// Runs until the consumer leaves, is too slow, or `cancel` fires. A consumer that leaves
    /// ends it at once, even while a store call is being retried.
    pub(crate) async fn run(mut self, start: Start, cancel: CancellationToken) {
        let left = self.sink.closed();
        let stopped = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(Stop::Read(ReadError::Cancelled)),
            () = left => Err(Stop::Ended),
            stopped = self.serve(start) => stopped,
        };
        match stopped {
            Ok(()) | Err(Stop::Ended) => {}
            Err(Stop::Read(err)) => self.sink.end(read_status(&err)),
        }
    }

    async fn serve(&mut self, start: Start) -> Result<(), Stop> {
        let mut published = self.live.subscribe();
        let (unsafe_head, heads) = self.source.heads().await?;
        self.heads = heads;
        self.send_heads(unsafe_head).await?;
        let mut mode = match start {
            Start::Number(number) => {
                self.next = number;
                Mode::Read
            }
            // From the block after the follower's last (or the store's head): nothing sent yet,
            // so a reorg of blocks before it is not the subscription's.
            Start::Head => {
                let (next, last) = self.live.tail();
                match last.or(unsafe_head) {
                    Some(last) => self.next = last.number.saturating_add(1),
                    None => self.any_first = true,
                }
                Mode::Follow(next)
            }
        };
        loop {
            let step = match mode {
                Mode::Read => self.read().await?,
                Mode::Follow(cursor) => self.follow(cursor).await?,
            };
            if let Some(next) = step {
                mode = next;
                continue;
            }
            // Nothing to send yet: the follower's next publish may change that.
            if published.changed().await.is_err() {
                return Ok(());
            }
        }
    }

    /// Joins the window if it can, else reads and sends one batch. `None` when there is
    /// nothing to read yet.
    async fn read(&mut self) -> Result<Option<Mode>, Stop> {
        if let Some((unsafe_head, heads)) = self.live.heads() {
            self.update_heads(unsafe_head, heads).await?;
        }
        if let Some(last) = self.sent.back()
            && let Some((cursor, receipts)) = self.live.join(*last)
        {
            for block in receipts {
                if self.without_receipts.remove(&block.at).is_some() {
                    self.send_receipts(block).await?;
                }
            }
            self.history = History::default();
            return Ok(Some(Mode::Follow(cursor)));
        }
        self.recheck_receipts().await?;
        if self.in_gap {
            let tip = self
                .source
                .archive_range()
                .await?
                .map(|(_, tip)| tip.number);
            if tip.is_none_or(|tip| tip < self.next) {
                return Ok(None);
            }
            self.in_gap = false;
        }
        let blocks = self
            .source
            .blocks_from(self.next, &mut self.history)
            .await?;
        if blocks.is_empty() {
            // Above the follower's last block, nothing is there yet; below it, maybe never.
            let (_, last) = self.live.tail();
            if last.is_none_or(|last| self.next <= last.number) {
                let holdings = self.source.holdings().await?;
                if let Err(status) = holdings.ensure_held(self.next) {
                    self.sink.end(status);
                    return Err(Stop::Ended);
                }
                // Held, so in a gap only if range sync fills it, into the archive.
                self.in_gap = holdings.gap_at(self.next).is_some();
            }
            return Ok(None);
        }
        for block in blocks {
            if let Some(last) = self.sent.back()
                && block.parent != last.hash
            {
                let removed = self.source.rewind(&mut self.sent).await?;
                if removed.is_empty() {
                    // The stores disagree for a moment (a block promoted between two reads).
                    return Ok(None);
                }
                self.send_reorg(&removed).await?;
                return Ok(Some(Mode::Read));
            }
            self.send_block(Arc::new(block), false).await?;
        }
        Ok(Some(Mode::Read))
    }

    /// Every [`RECEIPTS_RECHECK`], in both modes, sends the receipts the stores got for blocks
    /// it sent without them: the follower publishes only those of blocks it published itself.
    async fn recheck_receipts(&mut self) -> Result<(), Stop> {
        if !self.receipts || self.receipts_checked.elapsed() < RECEIPTS_RECHECK {
            return Ok(());
        }
        self.receipts_checked = Instant::now();
        let uncovered: Vec<BlockRef> = self
            .without_receipts
            .iter()
            .filter(|(_, covered)| !**covered)
            .map(|(at, _)| *at)
            .collect();
        for at in uncovered {
            if let Some(block) = self.source.with_receipts(at).await? {
                self.without_receipts.remove(&at);
                self.send_receipts(Arc::new(block)).await?;
            }
        }
        Ok(())
    }

    /// Sends the window's events from `cursor` on. `None` when there are none yet.
    async fn follow(&mut self, cursor: u64) -> Result<Option<Mode>, Stop> {
        let Some(events) = self.live.after(cursor) else {
            return Ok(Some(self.leave_window()));
        };
        let Some(next) = events.last().map(|(seq, _)| seq.saturating_add(1)) else {
            return Ok(None);
        };
        for (_, event) in events {
            match &*event {
                ChainEvent::Block(block) => {
                    // On its last block, or with none, at the height it expects next: a block
                    // past a gap is read from the stores instead.
                    let fits = match self.sent.back() {
                        Some(last) => last.hash == block.parent,
                        None => self.any_first || block.at.number == self.next,
                    };
                    if !fits {
                        return Ok(Some(self.leave_window()));
                    }
                    self.send_block(Arc::clone(block), true).await?;
                }
                ChainEvent::Receipts(block) => {
                    if self.without_receipts.remove(&block.at).is_some() {
                        self.send_receipts(Arc::clone(block)).await?;
                    }
                }
                ChainEvent::Reorg { removed, .. } => {
                    if let Some(index) = self.sent.iter().position(|block| removed.contains(block))
                    {
                        let ours: Vec<BlockRef> = self.sent.drain(index..).collect();
                        self.send_reorg(&ours).await?;
                    }
                }
                ChainEvent::Heads { unsafe_head, heads } => {
                    self.update_heads(*unsafe_head, *heads).await?;
                }
            }
        }
        self.recheck_receipts().await?;
        Ok(Some(Mode::Follow(next)))
    }

    /// Stops following: the window's events from here on are not seen, so its blocks without
    /// receipts are looked up again. A subscription from the head that has sent nothing yet
    /// goes back to the window's tail rather than to the stores.
    fn leave_window(&mut self) -> Mode {
        for covered in self.without_receipts.values_mut() {
            *covered = false;
        }
        if self.any_first {
            return Mode::Follow(self.live.tail().0);
        }
        Mode::Read
    }

    /// Sends a `Reorg` of `removed` (lowest first, no longer in `sent`) and reads on from the
    /// first of them.
    async fn send_reorg(&mut self, removed: &[BlockRef]) -> Result<(), Stop> {
        let Some(first) = removed.first() else {
            return Ok(());
        };
        self.next = first.number;
        for block in removed {
            self.without_receipts.remove(block);
        }
        let reorg = proto::Reorg {
            from: first.number,
            removed: removed
                .iter()
                .map(|block| bytes::Bytes::copy_from_slice(block.hash.as_slice()))
                .collect(),
        };
        self.send(proto::event::Event::Reorg(reorg)).await
    }

    async fn send(&mut self, event: proto::event::Event) -> Result<(), Stop> {
        let event = proto::Event { event: Some(event) };
        self.traffic.message(&event);
        self.sink.send(event).await?;
        Ok(())
    }

    /// Sends `block`; `covered` when it comes from the window, which then brings its receipts.
    async fn send_block(&mut self, block: Arc<Prepared>, covered: bool) -> Result<(), Stop> {
        let (payload, heads, at) = (self.payload, self.heads, block.at);
        let has_receipts = block.has_receipts();
        let message = self.convert(move || block.message(payload, &heads)).await?;
        self.send(proto::event::Event::Block(message)).await?;
        if !has_receipts {
            self.without_receipts.insert(at, covered);
        }
        if let Some(dropped) = push_bounded(&mut self.sent, at, CHAIN_WINDOW) {
            self.without_receipts.remove(&dropped);
        }
        self.next = at.number.saturating_add(1);
        self.any_first = false;
        Ok(())
    }

    async fn send_receipts(&mut self, block: Arc<Prepared>) -> Result<(), Stop> {
        let payload = self.payload;
        if let Some(receipts) = self.convert(move || block.receipts(payload)).await? {
            self.send(proto::event::Event::Receipts(receipts)).await?;
        }
        Ok(())
    }

    /// Sends `Heads` if `heads` move one past what was sent; never back: the stores and the
    /// follower's window may each be a little behind the other.
    async fn update_heads(
        &mut self,
        unsafe_head: Option<BlockRef>,
        heads: L1Heads,
    ) -> Result<(), Stop> {
        let newest = |sent: Option<BlockRef>, new: Option<BlockRef>| {
            [sent, new]
                .into_iter()
                .flatten()
                .max_by_key(|head| head.number)
        };
        let merged = L1Heads {
            safe: newest(self.heads.safe, heads.safe),
            finalized: newest(self.heads.finalized, heads.finalized),
        };
        if merged == self.heads {
            return Ok(());
        }
        self.heads = merged;
        self.send_heads(unsafe_head).await
    }

    async fn send_heads(&mut self, unsafe_head: Option<BlockRef>) -> Result<(), Stop> {
        let heads = heads_message(unsafe_head, self.heads, self.receipts);
        self.send(proto::event::Event::Heads(heads)).await
    }

    /// Runs a conversion off the runtime; a block that does not convert ends the
    /// subscription.
    async fn convert<T: Send + 'static>(
        &mut self,
        convert: impl FnOnce() -> Result<T, crate::convert::ConvertError> + Send + 'static,
    ) -> Result<T, Stop> {
        match tokio::task::spawn_blocking(convert).await {
            Ok(Ok(converted)) => Ok(converted),
            Ok(Err(err)) => Err(Stop::Read(err.into())),
            Err(err) => {
                warn!(%err, "a stream conversion task failed; the subscription ends");
                self.sink
                    .end(Status::internal("the node failed to convert a block"));
                Err(Stop::Ended)
            }
        }
    }
}
