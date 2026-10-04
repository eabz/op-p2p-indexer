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

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::BlockNumber;
use op_indexer_primitives::{BlockRef, L1Heads};
use op_indexer_storage::{ArchiveStore, UnsafeStore};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::warn;

use crate::convert::{Payload, Prepared, heads_message};
use crate::follower::{CHAIN_WINDOW, ChainEvent, Live};
use crate::proto;
use crate::sink::{Ended, Sink};
use crate::source::{ReadError, Source, push_bounded, read_status};

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
    /// The blocks sent, oldest first, at most [`CHAIN_WINDOW`].
    sent: VecDeque<BlockRef>,
    /// Blocks sent without receipts.
    without_receipts: HashSet<BlockRef>,
    heads: L1Heads,
    /// The next height to read.
    next: BlockNumber,
    /// The archive's last block, as far as the reads have seen.
    archive_tip: Option<BlockNumber>,
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
    ) -> Self {
        Self {
            source,
            live,
            payload,
            sink,
            _permit: permit,
            receipts,
            sent: VecDeque::new(),
            without_receipts: HashSet::new(),
            heads: L1Heads::default(),
            next: 0,
            archive_tip: None,
            receipts_checked: Instant::now(),
        }
    }

    /// Runs until the consumer leaves, is too slow, or `cancel` fires.
    pub(crate) async fn run(mut self, start: Start, cancel: CancellationToken) {
        let stopped = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(Stop::Read(ReadError::Cancelled)),
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
            Start::Head => {
                let (next, last) = self.live.tail();
                self.sent.extend(last);
                self.next = last.map_or(0, |last| last.number.saturating_add(1));
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
            // Nothing to send yet: the follower's next publish may change that. A consumer
            // that leaves meanwhile ends it now, not at the next send.
            tokio::select! {
                changed = published.changed() => if changed.is_err() {
                    return Ok(());
                },
                () = self.sink.closed() => return Err(Stop::Ended),
            }
        }
    }

    /// Joins the window if it can, else reads and sends one batch. `None` when there is
    /// nothing to read yet.
    async fn read(&mut self) -> Result<Option<Mode>, Stop> {
        if let Some((unsafe_head, heads)) = self.live.heads()
            && heads != self.heads
        {
            self.heads = heads;
            self.send_heads(unsafe_head).await?;
        }
        if let Some(last) = self.sent.back()
            && let Some((cursor, receipts)) = self.live.join(*last)
        {
            for block in receipts {
                if self.without_receipts.remove(&block.at) {
                    self.send_receipts(block).await?;
                }
            }
            return Ok(Some(Mode::Follow(cursor)));
        }
        if self.receipts && self.receipts_checked.elapsed() >= RECEIPTS_RECHECK {
            self.receipts_checked = Instant::now();
            self.recheck_receipts().await?;
        }
        let blocks = self
            .source
            .blocks_from(self.next, &mut self.archive_tip)
            .await?;
        if blocks.is_empty() {
            // Above the follower's last block, nothing is there yet; below it, maybe never.
            let (_, last) = self.live.tail();
            if last.is_none_or(|last| self.next <= last.number)
                && let Err(status) = self.source.ensure_held(self.next).await?
            {
                self.sink.end(status);
                return Err(Stop::Ended);
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
            self.send_block(Arc::new(block)).await?;
        }
        Ok(Some(Mode::Read))
    }

    /// Sends the receipts the stores got for blocks it sent without them.
    async fn recheck_receipts(&mut self) -> Result<(), Stop> {
        let pending: Vec<BlockRef> = self.without_receipts.iter().copied().collect();
        for at in pending {
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
            return Ok(Some(Mode::Read));
        };
        let Some(next) = events.last().map(|(seq, _)| seq.saturating_add(1)) else {
            return Ok(None);
        };
        for (_, event) in events {
            match &*event {
                ChainEvent::Block(block) => {
                    if self
                        .sent
                        .back()
                        .is_some_and(|last| last.hash != block.parent)
                    {
                        return Ok(Some(Mode::Read));
                    }
                    self.send_block(Arc::clone(block)).await?;
                }
                ChainEvent::Receipts(block) => {
                    if self.without_receipts.remove(&block.at) {
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
                    self.heads = *heads;
                    self.send_heads(*unsafe_head).await?;
                }
            }
        }
        Ok(Some(Mode::Follow(next)))
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
        Ok(self.sink.send(event).await?)
    }

    async fn send_block(&mut self, block: Arc<Prepared>) -> Result<(), Stop> {
        let (payload, heads, at) = (self.payload, self.heads, block.at);
        let has_receipts = block.has_receipts();
        let message = self.convert(move || block.message(payload, &heads)).await?;
        self.send(proto::event::Event::Block(message)).await?;
        if !has_receipts {
            self.without_receipts.insert(at);
        }
        if let Some(dropped) = push_bounded(&mut self.sent, at, CHAIN_WINDOW) {
            self.without_receipts.remove(&dropped);
        }
        self.next = at.number.saturating_add(1);
        Ok(())
    }

    async fn send_receipts(&mut self, block: Arc<Prepared>) -> Result<(), Stop> {
        let payload = self.payload;
        if let Some(receipts) = self.convert(move || block.receipts(payload)).await? {
            self.send(proto::event::Event::Receipts(receipts)).await?;
        }
        Ok(())
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
