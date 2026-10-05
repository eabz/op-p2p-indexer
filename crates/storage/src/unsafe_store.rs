//! The unsafe store in memory: unsafe blocks, fork choice and reorgs, and the events readers
//! follow, in the node process, journaled to a small fjall database so a restart needs no
//! network (docs/storage.md section 3).
//!
//! - `chain`: the state and fork choice, the rules the Redis store had before it.
//! - `journal`: what is stored, written as it changes and replayed on open.
//! - Events go to an in-memory ring with sequence ids; readers wait on a watch of the newest.
//!
//! Blocks are held in their consensus encoding, as gossip and the archive carry them, and
//! decoded when read whole. Their transactions root, and receipts root once receipts are
//! attached, are checked when they are stored, so what is served is what the header commits
//! to. Blocks leave when the caller prunes, when they fall more than a day behind the newest,
//! or when the chain takes more memory than its cap (the lowest heights first); the store
//! never decides that a block is committed.

mod chain;
mod journal;
mod layout;

use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use alloy_consensus::Header;
use alloy_primitives::{BlockHash, BlockNumber};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{
    BlockRef, ChainIdentity, DecodedBlock, EncodedBlock, InsertOutcome, L1Heads, UnsafeEvent,
    decode_block, encode_receipts, receipts_root, split_body, transactions_root,
};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use self::chain::{Chain, Inserted, Stored};
use self::journal::{Changes, Journal};
use self::layout::{EVENTS_KEPT, MAX_ANCESTRY_BLOCKS};
use crate::metrics::{self, Operation};
use crate::validate::validate_block;
use crate::{
    BlockPart, CanonicalItem, EventId, Events, InvalidBlockReason, StorageError, Store,
    UnsafeConfig, UnsafeStore,
};

/// The unsafe chain of one chain. Cheap to clone: clones share it.
#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    journal: Journal,
    /// The chain's Canyon time, for receipts roots.
    canyon_time: u64,
    /// The memory the stored blocks may take.
    max_bytes: usize,
    /// The newest event's id, for readers waiting for one.
    newest_event: watch::Sender<EventId>,
}

struct State {
    chain: Chain,
    /// The newest events, oldest first, at most [`EVENTS_KEPT`].
    events: VecDeque<(EventId, UnsafeEvent)>,
    /// The id of the next event.
    next_event: u64,
    /// The insertion order of the next block, the journal's key.
    next_seq: u64,
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

impl MemoryStore {
    /// Opens the journal at `config.path` (created if needed) and replays it: every block it
    /// holds goes through fork choice again, in the order it was stored, which rebuilds the
    /// chain. Blocking: run it at startup or on a blocking thread.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::UnsafeChain`] if the journal records another chain, and another
    /// [`StorageError`] if it cannot be opened or read.
    pub fn open(config: &UnsafeConfig, chain: ChainIdentity) -> Result<Self, StorageError> {
        let started = Instant::now();
        let journal = Journal::open(&config.path, chain)?;
        let (heads, blocks) = journal.replay()?;
        let max_bytes = usize::try_from(config.max_bytes).unwrap_or(usize::MAX);
        let mut state = State {
            chain: Chain::default(),
            events: VecDeque::new(),
            next_event: 1,
            next_seq: 0,
        };
        state.chain.set_heads(heads);
        let replayed = blocks.len();
        let mut dropped = Vec::new();
        for block in blocks {
            state.next_seq = state.next_seq.max(block.seq.saturating_add(1));
            let seq = block.seq;
            // A block refused or not stored again (below the safe head) leaves the journal.
            if !matches!(state.chain.insert(block), Ok(Inserted::Stored(_))) {
                dropped.push(seq);
            }
        }
        dropped.extend(state.chain.retain(max_bytes));
        journal.write(&Changes {
            delete: dropped,
            ..Changes::default()
        })?;
        info!(
            blocks = state.chain.len(),
            replayed,
            bytes = state.chain.bytes(),
            head = ?state.chain.head(),
            millis = started.elapsed().as_millis(),
            "unsafe chain replayed from its journal"
        );
        metrics::unsafe_held(state.chain.bytes(), state.chain.len());
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(state),
                journal,
                canyon_time: config.canyon_time,
                max_bytes,
                newest_event: watch::Sender::new(EventId::START),
            }),
        })
    }

    /// Runs `write` on a blocking thread: it changes the chain under the lock and writes the
    /// journal there, so the journal follows the chain's order.
    async fn blocking<T: Send + 'static>(
        &self,
        operation: &'static str,
        write: impl FnOnce(&Inner) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || write(&inner))
            .await
            .map_err(|source| StorageError::BlockingTask { operation, source })?
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        // The state is never left half-changed by a panic: every change is a few map writes.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores `block`: see [`UnsafeStore::insert`]. Blocking.
    fn insert(&self, mut block: Stored) -> Result<InsertOutcome, StorageError> {
        let mut state = self.state();
        block.seq = state.next_seq;
        let (number, hash) = (block.number, block.encoded.hash);
        let events = match state.chain.insert(block) {
            Ok(Inserted::Skipped) => {
                return Ok(InsertOutcome {
                    stored: false,
                    events: Vec::new(),
                });
            }
            Ok(Inserted::Stored(events)) => events,
            Err(reason) => return Err(StorageError::InvalidBlock { number, reason }),
        };
        state.next_seq = state.next_seq.saturating_add(1);
        let evicted = state.chain.retain(self.max_bytes);
        if !evicted.is_empty() {
            metrics::unsafe_evicted(evicted.len());
            debug!(
                blocks = evicted.len(),
                "unsafe blocks left by retention or the memory cap"
            );
        }
        // Journaled once the chain took it; a write that fails loses the block on a restart
        // only, and the insert reports the error. Retention may have dropped it already.
        self.journal.write(&Changes {
            put: state.chain.get(&hash),
            delete: evicted,
            heads: None,
        })?;
        metrics::unsafe_held(state.chain.bytes(), state.chain.len());
        self.publish(&mut state, &events);
        Ok(InsertOutcome {
            stored: true,
            events,
        })
    }

    /// Appends `events` to the ring, oldest first, and wakes the readers once.
    fn publish(&self, state: &mut State, events: &[UnsafeEvent]) {
        for event in events {
            let id = EventId::new(state.next_event);
            state.next_event = state.next_event.saturating_add(1);
            state.events.push_back((id, event.clone()));
            if state.events.len() > EVENTS_KEPT {
                state.events.pop_front();
            }
        }
        if let Some((newest, _)) = state.events.back() {
            self.newest_event.send_replace(*newest);
        }
    }
}

impl UnsafeStore for MemoryStore {
    async fn insert(&self, block: &DecodedBlock) -> Result<InsertOutcome, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Insert, async {
            validate_block(block)?;
            let stored = stored(block, self.inner.canyon_time)?;
            let outcome = self.blocking("insert", move |inner| inner.insert(stored)).await?;
            if outcome.stored {
                metrics::blocks_inserted(Store::Unsafe, 1);
            }
            for event in &outcome.events {
                if let UnsafeEvent::Reorg(reorg) = event {
                    metrics::reorg(reorg.replaced.len());
                }
            }
            debug!(number = block.block.header.number, hash = %block.hash, stored = outcome.stored, events = ?outcome.events, "inserted block");
            Ok(outcome)
        })
        .await
    }

    async fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> Result<bool, StorageError> {
        metrics::timed(Store::Unsafe, Operation::SetReceipts, async {
            let canyon_time = self.inner.canyon_time;
            let receipts = receipts.to_vec();
            let attached = self
                .blocking("set_receipts", move |inner| {
                    let mut state = inner.state();
                    let invalid = |reason| StorageError::InvalidBlock {
                        number: block.number,
                        reason,
                    };
                    let Some(stored) = state.chain.get(&block.hash) else {
                        return Ok(false);
                    };
                    if stored.number != block.number {
                        return Err(invalid(InvalidBlockReason::StoredNumber));
                    }
                    if stored.senders.len() != receipts.len() {
                        return Err(invalid(InvalidBlockReason::ReceiptCount));
                    }
                    let header = header(stored)?;
                    if receipts_root(&receipts, header.timestamp, canyon_time)
                        != header.receipts_root
                    {
                        return Err(invalid(InvalidBlockReason::ReceiptsRoot));
                    }
                    let event = state
                        .chain
                        .set_receipts(block, encode_receipts(&receipts))
                        .map_err(invalid)?;
                    if let Some(stored) = state.chain.get(&block.hash) {
                        inner.journal.write(&Changes {
                            put: Some(stored),
                            ..Changes::default()
                        })?;
                    }
                    metrics::unsafe_held(state.chain.bytes(), state.chain.len());
                    inner.publish(&mut state, event.as_slice());
                    Ok(event.is_some())
                })
                .await?;
            if attached {
                metrics::receipts_attached();
            }
            Ok(attached)
        })
        .await
    }

    async fn ancestry(
        &self,
        head: BlockRef,
        stop_at: BlockNumber,
    ) -> Result<Vec<DecodedBlock>, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Ancestry, async {
            let requested = head.number.saturating_sub(stop_at);
            if requested > MAX_ANCESTRY_BLOCKS {
                return Err(StorageError::AncestryTooLong {
                    requested,
                    max: MAX_ANCESTRY_BLOCKS,
                });
            }
            let blocks = {
                let state = self.inner.state();
                let mut blocks = Vec::new();
                let mut next = head;
                while next.number > stop_at {
                    let block = state
                        .chain
                        .get(&next.hash)
                        .filter(|block| block.number == next.number)
                        .ok_or(StorageError::MissingAncestor {
                            hash: next.hash,
                            number: next.number,
                        })?;
                    next = BlockRef {
                        // Above `stop_at`, so at least 1.
                        number: next.number.saturating_sub(1),
                        hash: block.parent_hash,
                    };
                    blocks.push(block.clone());
                }
                blocks
            };
            // Decoding is CPU work: off the runtime.
            let decoded = self
                .blocking("ancestry", move |_inner| {
                    blocks.into_iter().rev().map(decode).collect()
                })
                .await?;
            Ok(decoded)
        })
        .await
    }

    async fn prune(&self, up_to: BlockRef) -> Result<(), StorageError> {
        metrics::timed(Store::Unsafe, Operation::Prune, async {
            let removed = self
                .blocking("prune", move |inner| {
                    let mut state = inner.state();
                    let removed = state.chain.prune(up_to.number);
                    inner.journal.write(&Changes {
                        delete: removed.clone(),
                        ..Changes::default()
                    })?;
                    metrics::unsafe_held(state.chain.bytes(), state.chain.len());
                    inner.publish(&mut state, &[UnsafeEvent::Pruned { up_to }]);
                    Ok(removed.len())
                })
                .await?;
            metrics::blocks_pruned(removed);
            Ok(())
        })
        .await
    }

    fn head(&self) -> impl Future<Output = Result<Option<BlockRef>, StorageError>> + Send {
        ready(Ok(self.inner.state().chain.head()))
    }

    fn lowest(&self) -> impl Future<Output = Result<Option<BlockNumber>, StorageError>> + Send {
        ready(Ok(self.inner.state().chain.lowest()))
    }

    fn block(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<DecodedBlock>, StorageError>> + Send {
        let block = self.inner.state().chain.get(&hash).cloned();
        ready(block.map(decode).transpose())
    }

    fn canonical(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<Option<DecodedBlock>, StorageError>> + Send {
        ready({
            let block = {
                let state = self.inner.state();
                let chain = &state.chain;
                chain
                    .canonical_at(number)
                    .and_then(|hash| chain.get(&hash))
                    .cloned()
            };
            block.map(decode).transpose()
        })
    }

    fn canonical_number(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<BlockNumber>, StorageError>> + Send {
        ready({
            let state = self.inner.state();
            Ok(state.chain.canonical_block(&hash).map(|block| block.number))
        })
    }

    fn canonical_headers(
        &self,
        from: BlockNumber,
        count: usize,
        rising: bool,
    ) -> impl Future<Output = Result<Vec<CanonicalItem>, StorageError>> + Send {
        ready({
            let state = self.inner.state();
            let chain = &state.chain;
            let mut items: Vec<CanonicalItem> = Vec::new();
            let mut expected = Some(from);
            for (number, hash) in chain.canonical_from(from, rising).take(count) {
                let Some(block) = chain.get(&hash).filter(|_| Some(number) == expected) else {
                    break;
                };
                let links = items.last().is_none_or(|previous| {
                    if rising {
                        block.parent_hash == previous.block.hash
                    } else {
                        previous.parent_hash == hash
                    }
                });
                if !links {
                    break;
                }
                items.push(CanonicalItem {
                    block: BlockRef { number, hash },
                    parent_hash: block.parent_hash,
                    rlp: block.encoded.header.clone(),
                });
                expected = if rising {
                    number.checked_add(1)
                } else {
                    number.checked_sub(1)
                };
            }
            Ok(items)
        })
    }

    fn canonical_items(
        &self,
        hashes: &[BlockHash],
        part: BlockPart,
    ) -> impl Future<Output = Result<Vec<CanonicalItem>, StorageError>> + Send {
        ready({
            let state = self.inner.state();
            let mut items: Vec<CanonicalItem> = Vec::with_capacity(hashes.len());
            for hash in hashes {
                let Some(block) = state.chain.canonical_block(hash) else {
                    break;
                };
                let follows = items
                    .last()
                    .filter(|previous| previous.block.number.checked_add(1) == Some(block.number));
                if follows.is_some_and(|previous| previous.block.hash != block.parent_hash) {
                    break;
                }
                // Checked against the header's roots when stored.
                let rlp = match part {
                    BlockPart::Body => Some(&block.encoded.body),
                    BlockPart::Receipts => block.encoded.receipts.as_ref(),
                };
                let Some(rlp) = rlp else { break };
                items.push(CanonicalItem {
                    block: BlockRef {
                        number: block.number,
                        hash: *hash,
                    },
                    parent_hash: block.parent_hash,
                    rlp: rlp.clone(),
                });
            }
            Ok(items)
        })
    }

    fn canonical_run(
        &self,
        above: BlockRef,
        max: usize,
    ) -> impl Future<Output = Result<Option<BlockRef>, StorageError>> + Send {
        ready(Ok(self.inner.state().chain.canonical_run(above, max)))
    }

    /// A `None` head is unknown, not absent: the one recorded stays.
    async fn set_l1_heads(&self, heads: L1Heads) -> Result<(), StorageError> {
        metrics::timed(Store::Unsafe, Operation::SetL1Heads, async {
            self.blocking("set_l1_heads", move |inner| {
                let mut state = inner.state();
                state.chain.set_heads(heads);
                inner.journal.write(&Changes {
                    heads: Some(state.chain.heads()),
                    ..Changes::default()
                })
            })
            .await
        })
        .await
    }

    fn last_event_id(&self) -> impl Future<Output = Result<EventId, StorageError>> + Send {
        ready(Ok(*self.inner.newest_event.borrow()))
    }

    async fn events(
        &self,
        after: EventId,
        count: usize,
        block_for: Duration,
    ) -> Result<Events, StorageError> {
        let mut newest = self.inner.newest_event.subscribe();
        if !block_for.is_zero() {
            // Nothing came within the wait: no events. The watch's sender lives with the store.
            let _came = tokio::time::timeout(block_for, newest.wait_for(|id| *id > after)).await;
        }
        let state = self.inner.state();
        // Events after `after` were dropped exactly when the oldest one kept is further on.
        let missed = after != EventId::START
            && state
                .events
                .front()
                .is_some_and(|(oldest, _)| oldest.get() > after.get().saturating_add(1));
        // Ids are consecutive, so the first event after `after` is found by its offset.
        let skip = state.events.front().map_or(0, |(oldest, _)| {
            usize::try_from(after.get().saturating_add(1).saturating_sub(oldest.get()))
                .unwrap_or(usize::MAX)
        });
        let events = state
            .events
            .iter()
            .skip(skip)
            .take(count.max(1))
            .cloned()
            .collect();
        Ok(Events { events, missed })
    }
}

/// The block as the chain holds it, its transactions root, and its receipts root when it has
/// receipts, checked against its header.
fn stored(block: &DecodedBlock, canyon_time: u64) -> Result<Stored, StorageError> {
    let header = &block.block.header;
    let invalid = |reason| StorageError::InvalidBlock {
        number: header.number,
        reason,
    };
    let encoded = EncodedBlock::from(block);
    let parts = split_body(&encoded.body).ok_or_else(|| invalid(InvalidBlockReason::Ommers))?;
    if transactions_root(&parts.transactions) != header.transactions_root {
        warn!(number = header.number, hash = %block.hash, "a block's transactions do not encode to its header's root");
        return Err(invalid(InvalidBlockReason::TransactionsRoot));
    }
    if let Some(receipts) = &block.receipts
        && receipts_root(receipts, header.timestamp, canyon_time) != header.receipts_root
    {
        return Err(invalid(InvalidBlockReason::ReceiptsRoot));
    }
    Ok(Stored {
        number: header.number,
        parent_hash: header.parent_hash,
        timestamp: header.timestamp,
        encoded,
        senders: block.senders.clone(),
        source: block.source,
        seq: 0,
    })
}

/// The stored block's header.
fn header(block: &Stored) -> Result<Header, StorageError> {
    alloy_rlp::decode_exact(&block.encoded.header).map_err(|_err| damaged(block))
}

/// The stored block, decoded.
fn decode(block: Stored) -> Result<DecodedBlock, StorageError> {
    let (decoded, receipts) = decode_block(&block.encoded).map_err(|_err| damaged(&block))?;
    Ok(DecodedBlock {
        block: decoded,
        hash: block.encoded.hash,
        senders: block.senders,
        receipts,
        source: block.source,
    })
}

const fn damaged(block: &Stored) -> StorageError {
    StorageError::InvalidData {
        store: Store::Unsafe,
        what: "block",
        block: Some(block.encoded.hash),
        source: None,
    }
}
