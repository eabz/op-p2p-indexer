//! Block storage: an unsafe store for live blocks, and the archive of blocks committed to L1.
//!
//! ```text
//! DecodedBlock ─▶ UnsafeStore (in memory, journaled to fjall: unsafe blocks, fork choice,
//!                 events for readers)
//! ArchivedBlock ─▶ ArchiveStore (fjall: the committed store, a contiguous range of committed
//!                  blocks as RLP with their senders, and the committed L1 heads)
//! ```
//!
//! - [`UnsafeStore`] and [`ArchiveStore`] are the contracts; [`unsafe_store`] and
//!   [`archive_store`] hold the in-memory and fjall implementations.
//! - [`StorageConfig`] is plain data filled by the binary; [`StorageError`] classifies failures
//!   by [`Severity`]: transient, expected or fatal.
//! - [`metrics`] names and records every metric of the two stores.
//!
//! Takes blocks that are already decoded or verified, with or without their receipts. Does not
//! decode gossip payloads, execute transactions, know about L1 or networking, or retry: moving
//! blocks from the unsafe store to the archive, and retrying transient errors, is the caller's
//! job.

pub mod archive_store;
mod config;
mod error;
pub mod metrics;
mod retry;
pub mod unsafe_store;
mod validate;

use std::fmt;
use std::time::Duration;

use alloy_primitives::{BlockHash, BlockNumber, Bytes};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{
    ArchivedBlock, BlockRead, BlockRef, DecodedBlock, InsertOutcome, ItemConvert, L1Heads,
    ReadLimits, UnsafeEvent,
};

pub use config::{ArchiveConfig, StorageConfig, UnsafeConfig};
pub use error::{InvalidBlockReason, ParseError, Severity, StorageError};
pub use retry::{RetryError, retry};

/// One of the two stores, for errors and metric labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Store {
    /// The unsafe store (in memory).
    Unsafe,
    /// The local block archive (fjall), the committed store.
    Archive,
    /// The remote archive of sealed chunks (R2), read by a server.
    R2,
}

impl Store {
    /// The store's name: `unsafe`, `archive` or `r2`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsafe => "unsafe",
            Self::Archive => "archive",
            Self::R2 => "r2",
        }
    }
}

impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A position in the unsafe store's events: a sequence number, ordered as the events are;
/// [`EventId::START`] is before every event. It restarts with the process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(u64);

impl EventId {
    /// Before every event.
    pub const START: Self = Self(0);

    pub(crate) const fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    /// The sequence number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A part of a canonical unsafe block in its consensus encoding, with the block it belongs to
/// and that block's parent, so a reader can check that a run links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalItem {
    /// The block.
    pub block: BlockRef,
    /// Its parent's hash.
    pub parent_hash: BlockHash,
    /// The header, the body or the receipts (an RLP list of network encodings with bloom).
    pub rlp: Bytes,
}

/// Which part of a block [`UnsafeStore::canonical_items`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockPart {
    /// The body: transactions, no ommers, and withdrawals when the header has their root.
    Body,
    /// The receipts.
    Receipts,
}

/// What [`UnsafeStore::events`] read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Events {
    /// The events after the position asked for, oldest first.
    pub events: Vec<(EventId, UnsafeEvent)>,
    /// Events after the position asked for were dropped (the store keeps the newest ten
    /// thousand): the reader must read the state again. Never set for [`EventId::START`].
    pub missed: bool,
}

/// Live blocks not yet committed to L1, with fork choice.
///
/// Nothing is retried. Every write is idempotent, so a caller may retry it. The returned
/// futures are `Send`, so a store can be driven from any task. Dropping a future never corrupts
/// the store.
pub trait UnsafeStore {
    /// Stores a block and applies fork choice, atomically.
    ///
    /// A block that is already stored, or is at or below the safe head, is not stored again:
    /// the outcome has `stored = false` and no events.
    ///
    /// An insert that failed after it was applied (its journal write failed) is safe to retry,
    /// but the retry then reports `stored = false` and no events: the events of the first
    /// attempt went to the readers only. A caller that needs them re-reads [`Self::head`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] or [`StorageError::UnsupportedTransaction`] for a
    /// block the store cannot hold (nothing is stored then), and another [`StorageError`] if
    /// the journal cannot be written.
    fn insert(
        &self,
        block: &DecodedBlock,
    ) -> impl Future<Output = Result<InsertOutcome, StorageError>> + Send;

    /// Attaches receipts to a stored block. `Ok(false)` if the block is no longer stored.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] if the receipts do not match the stored block (not
    /// one per transaction, not its header's receipts root, or `block` gives another number
    /// than the stored one), and another [`StorageError`] if the journal cannot be written.
    fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;

    /// Returns the ancestry of `head` back to (excluding) height `stop_at`, oldest first, by
    /// parent links. The range is complete or the call fails; it is never partial.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::MissingAncestor`] if a block in the range is no longer stored,
    /// [`StorageError::AncestryTooLong`] if the range is longer than the store returns in one
    /// call, and another [`StorageError`] if stored data cannot be decoded.
    ///
    /// # Cancel safety
    ///
    /// Reads only. A dropped future changes nothing; call it again.
    fn ancestry(
        &self,
        head: BlockRef,
        stop_at: BlockNumber,
    ) -> impl Future<Output = Result<Vec<DecodedBlock>, StorageError>> + Send;

    /// Removes every block at or below `up_to`, canonical and side blocks, and publishes a
    /// `pruned` event to readers.
    ///
    /// Call [`Self::set_l1_heads`] first, so the store knows the safe head before the blocks
    /// below it disappear.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the journal cannot be written; the blocks are gone from
    /// memory anyway, and a restart removes them again.
    ///
    /// # Cancel safety
    ///
    /// The prune runs to its end once started, on a blocking thread.
    fn prune(&self, up_to: BlockRef) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Returns the unsafe head, or `None` if the store is empty.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if stored data cannot be decoded.
    fn head(&self) -> impl Future<Output = Result<Option<BlockRef>, StorageError>> + Send;

    /// Returns the lowest height with a canonical block, or `None` if the store is empty:
    /// nothing below it is held.
    ///
    /// # Errors
    ///
    /// Never fails in memory; the error is the trait's.
    fn lowest(&self) -> impl Future<Output = Result<Option<BlockNumber>, StorageError>> + Send;

    /// Returns the stored block with this hash, canonical or not.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if stored data cannot be decoded.
    fn block(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<DecodedBlock>, StorageError>> + Send;

    /// Returns the canonical block at height `number`, or `None` if no canonical block is
    /// stored there: it is below what the store still holds, above the head, or in a gap.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if stored data cannot be decoded.
    fn canonical(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<Option<DecodedBlock>, StorageError>> + Send;

    /// Returns the height of the block with `hash` if it is canonical, without reading it.
    ///
    /// # Errors
    ///
    /// Never fails in memory; the error is the trait's.
    fn canonical_number(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<BlockNumber>, StorageError>> + Send;

    /// Returns up to `count` consecutive canonical headers from height `from`, rising or
    /// falling, each in its consensus encoding (RLP). The run ends at the first height with no
    /// canonical block, a block no longer stored, or a block that does not link by parent hash
    /// to the one before.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if stored data cannot be decoded.
    fn canonical_headers(
        &self,
        from: BlockNumber,
        count: usize,
        rising: bool,
    ) -> impl Future<Output = Result<Vec<CanonicalItem>, StorageError>> + Send;

    /// Returns the bodies or receipts of the leading blocks of `hashes` that are canonical and
    /// stored (with their receipts, for [`BlockPart::Receipts`]), each in its consensus
    /// encoding, in order. The run ends at the first that is not, or that follows the one
    /// before by height without naming it as its parent. Each was checked against its
    /// header's roots when it was stored.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if stored data cannot be decoded.
    fn canonical_items(
        &self,
        hashes: &[BlockHash],
        part: BlockPart,
    ) -> impl Future<Output = Result<Vec<CanonicalItem>, StorageError>> + Send;

    /// Returns the last block of the unbroken canonical run that continues `above` and has its
    /// receipts: the canonical block at `above.number + 1` must name `above` as its parent, and
    /// the run ends before the first height with no canonical block or whose block has no
    /// receipts yet. At most `max` heights are looked at. `None` if no block qualifies.
    ///
    /// The canonical chain is linked by parent hash wherever its heights are unbroken (fork
    /// choice keeps it so), so the run is one chain, every block of it with its receipts.
    ///
    /// # Errors
    ///
    /// Never fails in memory; the error is the trait's.
    fn canonical_run(
        &self,
        above: BlockRef,
        max: usize,
    ) -> impl Future<Output = Result<Option<BlockRef>, StorageError>> + Send;

    /// Records the L1 safe and finalized heads.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the journal cannot be written.
    fn set_l1_heads(&self, heads: L1Heads)
    -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Returns the id of the newest event, or [`EventId::START`] if there is none yet:
    /// where a reader that has just read the store's state starts following [`Self::events`].
    ///
    /// # Errors
    ///
    /// Never fails in memory; the error is the trait's.
    fn last_event_id(&self) -> impl Future<Output = Result<EventId, StorageError>> + Send;

    /// Returns up to `count` events after `after`, oldest first, every write's events in the
    /// order the writes were applied (docs/storage.md section 3.3). Waits up to `block_for` for
    /// the first one when there is none yet (not at all when it is zero); empty if none came.
    ///
    /// A wait holds up nothing else: readers wait on their own.
    ///
    /// # Errors
    ///
    /// Never fails in memory; the error is the trait's.
    ///
    /// # Cancel safety
    ///
    /// Reads only. A dropped future loses nothing: call it again with the same `after`.
    fn events(
        &self,
        after: EventId,
        count: usize,
        block_for: Duration,
    ) -> impl Future<Output = Result<Events, StorageError>> + Send;
}

/// The committed store: a local, contiguous range of committed blocks (all of them, or a
/// window of the newest), kept in the encoding peers ask for, with their senders, and the
/// committed L1 heads.
///
/// The archive holds one range of blocks, each the parent of the next. Every write is one
/// fjall batch, applied entirely or not at all, so a crash or a dropped future leaves the range
/// contiguous. Nothing is retried. Each call runs on a blocking thread, so dropping its future
/// does not stop it: `append_batch` and `set_receipts` still run to completion.
/// The returned futures are `Send`, so a store can be driven from any task.
pub trait ArchiveStore {
    /// Appends consecutive blocks, oldest first, in their original encoding with their
    /// senders. The bytes are stored unchanged, so they must be bytes the caller has verified
    /// (range sync) or encoded from a verified block that survives the round trip (promoted
    /// gossip blocks), and the senders must be the ones recovered from them. A block with
    /// receipts stores them at once.
    ///
    /// The archive checks what it can without decoding a body: each header hashes to its `hash`,
    /// there is one sender per transaction, each block is the child of the one before it, and the
    /// first one extends the held range unless the archive is empty. It does **not** check that the
    /// body and the receipts belong to the header: the caller must have verified the transactions
    /// root and the receipts root over exactly these bytes.
    ///
    /// Blocks the archive already holds are skipped, so repeating a call is harmless: the
    /// leading blocks up to the tip when the tip is among them, and the whole list when it
    /// ends at or below the tip with its last block held. An empty list is a no-op.
    ///
    /// The whole list is checked before anything is written. It is then written in durable
    /// fjall batches of bounded size, each applied entirely or not at all. If a later batch
    /// fails, the earlier ones stay: [`range`](Self::range) tells where to resume.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] if a header does not hash to its `hash` or the
    /// senders are not one per transaction, [`StorageError::InvalidData`] if a header is not a
    /// header or a body not a body, [`StorageError::NotContiguous`]
    /// if a block is not the child of the one before it or the list does not extend the held
    /// range, and another [`StorageError`] if the archive cannot be written.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future does not stop the call: it runs to its end on a blocking thread.
    fn append_batch(
        &self,
        blocks: Vec<ArchivedBlock>,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Attaches receipts to an archived block. `Ok(false)` if it is not archived.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] if the receipts are not one per transaction or
    /// `block` gives another number than the archived one, and another [`StorageError`] if the
    /// archive cannot be written.
    fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;

    /// Reads a run of headers, bodies or receipts, each as the RLP the archive holds, in one
    /// call on one snapshot: what answers a peer's request. The run ends at the first block
    /// that is not held (or whose receipts are not set), and at `limits`.
    ///
    /// With `convert`, each item is passed through it before it counts against the limits
    /// and is returned; an item it returns `None` for ends the run. It runs on the blocking
    /// thread of the read.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read.
    fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> impl Future<Output = Result<Vec<Bytes>, StorageError>> + Send;

    /// Reads whole blocks (header, body, receipts if set, senders) from `from` upwards, in one
    /// call on one snapshot: what a reader of the committed chain streams. The run ends at the
    /// first block that is not held, and at `limits` (header, body and receipt bytes count);
    /// it is empty if `from` is not held. A block by hash is [`Self::number_of`], then this.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read or holds a block only in part.
    fn blocks(
        &self,
        from: BlockNumber,
        limits: ReadLimits,
    ) -> impl Future<Output = Result<Vec<ArchivedBlock>, StorageError>> + Send;

    /// Returns the committed L1 safe and finalized heads, as [`Self::set_heads`] recorded
    /// them; `None` for a head never recorded.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read or a head does not decode.
    fn heads(&self) -> impl Future<Output = Result<L1Heads, StorageError>> + Send;

    /// Records the committed L1 safe and finalized heads, durably and at once; a `None` head
    /// leaves the recorded one in place. Promotion's marker that the blocks up to the safe
    /// head are committed.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be written.
    fn set_heads(&self, heads: L1Heads) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Returns the archived blocks without receipts, at most `limit`, and how many there are
    /// in all: in block order from `from`, then from the lowest (wrapping round), so a caller
    /// that continues after the last block it got works through all of them in turn. Every
    /// block appended without receipts is listed, and leaves the list when
    /// [`Self::set_receipts`] fills it: in practice the few blocks promoted before their
    /// receipts arrived (an import always has them).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read.
    fn pending_receipts(
        &self,
        from: BlockNumber,
        limit: usize,
    ) -> impl Future<Output = Result<(Vec<BlockRef>, u64), StorageError>> + Send;

    /// Returns the number of the archived block with this hash: of a block the archive holds,
    /// never of what an earlier importer's interrupted bulk load left above the tip.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read.
    fn number_of(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<BlockNumber>, StorageError>> + Send;

    /// Returns the first and last archived block, or `None` if the archive is empty.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read.
    fn range(
        &self,
    ) -> impl Future<Output = Result<Option<(BlockRef, BlockRef)>, StorageError>> + Send;
}
