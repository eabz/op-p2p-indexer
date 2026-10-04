//! Block storage: an unsafe store for live blocks, a committed store for blocks on L1, and a
//! local archive of committed blocks for serving to peers.
//!
//! ```text
//! DecodedBlock ─▶ UnsafeStore (Redis: unsafe blocks, fork choice, events for readers)
//!               ├▶ CommittedStore (ClickHouse: blocks committed to L1 and backfill)
//!               └▶ ArchiveStore (fjall: a contiguous range of committed blocks, as RLP)
//! ```
//!
//! - [`UnsafeStore`], [`CommittedStore`] and [`ArchiveStore`] are the contracts;
//!   [`unsafe_store`], [`committed_store`] and [`archive_store`] hold the Redis, ClickHouse and
//!   fjall implementations.
//! - [`StorageConfig`] is plain data filled by the binary; [`StorageError`] classifies failures
//!   by [`Severity`]: transient, expected or fatal.
//! - [`metrics`] names and records every metric of the three stores.
//!
//! Takes blocks that are already decoded, with or without their receipts. Does not decode gossip
//! payloads, execute transactions, know about L1 or networking, or retry: moving blocks from
//! the unsafe store to the committed store and the archive, and retrying transient errors, is
//! the caller's job.

pub mod archive_store;
pub mod committed_store;
mod config;
mod error;
pub mod metrics;
pub mod unsafe_store;
mod validate;

use std::fmt;

use alloy_primitives::{BlockHash, BlockNumber, Bytes};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{
    ArchivedBlock, BlockRef, DecodedBlock, EncodedBlock, InsertOutcome, L1Heads,
};

pub use config::{ArchiveConfig, ArchiveRetention, ClickHouseConfig, RedisConfig, StorageConfig};
pub use error::{InvalidBlockReason, ParseError, Severity, StorageError};

/// One of the three stores, for errors and metric labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Store {
    /// The unsafe store (Redis).
    Unsafe,
    /// The committed store (ClickHouse).
    Committed,
    /// The local block archive (fjall).
    Archive,
}

impl Store {
    /// The store's backend: `redis`, `clickhouse` or `fjall`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsafe => "redis",
            Self::Committed => "clickhouse",
            Self::Archive => "fjall",
        }
    }
}

impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Live blocks not yet committed to L1, with fork choice.
///
/// Every remote call has a timeout and nothing is retried; some methods make several calls.
/// Every write is idempotent, so a caller may retry it. The returned futures are `Send`, so a
/// store can be driven from any task. Dropping a future never corrupts the store.
pub trait UnsafeStore {
    /// Stores a block and applies fork choice, atomically.
    ///
    /// A block that is already stored, or is at or below the safe head, is not stored again:
    /// the outcome has `stored = false` and no events.
    ///
    /// An insert that timed out may still have been applied. Retrying it is safe, but the
    /// retry then reports `stored = false` and no events: the events of the first attempt went
    /// to the event stream only. A caller that needs them re-reads [`Self::head`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] or [`StorageError::UnsupportedTransaction`] for a
    /// block the store cannot hold (nothing is stored then), and another [`StorageError`] if
    /// the store cannot be reached or the block cannot be encoded.
    fn insert(
        &self,
        block: &DecodedBlock,
    ) -> impl Future<Output = Result<InsertOutcome, StorageError>> + Send;

    /// Attaches receipts to a stored block. `Ok(false)` if the block is no longer stored.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] if the receipts do not match the stored block (not
    /// one per transaction, or `block` gives another number than the stored one), and another
    /// [`StorageError`] if the store cannot be reached or the receipts cannot be encoded.
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
    /// call, and another [`StorageError`] if the store cannot be reached or stored data cannot
    /// be decoded.
    ///
    /// # Cancel safety
    ///
    /// Reads only, in several calls under one overall deadline. A dropped future changes
    /// nothing; call it again.
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
    /// Returns [`StorageError::Timeout`] if the whole prune exceeds its deadline, and another
    /// [`StorageError`] if the store cannot be reached. Blocks removed before the error stay
    /// removed.
    ///
    /// # Cancel safety
    ///
    /// Removes in several steps within one call. A call that returned an error, or a future
    /// dropped part-way, leaves the remaining blocks stored; calling it again with the same
    /// `up_to` finishes the job.
    fn prune(&self, up_to: BlockRef) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Returns the unsafe head, or `None` if the store is empty.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached or stored data cannot be decoded.
    fn head(&self) -> impl Future<Output = Result<Option<BlockRef>, StorageError>> + Send;

    /// Returns the stored block with this hash, canonical or not.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached or stored data cannot be decoded.
    fn block(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<DecodedBlock>, StorageError>> + Send;

    /// Records the L1 safe and finalized heads.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached.
    fn set_l1_heads(&self, heads: L1Heads)
    -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// Blocks committed to L1, and historical backfill.
///
/// Every remote call has a timeout and nothing is retried; most methods make several calls.
/// Every write is idempotent, so a caller may retry it. The returned futures are `Send`, so a
/// store can be driven from any task. Dropping a future never corrupts the store.
pub trait CommittedStore {
    /// Inserts blocks with their transactions, receipts and logs. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached or rejects the rows.
    ///
    /// # Cancel safety
    ///
    /// Writes one table after another. A future dropped part-way may leave some tables without
    /// the batch; inserting the same blocks again completes it.
    fn insert(
        &self,
        blocks: &[DecodedBlock],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Deletes everything above `safe` (an L1 reorg moved the safe head back).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached.
    ///
    /// # Cancel safety
    ///
    /// Deletes from one table after another. A future dropped part-way leaves some tables
    /// with rows above `safe`; calling it again with the same `safe` finishes the job.
    fn rollback_to(&self, safe: BlockRef) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Returns the recorded L1 safe and finalized heads.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached or stored data cannot be decoded.
    fn l1_heads(&self) -> impl Future<Output = Result<L1Heads, StorageError>> + Send;

    /// Records the L1 safe and finalized heads.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the store cannot be reached.
    fn set_l1_heads(&self, heads: L1Heads)
    -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// A part of an archived block, for [`ArchiveStore::part`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockPart {
    /// The header.
    Header,
    /// The body.
    Body,
    /// The receipts.
    Receipts,
}

/// A local, contiguous window of recent committed blocks, kept in the encoding peers ask for.
///
/// The archive holds one range of blocks, each the parent of the next. Every write is one
/// fjall batch, applied entirely or not at all, so a crash or a dropped future leaves the range
/// contiguous. Nothing is retried. Each call runs on a blocking thread, so dropping its future
/// does not stop it: `append` and `set_receipts` still run to completion.
/// The returned futures are `Send`, so a store can be driven from any task.
pub trait ArchiveStore {
    /// Appends the next block. It must extend the held range (number = last + 1 and parent
    /// hash = last hash) unless the archive is empty. Appending the block already at the tip is
    /// a no-op. A block with receipts stores them at once.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::NotContiguous`] if the block does not extend the range,
    /// [`StorageError::InvalidBlock`] or [`StorageError::UnsupportedTransaction`] if it fails
    /// the shared validation or its header does not hash to its `hash`, and another
    /// [`StorageError`] if the archive cannot be written.
    fn append(&self, block: &DecodedBlock)
    -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Appends consecutive blocks, oldest first, in their original encoding. For blocks whose
    /// bytes the caller has verified (import, range sync); the bytes are stored unchanged.
    ///
    /// The archive checks what it can without decoding a body: each header hashes to its
    /// `hash`, each block is the child of the one before it, and the first one extends the held
    /// range unless the archive is empty. It does **not** check that the body and the receipts
    /// belong to the header: the caller must have verified the transactions root and the
    /// receipts root over exactly these bytes.
    ///
    /// Leading blocks the archive already holds, up to its tip, are skipped, so repeating a
    /// call is harmless. An empty list is a no-op.
    ///
    /// The whole list is checked before anything is written. It is then written in durable
    /// fjall batches of bounded size, each applied entirely or not at all. If a later batch
    /// fails, the earlier ones stay: [`range`](Self::range) tells where to resume.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] if a header does not hash to its `hash`,
    /// [`StorageError::InvalidData`] if a header is not a header, [`StorageError::NotContiguous`]
    /// if a block is not the child of the one before it or the list does not extend the held
    /// range, and another [`StorageError`] if the archive cannot be written.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future does not stop the call: it runs to its end on a blocking thread.
    fn append_batch(
        &self,
        blocks: Vec<EncodedBlock>,
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

    /// Returns the encoded block at `number`, or `None` outside the held range.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read.
    fn block(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<Option<ArchivedBlock>, StorageError>> + Send;

    /// Returns one part of the block at `number` as RLP, as [`block`](Self::block) gives it,
    /// reading only that part: `None` outside the held range, and for receipts not yet set.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the archive cannot be read.
    fn part(
        &self,
        number: BlockNumber,
        part: BlockPart,
    ) -> impl Future<Output = Result<Option<Bytes>, StorageError>> + Send;

    /// Returns the number of the archived block with this hash.
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

    /// Removes every block above `number` (an L1 reorg moved the safe head back).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Timeout`] if the removal exceeds its deadline, and another
    /// [`StorageError`] if the archive cannot be written.
    ///
    /// # Cancel safety
    ///
    /// Removes in bounded batches, newest first, so the range stays contiguous. Dropping the
    /// future does not stop the removal: it continues until it is done or reaches its deadline.
    /// Blocks removed before the deadline stay removed; calling it again finishes the job.
    fn truncate_above(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Removes the oldest blocks so at most `retain` remain. Returns how many were removed.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Timeout`] if the removal exceeds its deadline, and another
    /// [`StorageError`] if the archive cannot be written.
    ///
    /// # Cancel safety
    ///
    /// Removes in bounded batches, oldest first, so the range stays contiguous. Dropping the
    /// future does not stop the removal: it continues until it is done or reaches its deadline.
    /// Blocks removed before the deadline stay removed; calling it again finishes the job.
    fn trim(&self, retain: u64) -> impl Future<Output = Result<u64, StorageError>> + Send;
}
