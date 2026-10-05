//! Metrics of the two stores, emitted through the [`metrics`] facade.
//!
//! This module only records. It installs no recorder and serves no endpoint: until the binary
//! installs one every call here is a no-op, and [`describe`] must run after that. Call sites use
//! the typed helpers below, so metric names and labels live in this file only.
//!
//! Labels are low-cardinality by construction: store, operation and outcome. Block
//! numbers and hashes are never labels.
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `op_indexer_storage_operations_total` | counter | `store`, `operation`, `outcome` | Store operations. `outcome` is `ok`, or the error's [`Severity`]: `transient`, `expected` or `fatal`. |
//! | `op_indexer_storage_operation_duration_seconds` | histogram | `store`, `operation`, `outcome` | Time one store operation took. |
//! | `op_indexer_storage_blocks_inserted_total` | counter | `store` | Blocks written. In the unsafe store, blocks already stored are not counted. |
//! | `op_indexer_storage_reorgs_total` | counter | | Unsafe-store reorgs. |
//! | `op_indexer_storage_reorg_depth` | histogram | | Blocks replaced by one unsafe-store reorg. |
//! | `op_indexer_storage_receipts_attached_total` | counter | | Blocks that got receipts after they were stored in the unsafe store. |
//! | `op_indexer_storage_blocks_pruned_total` | counter | | Blocks removed from the unsafe store by pruning. |
//! | `op_indexer_storage_unsafe_bytes` | gauge | | Memory the unsafe chain's blocks take, roughly. |
//! | `op_indexer_storage_unsafe_blocks` | gauge | | Blocks the unsafe chain holds. |
//! | `op_indexer_storage_unsafe_evicted_blocks_total` | counter | | Unsafe blocks dropped by retention (a day behind the newest) or the memory cap. |
//! | `op_indexer_storage_retries_total` | counter | `store` | Store calls repeated by `retry` after a transient error. |
//! | `op_indexer_storage_archive_disk_bytes` | gauge | | Size of the archive directory, journal and blob files included. |
//! | `op_indexer_storage_archive_fragmented_blob_bytes` | gauge | | Stale bytes in the archive's blob files, which blob garbage collection will reclaim. |
//! | `op_indexer_storage_archive_active_compactions` | gauge | | Compactions running in the archive. |

use std::time::Instant;

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

use crate::{Severity, StorageError, Store};

const OPERATIONS: &str = "op_indexer_storage_operations_total";
const OPERATION_DURATION: &str = "op_indexer_storage_operation_duration_seconds";
const BLOCKS_INSERTED: &str = "op_indexer_storage_blocks_inserted_total";
const REORGS: &str = "op_indexer_storage_reorgs_total";
const REORG_DEPTH: &str = "op_indexer_storage_reorg_depth";
const RECEIPTS_ATTACHED: &str = "op_indexer_storage_receipts_attached_total";
const BLOCKS_PRUNED: &str = "op_indexer_storage_blocks_pruned_total";
const RETRIES: &str = "op_indexer_storage_retries_total";
const UNSAFE_BYTES: &str = "op_indexer_storage_unsafe_bytes";
const UNSAFE_BLOCKS: &str = "op_indexer_storage_unsafe_blocks";
const UNSAFE_EVICTED: &str = "op_indexer_storage_unsafe_evicted_blocks_total";
const ARCHIVE_DISK_BYTES: &str = "op_indexer_storage_archive_disk_bytes";
const ARCHIVE_FRAGMENTED_BLOB_BYTES: &str = "op_indexer_storage_archive_fragmented_blob_bytes";
const ARCHIVE_ACTIVE_COMPACTIONS: &str = "op_indexer_storage_archive_active_compactions";

/// A store operation, the `operation` label.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Operation {
    /// [`UnsafeStore::insert`](crate::UnsafeStore::insert).
    Insert,
    /// [`ArchiveStore::append_batch`](crate::ArchiveStore::append_batch).
    AppendBatch,
    /// [`ArchiveStore::number_of`](crate::ArchiveStore::number_of).
    NumberOf,
    /// [`ArchiveStore::range`](crate::ArchiveStore::range).
    Range,
    /// `set_receipts` of the unsafe store or the archive.
    SetReceipts,
    /// [`UnsafeStore::ancestry`](crate::UnsafeStore::ancestry).
    Ancestry,
    /// [`UnsafeStore::prune`](crate::UnsafeStore::prune).
    Prune,
    /// [`ArchiveStore::read`](crate::ArchiveStore::read).
    Read,
    /// [`ArchiveStore::blocks`](crate::ArchiveStore::blocks).
    Blocks,
    /// [`ArchiveStore::heads`](crate::ArchiveStore::heads).
    Heads,
    /// [`ArchiveStore::pending_receipts`](crate::ArchiveStore::pending_receipts).
    PendingReceipts,
    /// [`UnsafeStore::set_l1_heads`](crate::UnsafeStore::set_l1_heads) and
    /// [`ArchiveStore::set_heads`](crate::ArchiveStore::set_heads).
    SetL1Heads,
}

impl Operation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Read => "read",
            Self::AppendBatch => "append_batch",
            Self::NumberOf => "number_of",
            Self::Range => "range",
            Self::SetReceipts => "set_receipts",
            Self::Ancestry => "ancestry",
            Self::Prune => "prune",
            Self::Blocks => "blocks",
            Self::Heads => "heads",
            Self::PendingReceipts => "pending_receipts",
            Self::SetL1Heads => "set_l1_heads",
        }
    }
}

/// Registers the description and unit of every metric with the installed recorder.
///
/// A no-op without a recorder, so the binary calls this after installing one.
pub fn describe() {
    describe_counter!(OPERATIONS, Unit::Count, "Store operations, by outcome");
    describe_histogram!(
        OPERATION_DURATION,
        Unit::Seconds,
        "Time one store operation took"
    );
    describe_counter!(BLOCKS_INSERTED, Unit::Count, "Blocks written");
    describe_counter!(REORGS, Unit::Count, "Unsafe-store reorgs");
    describe_histogram!(
        REORG_DEPTH,
        Unit::Count,
        "Blocks replaced by one unsafe-store reorg"
    );
    describe_counter!(
        RECEIPTS_ATTACHED,
        Unit::Count,
        "Blocks that got receipts after they were stored in the unsafe store"
    );
    describe_counter!(
        BLOCKS_PRUNED,
        Unit::Count,
        "Blocks removed from the unsafe store by pruning"
    );
    describe_gauge!(
        UNSAFE_BYTES,
        Unit::Bytes,
        "Memory the unsafe chain's blocks take, roughly"
    );
    describe_gauge!(UNSAFE_BLOCKS, Unit::Count, "Blocks the unsafe chain holds");
    describe_counter!(
        UNSAFE_EVICTED,
        Unit::Count,
        "Unsafe blocks dropped by retention or the memory cap"
    );
    describe_counter!(
        RETRIES,
        Unit::Count,
        "Store calls repeated after a transient error, by store"
    );
    describe_gauge!(
        ARCHIVE_DISK_BYTES,
        Unit::Bytes,
        "Size of the archive directory, journal and blob files included"
    );
    describe_gauge!(
        ARCHIVE_FRAGMENTED_BLOB_BYTES,
        Unit::Bytes,
        "Stale bytes in the archive's blob files"
    );
    describe_gauge!(
        ARCHIVE_ACTIVE_COMPACTIONS,
        Unit::Count,
        "Compactions running in the archive"
    );
}

/// Runs one store operation and records its duration and outcome.
pub(crate) async fn timed<T>(
    store: Store,
    operation: Operation,
    call: impl Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    let started = Instant::now();
    let result = call.await;
    let operation = operation.as_str();
    let outcome = match result.as_ref().map_err(StorageError::severity) {
        Ok(_) => "ok",
        Err(Severity::Transient) => "transient",
        Err(Severity::Expected) => "expected",
        Err(Severity::Fatal) => "fatal",
    };
    histogram!(OPERATION_DURATION, "store" => store.as_str(), "operation" => operation, "outcome" => outcome)
        .record(started.elapsed());
    counter!(OPERATIONS, "store" => store.as_str(), "operation" => operation, "outcome" => outcome)
        .increment(1);
    result
}

/// Records blocks written to `store`.
pub(crate) fn blocks_inserted(store: Store, blocks: usize) {
    counter!(BLOCKS_INSERTED, "store" => store.as_str()).increment(count(blocks));
}

/// Records an unsafe-store reorg that replaced `depth` blocks.
pub(crate) fn reorg(depth: usize) {
    counter!(REORGS).increment(1);
    histogram!(REORG_DEPTH).record(f64::from(u32::try_from(depth).unwrap_or(u32::MAX)));
}

/// Records receipts attached to a block already in the unsafe store.
pub(crate) fn receipts_attached() {
    counter!(RECEIPTS_ATTACHED).increment(1);
}

/// Records blocks removed from the unsafe store by pruning.
pub(crate) fn blocks_pruned(blocks: usize) {
    counter!(BLOCKS_PRUNED).increment(count(blocks));
}

/// Sets what the unsafe chain holds.
pub(crate) fn unsafe_held(bytes: usize, blocks: usize) {
    gauge!(UNSAFE_BYTES).set(gauge_value(count(bytes)));
    gauge!(UNSAFE_BLOCKS).set(gauge_value(count(blocks)));
}

/// Records unsafe blocks dropped by retention or the memory cap.
pub(crate) fn unsafe_evicted(blocks: usize) {
    counter!(UNSAFE_EVICTED).increment(count(blocks));
}

/// Records a store call repeated after a transient error.
pub(crate) fn retried(store: Store) {
    counter!(RETRIES, "store" => store.as_str()).increment(1);
}

/// Sets the archive's size gauges, sampled from the engine after a write.
pub(crate) fn archive_usage(
    disk_bytes: u64,
    fragmented_blob_bytes: u64,
    active_compactions: usize,
) {
    gauge!(ARCHIVE_DISK_BYTES).set(gauge_value(disk_bytes));
    gauge!(ARCHIVE_FRAGMENTED_BLOB_BYTES).set(gauge_value(fragmented_blob_bytes));
    gauge!(ARCHIVE_ACTIVE_COMPACTIONS).set(gauge_value(count(active_compactions)));
}

/// Converts through the two 32-bit halves, because `f64::from(u32)` is lossless and needs no
/// cast suppression; the result is exact up to 2^53.
fn gauge_value(value: u64) -> f64 {
    const HIGH_UNIT: f64 = 4_294_967_296.0;
    let high = u32::try_from(value >> u32::BITS).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high).mul_add(HIGH_UNIT, f64::from(low))
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
