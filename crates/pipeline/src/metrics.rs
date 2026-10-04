//! Metrics of the pipeline, emitted through the [`metrics`] facade.
//!
//! This module only records. It installs no recorder and serves no endpoint: until the binary
//! installs one every call here is a no-op, and [`describe`] must run after that. Call sites use
//! the typed helpers below, so metric names and labels live in this file only. The stores'
//! own metrics (operations, durations, rows) are in `op_indexer_storage::metrics`.
//!
//! Labels are low-cardinality by construction: a drop reason, a hole reason or a store. Block numbers and
//! hashes are never labels.
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `op_indexer_pipeline_blocks_ingested_total` | counter | | Gossip blocks newly stored in the unsafe store. |
//! | `op_indexer_pipeline_blocks_dropped_total` | counter | `reason` | Gossip blocks not stored: `sender_recovery`, `invalid` or `unsupported`. |
//! | `op_indexer_pipeline_reorgs_total` | counter | | Unsafe-chain reorgs reported by an insert. |
//! | `op_indexer_pipeline_reorg_depth` | histogram | | Blocks replaced by one unsafe-chain reorg. |
//! | `op_indexer_pipeline_blocks_filled_total` | counter | | Blocks below the head that became canonical: gaps repaired. |
//! | `op_indexer_pipeline_retries_total` | counter | `store` | Store calls repeated after a transient error. |
//! | `op_indexer_pipeline_ingest_lag_seconds` | histogram | | Time from a block's timestamp to its insert. |
//! | `op_indexer_pipeline_channel_depth` | gauge | | Blocks waiting in the channel from the network. |
//! | `op_indexer_pipeline_blocks_promoted_total` | counter | | Blocks written to the committed store by promotion. |
//! | `op_indexer_pipeline_promotion_holes_total` | counter | `reason` | Promotions that could not read their whole range from the unsafe store: `missing_ancestor`, `too_long` (the blocks may exist) or `parent_mismatch` (nothing left out, but the range is another chain than the committed safe head). |
//! | `op_indexer_pipeline_promotion_blocks_missing_total` | counter | `reason` | Blocks those promotions left out of the committed store, for backfill. |
//! | `op_indexer_pipeline_l1_reorgs_total` | counter | | Times the safe head moved back or changed hash, rolling the committed store back. |
//! | `op_indexer_pipeline_archive_restarts_total` | counter | | Times the archive was emptied because a promoted block did not extend it. |
//! | `op_indexer_pipeline_safe_block_number` | gauge | | Number of the committed safe head. |

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use op_indexer_storage::Store;

const BLOCKS_INGESTED: &str = "op_indexer_pipeline_blocks_ingested_total";
const BLOCKS_DROPPED: &str = "op_indexer_pipeline_blocks_dropped_total";
const REORGS: &str = "op_indexer_pipeline_reorgs_total";
const REORG_DEPTH: &str = "op_indexer_pipeline_reorg_depth";
const BLOCKS_FILLED: &str = "op_indexer_pipeline_blocks_filled_total";
const RETRIES: &str = "op_indexer_pipeline_retries_total";
const INGEST_LAG: &str = "op_indexer_pipeline_ingest_lag_seconds";
const CHANNEL_DEPTH: &str = "op_indexer_pipeline_channel_depth";
const BLOCKS_PROMOTED: &str = "op_indexer_pipeline_blocks_promoted_total";
const PROMOTION_HOLES: &str = "op_indexer_pipeline_promotion_holes_total";
const PROMOTION_BLOCKS_MISSING: &str = "op_indexer_pipeline_promotion_blocks_missing_total";
const L1_REORGS: &str = "op_indexer_pipeline_l1_reorgs_total";
const ARCHIVE_RESTARTS: &str = "op_indexer_pipeline_archive_restarts_total";
const SAFE_BLOCK_NUMBER: &str = "op_indexer_pipeline_safe_block_number";
const RECEIPT_REQUESTS: &str = "op_indexer_pipeline_receipt_requests_total";
const RECEIPTS_ATTACHED: &str = "op_indexer_pipeline_receipts_attached_total";
const RECEIPTS_UNMATCHED: &str = "op_indexer_pipeline_receipts_unmatched_total";

/// Why ingest did not store a block, the `reason` label.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DropReason {
    /// A transaction's sender could not be recovered.
    SenderRecovery,
    /// The stores cannot hold the block (`StorageError::InvalidBlock`).
    Invalid,
    /// The block has a transaction type the stores have no place for.
    Unsupported,
}

/// Why a promotion could not read its whole range, the `reason` label.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HoleReason {
    /// A block of the range is not in the unsafe store: never received, or expired.
    MissingAncestor,
    /// The range is longer than one ancestry call returns. The blocks may all be stored.
    TooLong,
    /// The oldest block of the range does not build on the committed safe head.
    ParentMismatch,
}

impl DropReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SenderRecovery => "sender_recovery",
            Self::Invalid => "invalid",
            Self::Unsupported => "unsupported",
        }
    }
}

impl HoleReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MissingAncestor => "missing_ancestor",
            Self::TooLong => "too_long",
            Self::ParentMismatch => "parent_mismatch",
        }
    }
}

/// What happened to a request for a block's receipts.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RequestOutcome {
    /// Handed to the fetcher.
    Sent,
    /// Dropped: the fetcher's channel was full or closed.
    Dropped,
}

impl RequestOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sent => "sent",
            Self::Dropped => "dropped",
        }
    }
}

/// Why verified receipts were not attached to a block.
#[derive(Debug, Clone, Copy)]
pub(crate) enum UnmatchedReason {
    /// No store holds the block any more.
    UnknownBlock,
    /// The store holding the block refused them: wrong count or wrong number.
    Refused,
}

impl UnmatchedReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownBlock => "unknown_block",
            Self::Refused => "refused",
        }
    }
}

/// Registers the description and unit of every metric with the installed recorder.
///
/// A no-op without a recorder, so the binary calls this after installing one.
pub fn describe() {
    describe_counter!(
        RECEIPT_REQUESTS,
        Unit::Count,
        "Requests for a block's receipts, by outcome"
    );
    describe_counter!(
        RECEIPTS_ATTACHED,
        Unit::Count,
        "Blocks that got their receipts, by the store that held the block; in the archive the \
         committed store is not updated"
    );
    describe_counter!(
        RECEIPTS_UNMATCHED,
        Unit::Count,
        "Verified receipts that were not attached, by reason"
    );
    describe_counter!(
        BLOCKS_INGESTED,
        Unit::Count,
        "Gossip blocks stored in the unsafe store"
    );
    describe_counter!(
        BLOCKS_DROPPED,
        Unit::Count,
        "Gossip blocks not stored, by reason"
    );
    describe_counter!(REORGS, Unit::Count, "Unsafe-chain reorgs");
    describe_histogram!(
        REORG_DEPTH,
        Unit::Count,
        "Blocks replaced by one unsafe-chain reorg"
    );
    describe_counter!(
        BLOCKS_FILLED,
        Unit::Count,
        "Blocks that repaired a gap below the head"
    );
    describe_counter!(
        RETRIES,
        Unit::Count,
        "Store calls repeated after a transient error"
    );
    describe_histogram!(
        INGEST_LAG,
        Unit::Seconds,
        "Time from a block's timestamp to its insert"
    );
    describe_gauge!(
        CHANNEL_DEPTH,
        Unit::Count,
        "Blocks waiting in the channel from the network"
    );
    describe_counter!(
        BLOCKS_PROMOTED,
        Unit::Count,
        "Blocks written to the committed store by promotion"
    );
    describe_counter!(
        PROMOTION_HOLES,
        Unit::Count,
        "Promotions whose range could not be read, by reason"
    );
    describe_counter!(
        PROMOTION_BLOCKS_MISSING,
        Unit::Count,
        "Blocks left out of the committed store by promotion holes"
    );
    describe_counter!(
        L1_REORGS,
        Unit::Count,
        "Rollbacks of the committed store after the safe head moved back"
    );
    describe_counter!(
        ARCHIVE_RESTARTS,
        Unit::Count,
        "Times the archive was emptied to start a new range"
    );
    describe_gauge!(
        SAFE_BLOCK_NUMBER,
        Unit::Count,
        "Number of the committed safe head"
    );
}

/// Records a gossip block newly stored in the unsafe store.
pub(crate) fn block_ingested() {
    counter!(BLOCKS_INGESTED).increment(1);
}

/// Records a gossip block that was not stored.
pub(crate) fn block_dropped(reason: DropReason) {
    counter!(BLOCKS_DROPPED, "reason" => reason.as_str()).increment(1);
}

/// Records an unsafe-chain reorg that replaced `depth` blocks.
pub(crate) fn reorg(depth: usize) {
    counter!(REORGS).increment(1);
    histogram!(REORG_DEPTH).record(small(count(depth)));
}

/// Records blocks below the head that became canonical.
pub(crate) fn fill(blocks: usize) {
    counter!(BLOCKS_FILLED).increment(count(blocks));
}

/// Records a call to `store` repeated after a transient error.
pub(crate) fn retry(store: Store) {
    counter!(RETRIES, "store" => store.as_str()).increment(1);
}

/// Records how long after its timestamp a block was inserted.
pub(crate) fn ingest_lag(seconds: u64) {
    histogram!(INGEST_LAG).record(small(seconds));
}

/// Sets the number of blocks waiting in the channel from the network.
pub(crate) fn channel_depth(blocks: usize) {
    gauge!(CHANNEL_DEPTH).set(small(count(blocks)));
}

/// Records blocks written to the committed store by one promotion.
pub(crate) fn blocks_promoted(blocks: usize) {
    counter!(BLOCKS_PROMOTED).increment(count(blocks));
}

/// Records a promotion that could not read its whole range and left `blocks_missing` blocks
/// out of the committed store.
pub(crate) fn promotion_hole(reason: HoleReason, blocks_missing: u64) {
    counter!(PROMOTION_HOLES, "reason" => reason.as_str()).increment(1);
    counter!(PROMOTION_BLOCKS_MISSING, "reason" => reason.as_str()).increment(blocks_missing);
}

/// Records a rollback of the committed store after the safe head moved back or changed hash.
pub(crate) fn l1_reorg() {
    counter!(L1_REORGS).increment(1);
}

/// Records the archive being emptied to start a new range.
pub(crate) fn archive_restart() {
    counter!(ARCHIVE_RESTARTS).increment(1);
}

/// Sets the number of the committed safe head.
pub(crate) fn safe_block_number(number: u64) {
    gauge!(SAFE_BLOCK_NUMBER).set(gauge_value(number));
}

/// A count as a counter increment. `usize` is at most 64 bits on every supported target.
fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A small value (a depth, seconds of lag) as `f64`, saturating at `u32::MAX`: `f64::from(u32)`
/// is lossless and needs no cast.
fn small(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

/// A block number as `f64` through its two 32-bit halves, exact up to 2^53.
fn gauge_value(value: u64) -> f64 {
    const HIGH_UNIT: f64 = 4_294_967_296.0;
    let high = u32::try_from(value >> u32::BITS).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high).mul_add(HIGH_UNIT, f64::from(low))
}

/// Records a request for a block's receipts.
pub(crate) fn receipts_requested(outcome: RequestOutcome) {
    counter!(RECEIPT_REQUESTS, "outcome" => outcome.as_str()).increment(1);
}

/// Records receipts attached to a block held by `store`.
pub(crate) fn receipts_attached(store: Store) {
    counter!(RECEIPTS_ATTACHED, "store" => store.as_str()).increment(1);
}

/// Records verified receipts that could not be attached.
pub(crate) fn receipts_unmatched(reason: UnmatchedReason) {
    counter!(RECEIPTS_UNMATCHED, "reason" => reason.as_str()).increment(1);
}
