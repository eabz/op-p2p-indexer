//! Metrics of the pipeline, emitted through the [`metrics`] facade.
//!
//! This module only records. It installs no recorder and serves no endpoint: until the binary
//! installs one every call here is a no-op, and [`describe`] must run after that. Call sites use
//! the typed helpers below, so metric names and labels live in this file only. The stores'
//! own metrics are in `op_indexer_storage::metrics` and are not repeated here: blocks written
//! per store, unsafe-chain reorgs and their depth, and blocks removed from the archive.
//!
//! Labels are low-cardinality by construction: a drop reason, a hole reason or a store. Block numbers and
//! hashes are never labels.
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `op_indexer_pipeline_blocks_dropped_total` | counter | `reason` | Gossip blocks not stored: `sender_recovery`, `invalid` or `unsupported`. |
//! | `op_indexer_pipeline_blocks_filled_total` | counter | | Blocks below the head that became canonical: gaps repaired. |
//! | `op_indexer_pipeline_ingest_lag_seconds` | histogram | | Time from a block's timestamp to its insert. |
//! | `op_indexer_pipeline_channel_depth` | gauge | | Blocks waiting in the channel from the network. |
//! | `op_indexer_pipeline_blocks_promoted_total` | counter | | Blocks appended to the archive by promotion. |
//! | `op_indexer_pipeline_blocks_promoted_without_receipts_total` | counter | | Of those, the blocks promoted before their receipts arrived. Receipts that come later are attached in the archive. |
//! | `op_indexer_pipeline_promotion_holes_total` | counter | `reason` | Promotions that could not read their whole range from the unsafe store: `missing_ancestor`, `too_long` (the blocks may exist) or `parent_mismatch` (nothing left out, but the range is another chain than the committed safe head). |
//! | `op_indexer_pipeline_promotion_blocks_missing_total` | counter | `reason` | Blocks those promotions left out of the archive, for backfill. |
//! | `op_indexer_pipeline_archive_skipped_blocks_total` | counter | | Promoted blocks not archived because they did not extend the archive's tip. |
//! | `op_indexer_pipeline_safe_block_number` | gauge | | Number of the committed safe head. |
//! | `op_indexer_pipeline_archive_pending_receipts` | gauge | | Archived blocks still without receipts, as last counted by the receipts task. |
//! | `op_indexer_pipeline_range_blocks_stored_total` | counter | | Blocks of a range sync appended to the archive. |
//! | `op_indexer_pipeline_range_block_number` | gauge | | Last block of the range sync that is stored. |
//! | `op_indexer_pipeline_l1_games_total` | counter | `outcome` | Dispute games verified on L1 and compared with our block at their height: `matched` (the block became a head), `mismatch` (the head did not advance) or `unchecked` (a block before Isthmus). |

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use op_indexer_storage::Store;

const BLOCKS_DROPPED: &str = "op_indexer_pipeline_blocks_dropped_total";
const BLOCKS_FILLED: &str = "op_indexer_pipeline_blocks_filled_total";
const INGEST_LAG: &str = "op_indexer_pipeline_ingest_lag_seconds";
const CHANNEL_DEPTH: &str = "op_indexer_pipeline_channel_depth";
const BLOCKS_PROMOTED: &str = "op_indexer_pipeline_blocks_promoted_total";
const BLOCKS_PROMOTED_WITHOUT_RECEIPTS: &str =
    "op_indexer_pipeline_blocks_promoted_without_receipts_total";
const PROMOTION_HOLES: &str = "op_indexer_pipeline_promotion_holes_total";
const PROMOTION_BLOCKS_MISSING: &str = "op_indexer_pipeline_promotion_blocks_missing_total";
const ARCHIVE_SKIPPED_BLOCKS: &str = "op_indexer_pipeline_archive_skipped_blocks_total";
const SAFE_BLOCK_NUMBER: &str = "op_indexer_pipeline_safe_block_number";
const ARCHIVE_PENDING_RECEIPTS: &str = "op_indexer_pipeline_archive_pending_receipts";
const RANGE_BLOCKS_STORED: &str = "op_indexer_pipeline_range_blocks_stored_total";
const RANGE_BLOCK_NUMBER: &str = "op_indexer_pipeline_range_block_number";
const L1_GAMES: &str = "op_indexer_pipeline_l1_games_total";
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

/// What came of comparing a dispute game with our block, the `outcome` label.
#[derive(Debug, Clone, Copy)]
pub(crate) enum GameOutcome {
    /// The claim equals our block's output root.
    Matched,
    /// The claim differs from our block.
    Mismatch,
    /// Our header does not carry what the claim commits to (a block before Isthmus).
    Unchecked,
}

impl GameOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Mismatch => "mismatch",
            Self::Unchecked => "unchecked",
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
        L1_GAMES,
        Unit::Count,
        "Dispute games verified on L1 and compared with our block, by outcome"
    );
    describe_counter!(
        RECEIPT_REQUESTS,
        Unit::Count,
        "Requests for a block's receipts, by outcome"
    );
    describe_counter!(
        RECEIPTS_ATTACHED,
        Unit::Count,
        "Blocks that got their receipts, by the store that held the block"
    );
    describe_counter!(
        RECEIPTS_UNMATCHED,
        Unit::Count,
        "Verified receipts that were not attached, by reason"
    );
    describe_counter!(
        BLOCKS_DROPPED,
        Unit::Count,
        "Gossip blocks not stored, by reason"
    );
    describe_counter!(
        BLOCKS_FILLED,
        Unit::Count,
        "Blocks that repaired a gap below the head"
    );
    describe_histogram!(
        INGEST_LAG,
        Unit::Seconds,
        "Time from a block's timestamp to its insert"
    );
    describe_counter!(
        BLOCKS_PROMOTED_WITHOUT_RECEIPTS,
        Unit::Count,
        "Blocks promoted before their receipts arrived"
    );
    describe_gauge!(
        CHANNEL_DEPTH,
        Unit::Count,
        "Blocks waiting in the channel from the network"
    );
    describe_counter!(
        BLOCKS_PROMOTED,
        Unit::Count,
        "Blocks appended to the archive by promotion"
    );
    describe_counter!(
        PROMOTION_HOLES,
        Unit::Count,
        "Promotions whose range could not be read, by reason"
    );
    describe_counter!(
        PROMOTION_BLOCKS_MISSING,
        Unit::Count,
        "Blocks left out of the archive by promotion holes"
    );
    describe_counter!(
        ARCHIVE_SKIPPED_BLOCKS,
        Unit::Count,
        "Promoted blocks not archived because they did not extend the archive"
    );
    describe_gauge!(
        SAFE_BLOCK_NUMBER,
        Unit::Count,
        "Number of the committed safe head"
    );
    describe_gauge!(
        ARCHIVE_PENDING_RECEIPTS,
        Unit::Count,
        "Archived blocks still without receipts"
    );
    describe_counter!(
        RANGE_BLOCKS_STORED,
        Unit::Count,
        "Blocks of a range sync appended to the archive"
    );
    describe_gauge!(
        RANGE_BLOCK_NUMBER,
        Unit::Count,
        "Last block of the range sync that is stored"
    );
}

/// Records a gossip block that was not stored.
pub(crate) fn block_dropped(reason: DropReason) {
    counter!(BLOCKS_DROPPED, "reason" => reason.as_str()).increment(1);
}

/// Records blocks below the head that became canonical.
pub(crate) fn fill(blocks: usize) {
    counter!(BLOCKS_FILLED).increment(count(blocks));
}

/// Records how long after its timestamp a block was inserted.
pub(crate) fn ingest_lag(seconds: u64) {
    histogram!(INGEST_LAG).record(small(seconds));
}

/// Sets the number of blocks waiting in the channel from the network.
pub(crate) fn channel_depth(blocks: usize) {
    gauge!(CHANNEL_DEPTH).set(small(count(blocks)));
}

/// Records blocks written to the committed store by one promotion, and how many of them had
/// no receipts yet.
pub(crate) fn blocks_promoted(blocks: usize, without_receipts: usize) {
    counter!(BLOCKS_PROMOTED).increment(count(blocks));
    counter!(BLOCKS_PROMOTED_WITHOUT_RECEIPTS).increment(count(without_receipts));
}

/// Records a promotion that could not read its whole range and left `blocks_missing` blocks
/// out of the committed store.
pub(crate) fn promotion_hole(reason: HoleReason, blocks_missing: u64) {
    counter!(PROMOTION_HOLES, "reason" => reason.as_str()).increment(1);
    counter!(PROMOTION_BLOCKS_MISSING, "reason" => reason.as_str()).increment(blocks_missing);
}

/// Records promoted blocks that were not archived because they do not extend the archive.
pub(crate) fn archive_skipped(blocks: usize) {
    counter!(ARCHIVE_SKIPPED_BLOCKS).increment(count(blocks));
}

/// Sets how many archived blocks are still without receipts.
pub(crate) fn archive_pending_receipts(blocks: u64) {
    gauge!(ARCHIVE_PENDING_RECEIPTS).set(gauge_value(blocks));
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

/// Records a stored batch of the range sync, ending at block `last`.
pub(crate) fn range_stored(blocks: usize, last: u64) {
    counter!(RANGE_BLOCKS_STORED).increment(count(blocks));
    gauge!(RANGE_BLOCK_NUMBER).set(gauge_value(last));
}

/// Records a dispute game compared with our block.
pub(crate) fn game(outcome: GameOutcome) {
    counter!(L1_GAMES, "outcome" => outcome.as_str()).increment(1);
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
