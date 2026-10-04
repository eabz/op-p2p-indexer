//! Metrics of the p2p node, emitted through the [`metrics`] facade.
//!
//! This module only records. It installs no recorder and serves no endpoint: until the binary
//! installs one (e.g. a Prometheus exporter) every call here is a no-op. Call sites use the typed
//! helpers below, so metric names and labels live in this file only.
//!
//! Labels are low-cardinality by construction: payload version, rejection reason, connection
//! direction, and dial outcome. Peer ids, addresses, and block numbers are never labels.
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `op_indexer_p2p_peers_connected` | gauge | `direction` | Connected peers, by who dialed. |
//! | `op_indexer_p2p_peers_subscribed` | gauge | | Connected peers subscribed to our block topics. |
//! | `op_indexer_p2p_peers_evicted_total` | counter | | Peers disconnected for not subscribing to our block topics. |
//! | `op_indexer_p2p_blocks_accepted_total` | counter | `version` | Valid, new unsafe blocks. |
//! | `op_indexer_p2p_blocks_duplicate_total` | counter | | Valid blocks already seen, ignored. |
//! | `op_indexer_p2p_blocks_rejected_total` | counter | `reason` | Block messages rejected, by [`BlockError`] variant. |
//! | `op_indexer_p2p_blocks_ignored_backlog_total` | counter | | Block messages ignored because the validation backlog was full. |
//! | `op_indexer_p2p_blocks_dropped_total` | counter | | Accepted blocks dropped because the consumer channel was full. |
//! | `op_indexer_p2p_gossip_decompress_failures_total` | counter | | Gossip messages dropped as invalid or oversized snappy. |
//! | `op_indexer_p2p_block_validation_duration_seconds` | histogram | | Time to validate one block message. |
//! | `op_indexer_p2p_validations_pending` | gauge | | Block validations in flight. |
//! | `op_indexer_p2p_latest_block_number` | gauge | | Number of the last accepted block. |
//! | `op_indexer_p2p_latest_block_lag_seconds` | gauge | | Local time minus the last accepted block's timestamp; near the block time at the tip. |
//! | `op_indexer_p2p_gaps_detected_total` | counter | | Gaps in the accepted block sequence (a block arrived whose parent height was never accepted). |
//! | `op_indexer_p2p_blocks_missed_total` | counter | | Blocks skipped by those gaps (each gap adds its size). |
//! | `op_indexer_p2p_clock_skew_warnings_total` | counter | | Warnings that the local clock looks slow. |
//! | `op_indexer_p2p_dials_total` | counter | `outcome` | Dials `started`, `skipped` (backoff, already connected, limits), or `failed`. |
//! | `op_indexer_p2p_known_peers_saved_total` | counter | | Peers saved to the node store as known good. |
//! | `op_indexer_p2p_discovery_lookups_total` | counter | | discv5 random lookups run. |
//! | `op_indexer_p2p_discovery_candidates_total` | counter | | Dialable peers of our chain found per round, repeats included. |
//! | `op_indexer_p2p_discovery_new_candidates_total` | counter | | Candidates reported to the network for the first time. |

use std::time::Instant;

use libp2p::core::ConnectedPoint;
use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use op_indexer_primitives::{PayloadVersion, UnsafeBlock};

use crate::block::BlockError;

const PEERS_CONNECTED: &str = "op_indexer_p2p_peers_connected";
const PEERS_SUBSCRIBED: &str = "op_indexer_p2p_peers_subscribed";
const PEERS_EVICTED: &str = "op_indexer_p2p_peers_evicted_total";
const BLOCKS_ACCEPTED: &str = "op_indexer_p2p_blocks_accepted_total";
const BLOCKS_DUPLICATE: &str = "op_indexer_p2p_blocks_duplicate_total";
const BLOCKS_REJECTED: &str = "op_indexer_p2p_blocks_rejected_total";
const BLOCKS_IGNORED_BACKLOG: &str = "op_indexer_p2p_blocks_ignored_backlog_total";
const BLOCKS_DROPPED: &str = "op_indexer_p2p_blocks_dropped_total";
const GOSSIP_DECOMPRESS_FAILURES: &str = "op_indexer_p2p_gossip_decompress_failures_total";
const BLOCK_VALIDATION_DURATION: &str = "op_indexer_p2p_block_validation_duration_seconds";
const VALIDATIONS_PENDING: &str = "op_indexer_p2p_validations_pending";
const LATEST_BLOCK_NUMBER: &str = "op_indexer_p2p_latest_block_number";
const LATEST_BLOCK_LAG: &str = "op_indexer_p2p_latest_block_lag_seconds";
const GAPS_DETECTED: &str = "op_indexer_p2p_gaps_detected_total";
const BLOCKS_MISSED: &str = "op_indexer_p2p_blocks_missed_total";
const CLOCK_SKEW_WARNINGS: &str = "op_indexer_p2p_clock_skew_warnings_total";
const DIALS: &str = "op_indexer_p2p_dials_total";
const KNOWN_PEERS_SAVED: &str = "op_indexer_p2p_known_peers_saved_total";
const DISCOVERY_LOOKUPS: &str = "op_indexer_p2p_discovery_lookups_total";
const DISCOVERY_CANDIDATES: &str = "op_indexer_p2p_discovery_candidates_total";
const DISCOVERY_NEW_CANDIDATES: &str = "op_indexer_p2p_discovery_new_candidates_total";

/// How a dial attempt ended, the `outcome` label of the dials counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DialOutcome {
    /// The swarm started dialing.
    Started,
    /// Not dialed: in backoff, already connected or dialing, or refused by connection limits.
    Skipped,
    /// A started dial did not produce a connection.
    Failed,
}

/// Registers the description and unit of every metric with the installed recorder.
///
/// A no-op without a recorder, so the recorder must be installed first.
pub(crate) fn describe() {
    describe_gauge!(
        PEERS_CONNECTED,
        Unit::Count,
        "Connected peers, by who dialed"
    );
    describe_gauge!(
        PEERS_SUBSCRIBED,
        Unit::Count,
        "Connected peers subscribed to our block topics"
    );
    describe_counter!(
        PEERS_EVICTED,
        Unit::Count,
        "Peers disconnected for not subscribing to our block topics"
    );
    describe_counter!(BLOCKS_ACCEPTED, Unit::Count, "Valid, new unsafe blocks");
    describe_counter!(
        BLOCKS_DUPLICATE,
        Unit::Count,
        "Valid blocks already seen, ignored"
    );
    describe_counter!(
        BLOCKS_REJECTED,
        Unit::Count,
        "Block messages rejected, by reason"
    );
    describe_counter!(
        BLOCKS_IGNORED_BACKLOG,
        Unit::Count,
        "Block messages ignored because the validation backlog was full"
    );
    describe_counter!(
        BLOCKS_DROPPED,
        Unit::Count,
        "Accepted blocks dropped because the consumer channel was full"
    );
    describe_counter!(
        GOSSIP_DECOMPRESS_FAILURES,
        Unit::Count,
        "Gossip messages dropped as invalid or oversized snappy"
    );
    describe_histogram!(
        BLOCK_VALIDATION_DURATION,
        Unit::Seconds,
        "Time to validate one block message"
    );
    describe_gauge!(
        VALIDATIONS_PENDING,
        Unit::Count,
        "Block validations in flight"
    );
    describe_gauge!(
        LATEST_BLOCK_NUMBER,
        Unit::Count,
        "Number of the last accepted block"
    );
    describe_gauge!(
        LATEST_BLOCK_LAG,
        Unit::Seconds,
        "Local time minus the last accepted block's timestamp"
    );
    describe_counter!(
        GAPS_DETECTED,
        Unit::Count,
        "Gaps in the accepted block sequence"
    );
    describe_counter!(BLOCKS_MISSED, Unit::Count, "Blocks skipped by gaps");
    describe_counter!(
        CLOCK_SKEW_WARNINGS,
        Unit::Count,
        "Warnings that the local clock looks slow"
    );
    describe_counter!(DIALS, Unit::Count, "Dial attempts, by outcome");
    describe_counter!(
        KNOWN_PEERS_SAVED,
        Unit::Count,
        "Peers saved to the node store as known good"
    );
    describe_counter!(DISCOVERY_LOOKUPS, Unit::Count, "discv5 random lookups run");
    describe_counter!(
        DISCOVERY_CANDIDATES,
        Unit::Count,
        "Dialable peers of our chain found per round, repeats included"
    );
    describe_counter!(
        DISCOVERY_NEW_CANDIDATES,
        Unit::Count,
        "Candidates reported to the network for the first time"
    );
}

/// Records a new connection to a peer.
pub(crate) fn peer_connected(endpoint: &ConnectedPoint) {
    gauge!(PEERS_CONNECTED, "direction" => direction(endpoint)).increment(1.0);
}

/// Records a closed connection to a peer.
pub(crate) fn peer_disconnected(endpoint: &ConnectedPoint) {
    gauge!(PEERS_CONNECTED, "direction" => direction(endpoint)).decrement(1.0);
}

/// Sets the number of connected peers subscribed to our block topics.
pub(crate) fn peers_subscribed(peers: usize) {
    gauge!(PEERS_SUBSCRIBED).set(gauge_value(peers));
}

/// Records a peer disconnected for not subscribing to our block topics.
pub(crate) fn peer_evicted() {
    counter!(PEERS_EVICTED).increment(1);
}

/// Records an accepted block: the per-version counter, its number, and its lag behind
/// `now_secs` (Unix seconds).
pub(crate) fn block_accepted(block: &UnsafeBlock, now_secs: u64) {
    counter!(BLOCKS_ACCEPTED, "version" => version(block.version)).increment(1);
    gauge!(LATEST_BLOCK_NUMBER).set(gauge_value(block.number()));
    gauge!(LATEST_BLOCK_LAG).set(gauge_value(now_secs.saturating_sub(block.timestamp_secs())));
}

/// Records a valid block that was already seen.
pub(crate) fn block_duplicate() {
    counter!(BLOCKS_DUPLICATE).increment(1);
}

/// Records a rejected block message under its [`BlockError`] variant.
pub(crate) fn block_rejected(err: &BlockError) {
    counter!(BLOCKS_REJECTED, "reason" => reason(err)).increment(1);
}

/// Records a block message ignored because the validation backlog was full.
pub(crate) fn block_ignored_backlog() {
    counter!(BLOCKS_IGNORED_BACKLOG).increment(1);
}

/// Records an accepted block dropped because the consumer channel was full.
pub(crate) fn block_dropped() {
    counter!(BLOCKS_DROPPED).increment(1);
}

/// Records a gossip message dropped because it was not valid snappy or was too large.
pub(crate) fn gossip_decompress_failed() {
    counter!(GOSSIP_DECOMPRESS_FAILURES).increment(1);
}

/// Runs `validate` and records how long it took.
pub(crate) fn timed_validation<T>(validate: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = validate();
    histogram!(BLOCK_VALIDATION_DURATION).record(started.elapsed());
    result
}

/// Sets the number of block validations in flight.
pub(crate) fn validations_pending(pending: usize) {
    gauge!(VALIDATIONS_PENDING).set(gauge_value(pending));
}

/// Records a gap in the accepted block sequence that skipped `missed` blocks.
pub(crate) fn gap_detected(missed: u64) {
    counter!(GAPS_DETECTED).increment(1);
    counter!(BLOCKS_MISSED).increment(missed);
}

/// Records a warning that the local clock looks slow.
pub(crate) fn clock_skew_warned() {
    counter!(CLOCK_SKEW_WARNINGS).increment(1);
}

/// Records the outcome of a dial attempt.
pub(crate) fn dial(outcome: DialOutcome) {
    let outcome = match outcome {
        DialOutcome::Started => "started",
        DialOutcome::Skipped => "skipped",
        DialOutcome::Failed => "failed",
    };
    counter!(DIALS, "outcome" => outcome).increment(1);
}

/// Records a peer saved to the node store as known good.
pub(crate) fn known_peer_saved() {
    counter!(KNOWN_PEERS_SAVED).increment(1);
}

/// Records one discovery round: the lookups it ran, the dialable candidates it found,
/// and how many of those were new.
pub(crate) fn discovery_round(lookups: usize, candidates: usize, new_candidates: usize) {
    counter!(DISCOVERY_LOOKUPS).increment(counter_value(lookups));
    counter!(DISCOVERY_CANDIDATES).increment(counter_value(candidates));
    counter!(DISCOVERY_NEW_CANDIDATES).increment(counter_value(new_candidates));
}

fn direction(endpoint: &ConnectedPoint) -> &'static str {
    if endpoint.is_dialer() {
        "outbound"
    } else {
        "inbound"
    }
}

fn version(version: PayloadVersion) -> &'static str {
    match version {
        PayloadVersion::V1 => "v1",
        PayloadVersion::V2 => "v2",
        PayloadVersion::V3 => "v3",
        PayloadVersion::V4 => "v4",
    }
}

/// The variant name, never the message: messages carry block numbers and addresses.
fn reason(err: &BlockError) -> &'static str {
    match err {
        BlockError::TooShort { .. } => "too_short",
        BlockError::InvalidPayload(_) => "invalid_payload",
        BlockError::Stale { .. } => "stale",
        BlockError::TooFarInFuture { .. } => "too_far_in_future",
        BlockError::InvalidRecoveryId { .. } => "invalid_recovery_id",
        BlockError::MalformedSignature { .. } => "malformed_signature",
        BlockError::WrongSigner { .. } => "wrong_signer",
        BlockError::InvalidBlock { .. } => "invalid_block",
        BlockError::HashMismatch { .. } => "hash_mismatch",
        BlockError::TooManyAtHeight { .. } => "too_many_at_height",
        BlockError::UndecodableTransaction { .. } => "undecodable_transaction",
    }
}

/// Converts through `u32` because `f64::from(u32)` is lossless and needs no cast suppression;
/// values above `u32::MAX` (about 4.29 billion) are reported as `u32::MAX`.
fn gauge_value(value: impl TryInto<u32>) -> f64 {
    f64::from(value.try_into().unwrap_or(u32::MAX))
}

fn counter_value(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
