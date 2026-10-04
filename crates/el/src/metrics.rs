//! Metrics of the execution network, emitted through the [`metrics`] facade.
//!
//! This module only records. It installs no recorder and serves no endpoint: until the binary
//! installs one every call here is a no-op, and [`describe`] must run after that. Call sites use
//! the typed helpers below, so metric names and labels live in this file only.
//!
//! Labels are low-cardinality by construction: a direction, an outcome or a reason from a fixed
//! set. Peer ids, addresses and block numbers are never labels.
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `op_indexer_el_candidates_discovered_total` | counter | | Peers of our chain and fork handed to the peer set by discovery, repeats included. |
//! | `op_indexer_el_dials_total` | counter | `outcome` | Dial attempts: `connected`, `too_many_peers`, `handshake_dropped`, `unreachable`, `timeout`, `wrong_fork`, `incompatible` or `failed`. |
//! | `op_indexer_el_inbound_handshakes_failed_total` | counter | `stage` | Inbound connections that did not become a session. |
//! | `op_indexer_el_inbound_refused_total` | counter | | Inbound sessions refused by the peer set: over the limit, already connected, or banned. |
//! | `op_indexer_el_sessions_opened_total` | counter | `direction` | Sessions kept after the handshake: `outbound` or `inbound`. |
//! | `op_indexer_el_sessions_alive` | gauge | | Sessions open now. |
//! | `op_indexer_el_sessions_ended_total` | counter | `reason` | Sessions that ended: `cancelled`, `too_many_peers`, `useless_peer`, `disconnected`, `closed`, `io` or `protocol`. |
//! | `op_indexer_el_session_duration_seconds` | histogram | | How long a session lasted. |
//! | `op_indexer_el_peers_dropped_total` | counter | `reason` | Peers this node disconnected: `bad_data` (also remembered) or `unresponsive`. |
//! | `op_indexer_el_requests_total` | counter | `outcome` | Receipts requests sent to a peer: `verified`, `empty`, `timeout`, `closed`, `malformed` or `invalid`. |
//! | `op_indexer_el_verification_failures_total` | counter | `kind` | Answers that failed verification: `count` or `root`. |
//! | `op_indexer_el_blocks_delivered_total` | counter | | Blocks whose verified receipts were handed on. |
//! | `op_indexer_el_receipts_delivered_total` | counter | | Receipts in those blocks. |
//! | `op_indexer_el_queue_depth` | gauge | | Blocks waiting for receipts. |
//! | `op_indexer_el_queue_dropped_total` | counter | | Requests dropped because the queue was full, oldest first. |
//! | `op_indexer_el_fetch_duration_seconds` | histogram | | Time from a request's arrival to its verified receipts. |

use std::time::Duration;

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

use crate::session::Direction;

const CANDIDATES_DISCOVERED: &str = "op_indexer_el_candidates_discovered_total";
const DIALS: &str = "op_indexer_el_dials_total";
const INBOUND_HANDSHAKES_FAILED: &str = "op_indexer_el_inbound_handshakes_failed_total";
const INBOUND_REFUSED: &str = "op_indexer_el_inbound_refused_total";
const SESSIONS_OPENED: &str = "op_indexer_el_sessions_opened_total";
const SESSIONS_ALIVE: &str = "op_indexer_el_sessions_alive";
const SESSIONS_ENDED: &str = "op_indexer_el_sessions_ended_total";
const SESSION_DURATION: &str = "op_indexer_el_session_duration_seconds";
const PEERS_DROPPED: &str = "op_indexer_el_peers_dropped_total";
const REQUESTS: &str = "op_indexer_el_requests_total";
const VERIFICATION_FAILURES: &str = "op_indexer_el_verification_failures_total";
const BLOCKS_DELIVERED: &str = "op_indexer_el_blocks_delivered_total";
const RECEIPTS_DELIVERED: &str = "op_indexer_el_receipts_delivered_total";
const QUEUE_DEPTH: &str = "op_indexer_el_queue_depth";
const QUEUE_DROPPED: &str = "op_indexer_el_queue_dropped_total";
const FETCH_DURATION: &str = "op_indexer_el_fetch_duration_seconds";

/// How a dial attempt ended, the `outcome` label of the dial counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DialOutcome {
    /// The handshake completed.
    Connected,
    /// The peer said it has too many peers.
    TooManyPeers,
    /// The peer closed the connection during the encrypted handshake, without a reason.
    HandshakeDropped,
    /// The TCP connection could not be opened.
    Unreachable,
    /// A step of the handshake timed out.
    Timeout,
    /// The peer is on another fork or another chain.
    WrongFork,
    /// The peer does not speak our eth version.
    Incompatible,
    /// The hello or status exchange failed for another reason.
    Failed,
}

/// Why a session ended, the `reason` label of the ended-sessions counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum EndLabel {
    /// The node shut down, or this node ended the session.
    Cancelled,
    /// The peer left saying it has too many peers.
    TooManyPeers,
    /// The peer left saying we are useless to it.
    UselessPeer,
    /// The peer left with another reason.
    Disconnected,
    /// The peer closed the connection without a reason.
    Closed,
    /// The connection failed.
    Io,
    /// The peer broke the protocol.
    Protocol,
}

/// Why this node disconnected a peer, the `reason` label of the dropped-peers counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DropReason {
    /// Its answer failed verification or could not be decoded.
    BadData,
    /// It stopped answering requests.
    Unresponsive,
}

/// How a receipts request to one peer ended, the `outcome` label of the request counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RequestOutcome {
    /// The answer matched the receipts root.
    Verified,
    /// The peer does not hold the receipts.
    Empty,
    /// The peer did not answer in time.
    Timeout,
    /// The session ended before the answer.
    Closed,
    /// The answer could not be decoded.
    Malformed,
    /// The answer failed verification.
    Invalid,
}

/// How an answer failed verification, the `kind` label.
#[derive(Debug, Clone, Copy)]
pub(crate) enum VerificationFailure {
    /// Not one receipt per transaction.
    Count,
    /// The receipts do not hash to the header's receipts root.
    Root,
}

impl DialOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::TooManyPeers => "too_many_peers",
            Self::HandshakeDropped => "handshake_dropped",
            Self::Unreachable => "unreachable",
            Self::Timeout => "timeout",
            Self::WrongFork => "wrong_fork",
            Self::Incompatible => "incompatible",
            Self::Failed => "failed",
        }
    }
}

impl EndLabel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::TooManyPeers => "too_many_peers",
            Self::UselessPeer => "useless_peer",
            Self::Disconnected => "disconnected",
            Self::Closed => "closed",
            Self::Io => "io",
            Self::Protocol => "protocol",
        }
    }
}

impl DropReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BadData => "bad_data",
            Self::Unresponsive => "unresponsive",
        }
    }
}

impl RequestOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Empty => "empty",
            Self::Timeout => "timeout",
            Self::Closed => "closed",
            Self::Malformed => "malformed",
            Self::Invalid => "invalid",
        }
    }
}

impl VerificationFailure {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Root => "root",
        }
    }
}

/// Registers the description and unit of every metric with the installed recorder.
///
/// A no-op without a recorder, so it runs after the binary installed one.
pub(crate) fn describe() {
    describe_counter!(
        CANDIDATES_DISCOVERED,
        Unit::Count,
        "Peers of our chain and fork found by discovery"
    );
    describe_counter!(DIALS, Unit::Count, "Dial attempts, by outcome");
    describe_counter!(
        INBOUND_HANDSHAKES_FAILED,
        Unit::Count,
        "Inbound connections that did not become a session, by handshake stage"
    );
    describe_counter!(
        INBOUND_REFUSED,
        Unit::Count,
        "Inbound sessions refused by the peer set"
    );
    describe_counter!(
        SESSIONS_OPENED,
        Unit::Count,
        "Sessions kept after the handshake, by direction"
    );
    describe_gauge!(SESSIONS_ALIVE, Unit::Count, "Sessions open now");
    describe_counter!(SESSIONS_ENDED, Unit::Count, "Sessions ended, by reason");
    describe_histogram!(SESSION_DURATION, Unit::Seconds, "How long a session lasted");
    describe_counter!(
        PEERS_DROPPED,
        Unit::Count,
        "Peers this node disconnected, by reason"
    );
    describe_counter!(
        REQUESTS,
        Unit::Count,
        "Receipts requests sent to a peer, by outcome"
    );
    describe_counter!(
        VERIFICATION_FAILURES,
        Unit::Count,
        "Answers that failed verification, by kind"
    );
    describe_counter!(
        BLOCKS_DELIVERED,
        Unit::Count,
        "Blocks whose verified receipts were handed on"
    );
    describe_counter!(
        RECEIPTS_DELIVERED,
        Unit::Count,
        "Verified receipts handed on"
    );
    describe_gauge!(QUEUE_DEPTH, Unit::Count, "Blocks waiting for receipts");
    describe_counter!(
        QUEUE_DROPPED,
        Unit::Count,
        "Requests dropped because the queue was full"
    );
    describe_histogram!(
        FETCH_DURATION,
        Unit::Seconds,
        "Time from a request's arrival to its verified receipts"
    );
}

/// Records a candidate handed to the peer set by discovery.
pub(crate) fn candidate_discovered() {
    counter!(CANDIDATES_DISCOVERED).increment(1);
}

/// Records a dial attempt.
pub(crate) fn dial(outcome: DialOutcome) {
    counter!(DIALS, "outcome" => outcome.as_str()).increment(1);
}

/// Records an inbound connection whose handshake failed at `stage`.
pub(crate) fn inbound_handshake_failed(stage: &'static str) {
    counter!(INBOUND_HANDSHAKES_FAILED, "stage" => stage).increment(1);
}

/// Records an inbound session the peer set refused.
pub(crate) fn inbound_refused() {
    counter!(INBOUND_REFUSED).increment(1);
}

/// Records a session kept after its handshake.
pub(crate) fn session_opened(direction: Direction) {
    let direction = match direction {
        Direction::Inbound => "inbound",
        Direction::Outbound => "outbound",
    };
    counter!(SESSIONS_OPENED, "direction" => direction).increment(1);
}

/// Sets the number of sessions open now.
pub(crate) fn sessions_alive(sessions: usize) {
    gauge!(SESSIONS_ALIVE).set(small(sessions));
}

/// Records a session that ended after `lasted`.
pub(crate) fn session_ended(reason: EndLabel, lasted: Duration) {
    counter!(SESSIONS_ENDED, "reason" => reason.as_str()).increment(1);
    histogram!(SESSION_DURATION).record(lasted);
}

/// Records a peer this node disconnected.
pub(crate) fn peer_dropped(reason: DropReason) {
    counter!(PEERS_DROPPED, "reason" => reason.as_str()).increment(1);
}

/// Records how a receipts request to one peer ended.
pub(crate) fn request(outcome: RequestOutcome) {
    counter!(REQUESTS, "outcome" => outcome.as_str()).increment(1);
}

/// Records an answer that failed verification.
pub(crate) fn verification_failed(kind: VerificationFailure) {
    counter!(VERIFICATION_FAILURES, "kind" => kind.as_str()).increment(1);
}

/// Records a block's verified receipts handed on, `waited` after its request arrived.
pub(crate) fn delivered(receipts: usize, waited: Duration) {
    counter!(BLOCKS_DELIVERED).increment(1);
    counter!(RECEIPTS_DELIVERED).increment(count(receipts));
    histogram!(FETCH_DURATION).record(waited);
}

/// Sets the number of blocks waiting for receipts.
pub(crate) fn queue_depth(blocks: usize) {
    gauge!(QUEUE_DEPTH).set(small(blocks));
}

/// Records a request dropped because the queue was full.
pub(crate) fn queue_dropped() {
    counter!(QUEUE_DROPPED).increment(1);
}

/// A count as a counter increment. `usize` is at most 64 bits on every supported target.
fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A small count as a gauge value, saturating at `u32::MAX`: `f64::from(u32)` is lossless and
/// needs no cast.
fn small(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}
