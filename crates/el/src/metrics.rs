//! Metrics of the execution network, emitted through the [`metrics`] facade.
//!
//! This module only records. It installs no recorder and serves no endpoint: until the binary
//! installs one every call here is a no-op, and [`describe`] must run after that. Call sites use
//! the typed helpers below, so metric names and labels live in this file only.
//!
//! The metrics of discovery, dials and sessions carry a `network` label (`op` for the chain's
//! execution network, `l1` for Ethereum's), because both can run in one process; the rest are
//! the chain's own.
//!
//! Labels are low-cardinality by construction: a direction, an outcome or a reason from a fixed
//! set. Peer ids, addresses and block numbers are never labels.
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `op_indexer_el_candidates_discovered_total` | counter | `network` | Peers of our chain and fork handed to the peer set by discovery, repeats included. |
//! | `op_indexer_el_dials_total` | counter | `network`, `outcome` | Dial attempts: `connected`, `too_many_peers`, `handshake_dropped`, `unreachable`, `timeout`, `wrong_fork`, `incompatible` or `failed`. |
//! | `op_indexer_el_inbound_handshakes_failed_total` | counter | `network`, `stage` | Inbound connections that did not become a session. |
//! | `op_indexer_el_inbound_refused_total` | counter | `network` | Inbound sessions refused by the peer set: over the limit, already connected, or banned. |
//! | `op_indexer_el_sessions_opened_total` | counter | `network`, `direction` | Sessions kept after the handshake: `outbound` or `inbound`. |
//! | `op_indexer_el_sessions_ended_total` | counter | `network`, `reason` | Sessions that ended: `cancelled`, `too_many_peers`, `useless_peer`, `disconnected`, `closed`, `io`, `protocol` or `stalled` (did not read what it asked for). |
//! | `op_indexer_el_session_duration_seconds` | histogram | `network` | How long a session lasted. |
//! | `op_indexer_el_peers_dropped_total` | counter | `network`, `reason` | Peers this node disconnected: `bad_data` (also banned), `undecodable` or `unresponsive`. |
//! | `op_indexer_el_requests_total` | counter | `outcome` | Receipts requests sent to a peer: `verified`, `empty`, `timeout`, `closed`, `malformed` or `invalid`. |
//! | `op_indexer_el_verification_failures_total` | counter | `kind` | Answers that failed verification: `count` or `root`. |
//! | `op_indexer_el_receipts_delivered_total` | counter | | Receipts in those blocks. |
//! | `op_indexer_el_queue_depth` | gauge | | Blocks waiting for receipts. |
//! | `op_indexer_el_queue_dropped_total` | counter | | Requests dropped because the queue was full, oldest first. |
//! | `op_indexer_el_fetch_duration_seconds` | histogram | | Time from a request's arrival to its verified receipts. |
//! | `op_indexer_el_sync_requests_total` | counter | `outcome` | Jobs of the range sync on one session (a page of headers, or a segment of blocks): `verified`, `not_held`, `invalid`, `malformed`, `unsupported`, `timeout` or `closed`. |
//! | `op_indexer_el_sync_blocks_total` | counter | | Blocks of the range sync verified and handed on. |
//! | `op_indexer_el_sync_block_number` | gauge | | Last block of the range sync handed on. |
//! | `op_indexer_el_served_requests_total` | counter | `kind`, `outcome` | Peers' requests for `headers`, `bodies` or `receipts`: `answered`, `empty` (nothing held), `rate_limited`, `busy` (too many in flight; both answered empty), `malformed` or `failed` (the provider failed). |
//! | `op_indexer_el_served_items_total` | counter | `kind` | Headers, bodies or blocks of receipts sent to peers. |
//! | `op_indexer_el_served_bytes_total` | counter | `kind` | Their size. |

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
const SESSIONS_ENDED: &str = "op_indexer_el_sessions_ended_total";
const SESSION_DURATION: &str = "op_indexer_el_session_duration_seconds";
const PEERS_DROPPED: &str = "op_indexer_el_peers_dropped_total";
const REQUESTS: &str = "op_indexer_el_requests_total";
const VERIFICATION_FAILURES: &str = "op_indexer_el_verification_failures_total";
const RECEIPTS_DELIVERED: &str = "op_indexer_el_receipts_delivered_total";
const QUEUE_DEPTH: &str = "op_indexer_el_queue_depth";
const QUEUE_DROPPED: &str = "op_indexer_el_queue_dropped_total";
const FETCH_DURATION: &str = "op_indexer_el_fetch_duration_seconds";
const SYNC_REQUESTS: &str = "op_indexer_el_sync_requests_total";
const SYNC_BLOCKS: &str = "op_indexer_el_sync_blocks_total";
const SYNC_BLOCK_NUMBER: &str = "op_indexer_el_sync_block_number";
const SERVED_REQUESTS: &str = "op_indexer_el_served_requests_total";
const SERVED_ITEMS: &str = "op_indexer_el_served_items_total";
const SERVED_BYTES: &str = "op_indexer_el_served_bytes_total";

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
    /// The peer did not read what it asked for.
    Stalled,
}

/// Why this node disconnected a peer, the `reason` label of the dropped-peers counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DropReason {
    /// Its answer failed verification or could not be decoded.
    BadData,
    /// Its answer could not be decoded.
    Undecodable,
    /// It stopped answering requests.
    Unresponsive,
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
            Self::Stalled => "stalled",
        }
    }
}

impl DropReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BadData => "bad_data",
            Self::Undecodable => "undecodable",
            Self::Unresponsive => "unresponsive",
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

/// What a peer asked for, the `kind` label of the serving metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ServeKind {
    /// `GetBlockHeaders`.
    Headers,
    /// `GetBlockBodies`.
    Bodies,
    /// `GetReceipts`.
    Receipts,
}

impl ServeKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Headers => "headers",
            Self::Bodies => "bodies",
            Self::Receipts => "receipts",
        }
    }
}

/// How a peer's request was handled, the `outcome` label of the served-requests counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ServeOutcome {
    /// Answered with at least one item.
    Answered,
    /// Nothing of what was asked is held.
    Empty,
    /// The peer is over its requests per minute; answered empty.
    RateLimited,
    /// Too many requests in flight, of the peer or of all peers; answered empty.
    Busy,
    /// The request could not be decoded.
    Malformed,
    /// The provider failed, or held data could not be decoded.
    Failed,
}

impl ServeOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::Empty => "empty",
            Self::RateLimited => "rate_limited",
            Self::Busy => "busy",
            Self::Malformed => "malformed",
            Self::Failed => "failed",
        }
    }
}

/// How a job of the range sync on one session ended, the `outcome` label of its counter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SyncOutcome {
    /// Everything fetched matched the trusted hashes.
    Verified,
    /// The peer does not hold the blocks.
    NotHeld,
    /// An answer failed verification.
    Invalid,
    /// An answer could not be decoded.
    Malformed,
    /// Verified data this build cannot read.
    Unsupported,
    /// The peer did not answer in time.
    Timeout,
    /// The session ended before the answer.
    Closed,
}

impl SyncOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::NotHeld => "not_held",
            Self::Invalid => "invalid",
            Self::Malformed => "malformed",
            Self::Unsupported => "unsupported",
            Self::Timeout => "timeout",
            Self::Closed => "closed",
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
        SERVED_REQUESTS,
        Unit::Count,
        "Peers' requests for headers, bodies or receipts, by kind and outcome"
    );
    describe_counter!(
        SERVED_ITEMS,
        Unit::Count,
        "Headers, bodies or blocks of receipts sent to peers"
    );
    describe_counter!(SERVED_BYTES, Unit::Bytes, "Size of the items sent to peers");
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
    describe_counter!(
        SYNC_REQUESTS,
        Unit::Count,
        "Jobs of the range sync on one session, by outcome"
    );
    describe_counter!(
        SYNC_BLOCKS,
        Unit::Count,
        "Blocks of the range sync verified and handed on"
    );
    describe_gauge!(
        SYNC_BLOCK_NUMBER,
        Unit::Count,
        "Last block of the range sync handed on"
    );
}

/// Records a candidate handed to the peer set by discovery.
pub(crate) fn candidate_discovered(network: &'static str) {
    counter!(CANDIDATES_DISCOVERED, "network" => network).increment(1);
}

/// Records a dial attempt.
pub(crate) fn dial(network: &'static str, outcome: DialOutcome) {
    counter!(DIALS, "network" => network, "outcome" => outcome.as_str()).increment(1);
}

/// Records an inbound connection whose handshake failed at `stage`.
pub(crate) fn inbound_handshake_failed(network: &'static str, stage: &'static str) {
    counter!(INBOUND_HANDSHAKES_FAILED, "network" => network, "stage" => stage).increment(1);
}

/// Records an inbound session the peer set refused.
pub(crate) fn inbound_refused(network: &'static str) {
    counter!(INBOUND_REFUSED, "network" => network).increment(1);
}

/// Records a session kept after its handshake.
pub(crate) fn session_opened(network: &'static str, direction: Direction) {
    let direction = match direction {
        Direction::Inbound => "inbound",
        Direction::Outbound => "outbound",
    };
    counter!(SESSIONS_OPENED, "network" => network, "direction" => direction).increment(1);
}

/// Records a session that ended after `lasted`.
pub(crate) fn session_ended(network: &'static str, reason: EndLabel, lasted: Duration) {
    counter!(SESSIONS_ENDED, "network" => network, "reason" => reason.as_str()).increment(1);
    histogram!(SESSION_DURATION, "network" => network).record(lasted);
}

/// Records a peer this node disconnected.
pub(crate) fn peer_dropped(network: &'static str, reason: DropReason) {
    counter!(PEERS_DROPPED, "network" => network, "reason" => reason.as_str()).increment(1);
}

/// Records how a receipts request to one peer ended; `outcome` is the label
/// (`fetch::Outcome::label`).
pub(crate) fn request(outcome: &'static str) {
    counter!(REQUESTS, "outcome" => outcome).increment(1);
}

/// Records an answer that failed verification.
pub(crate) fn verification_failed(kind: VerificationFailure) {
    counter!(VERIFICATION_FAILURES, "kind" => kind.as_str()).increment(1);
}

/// Records a block's verified receipts handed on, `waited` after its request arrived.
pub(crate) fn delivered(receipts: usize, waited: Duration) {
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

/// Records how a peer's request was handled.
pub(crate) fn served(kind: ServeKind, outcome: ServeOutcome) {
    counter!(SERVED_REQUESTS, "kind" => kind.as_str(), "outcome" => outcome.as_str()).increment(1);
}

/// Records the items sent in one answer and their size.
pub(crate) fn served_items(kind: ServeKind, items: usize, bytes: usize) {
    counter!(SERVED_ITEMS, "kind" => kind.as_str()).increment(count(items));
    counter!(SERVED_BYTES, "kind" => kind.as_str()).increment(count(bytes));
}

/// Records how a job of the range sync ended.
pub(crate) fn sync_request(outcome: SyncOutcome) {
    counter!(SYNC_REQUESTS, "outcome" => outcome.as_str()).increment(1);
}

/// Records blocks of the range sync handed on, the last of them block `last`.
pub(crate) fn sync_blocks(blocks: usize, last: u64) {
    const HIGH_UNIT: f64 = 4_294_967_296.0;
    counter!(SYNC_BLOCKS).increment(count(blocks));
    // A block number as `f64` through its two 32-bit halves, exact up to 2^53.
    let high = u32::try_from(last >> u32::BITS).unwrap_or(u32::MAX);
    let low = u32::try_from(last & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    gauge!(SYNC_BLOCK_NUMBER).set(f64::from(high).mul_add(HIGH_UNIT, f64::from(low)));
}
