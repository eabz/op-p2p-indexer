//! A session's side of serving: the per-peer limits, handing a request to the server without
//! waiting, and what the session tells its peer about the blocks this node serves.
//!
//! Does not read blocks: the server does (parent module).

use std::time::{Duration, Instant};

use alloy_primitives::{BlockNumber, Bytes};
use alloy_rlp::Encodable;
use op_indexer_primitives::BlockRef;
use reth_eth_wire_types::{BlockRangeUpdate, EthVersion};
use tokio::sync::{mpsc, watch};

use super::{HeldRange, MAX_ITEMS, Request, ServeKind, response, response_id};
use crate::wire;

/// Requests answered from the provider per peer per [`RATE_WINDOW`]; further ones get an empty
/// answer. Two a second, each up to 2 MiB, is more than a syncing node asks
/// of one peer, and bounds what one peer can make this node read.
const MAX_REQUESTS_PER_WINDOW: u32 = 120;

/// The window of the per-peer request limit.
const RATE_WINDOW: Duration = Duration::from_mins(1);

/// Requests of one peer being answered at once; further ones get an empty answer. A syncing
/// node keeps a few requests open per peer.
const MAX_IN_FLIGHT_PER_PEER: usize = 4;

/// Largest request body read: a request for [`MAX_ITEMS`] hashes (33 bytes each as RLP), its
/// list headers and its request id. A larger request asks for more than is ever answered, and
/// is refused before it is copied or decoded: a 10 MiB request holds 300,000 hashes.
const MAX_REQUEST_BYTES: usize = MAX_ITEMS * 33 + 32;

/// Shortest time between two `BlockRangeUpdate`s to one peer: "about once every two minutes"
/// (devp2p `caps/eth.md`, `BlockRangeUpdate`), which is also at most once per 32 blocks on the
/// chains served (EIP-7642).
const RANGE_UPDATE_INTERVAL: Duration = Duration::from_mins(2);

/// The range a session tells its peer, in the status and in `BlockRangeUpdate`: only blocks
/// this node serves, or its tip alone.
///
/// - Blocks are held: the held range as it is, from its first block to its last, with the last
///   block as the head, however far that is behind the chain's tip. Every block advertised is
///   served, and peers (and our own range sync) know to ask for them: making an imported
///   history available is the point of holding it. The node then looks like one that is
///   behind. Earlier runs suggest peers accept that (a stale but real head kept sessions; only
///   genesis as the head ended them), but a status hours or days behind is **not confirmed
///   live**: whether peers keep such a session, and still answer its requests for the tip's
///   receipts, is the first thing to check in the next run.
/// - Nothing is held: the tip the node knows, earliest and latest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdvertisedRange {
    pub(crate) earliest: BlockNumber,
    pub(crate) latest: BlockRef,
}

/// What every session needs to serve: the way to the server and the range it holds.
#[derive(Debug, Clone)]
pub(crate) struct Serving {
    pub(super) requests: mpsc::Sender<Request>,
    pub(super) range: watch::Receiver<Option<HeldRange>>,
    /// Whether anything answers requests: `false` for a network the node only asks.
    pub(super) enabled: bool,
}

impl Serving {
    /// Whether this node serves blocks on this network.
    pub(crate) const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The serving side of one new session, and the channel its answers arrive on. `tip`
    /// follows the newest block the node knows. The session is taken to advertise the current
    /// range in its status.
    pub(crate) fn session(
        &self,
        tip: watch::Receiver<Option<BlockRef>>,
        lowest: BlockNumber,
        version: EthVersion,
    ) -> (SessionServing, mpsc::Receiver<Bytes>) {
        let (answers_tx, answers_rx) = mpsc::channel(MAX_IN_FLIGHT_PER_PEER);
        let now = Instant::now();
        let mut session = SessionServing {
            requests: self.requests.clone(),
            answers: answers_tx,
            held: self.range.clone(),
            tip,
            lowest,
            version,
            advertised: None,
            advertised_at: now,
            window_start: now,
            window_requests: 0,
        };
        session.advertised = session.range();
        (session, answers_rx)
    }
}

/// What a session does with a message that may be a request.
#[derive(Debug)]
pub(crate) enum Handled {
    /// Send this answer now.
    Now(Bytes),
    /// The answer will arrive on the session's answer channel.
    Later,
    /// Not a request this node answers.
    NotARequest,
}

/// One session's serving state: its limits and what it last told the peer about our range.
#[derive(Debug)]
pub(crate) struct SessionServing {
    requests: mpsc::Sender<Request>,
    answers: mpsc::Sender<Bytes>,
    held: watch::Receiver<Option<HeldRange>>,
    tip: watch::Receiver<Option<BlockRef>>,
    /// The lowest block this peer is served and told about: 0 for an op-p2p-indexer, the
    /// network's Bedrock block for anyone else (`NetworkSpec::indexers_only_below`).
    lowest: BlockNumber,
    /// The session's eth version: receipts are served in its format, and `BlockRangeUpdate`
    /// exists from eth/69 on.
    version: EthVersion,
    /// The range the peer was last told.
    advertised: Option<AdvertisedRange>,
    advertised_at: Instant,
    window_start: Instant,
    /// Requests handed to the server since `window_start`.
    window_requests: u32,
}

impl SessionServing {
    /// The range the peer was last told: the one to put in the status of a new session.
    pub(crate) const fn advertised(&self) -> Option<AdvertisedRange> {
        self.advertised
    }

    /// The range to advertise now; `None` until the node knows a tip.
    fn range(&self) -> Option<AdvertisedRange> {
        let tip = (*self.tip.borrow())?;
        // Blocks below `lowest` are not this peer's to ask for.
        let held = (*self.held.borrow()).filter(|(_, last)| last.number >= self.lowest);
        Some(held.map_or(
            AdvertisedRange {
                earliest: tip.number,
                latest: tip,
            },
            |(first, last)| AdvertisedRange {
                earliest: first.number.max(self.lowest),
                latest: last,
            },
        ))
    }

    /// Handles a message from the peer if it is a request. Never waits: within the limits the
    /// request goes to the server, otherwise the peer gets an empty answer.
    pub(crate) fn request(&mut self, message_id: u8, body: &[u8]) -> Handled {
        let kind = match message_id {
            wire::GET_BLOCK_HEADERS => Some(ServeKind::Headers),
            wire::GET_BLOCK_BODIES => Some(ServeKind::Bodies),
            wire::GET_RECEIPTS => Some(ServeKind::Receipts),
            // This node has no transaction pool.
            wire::GET_POOLED_TRANSACTIONS => None,
            _ => return Handled::NotARequest,
        };
        let response_id = kind.map_or(wire::POOLED_TRANSACTIONS, response_id);
        let Some(request_id) = wire::request_id(body) else {
            return Handled::NotARequest;
        };
        let empty = || Handled::Now(response(response_id, request_id, &[]));
        let Some(kind) = kind else {
            return empty();
        };
        if body.len() > MAX_REQUEST_BYTES {
            return empty();
        }
        if !self.within_rate() {
            return empty();
        }
        // No free place means the peer already has its share of requests being answered.
        let Ok(answer) = self.answers.clone().try_reserve_owned() else {
            return empty();
        };
        let request = Request {
            kind,
            lowest: self.lowest,
            version: self.version,
            id: request_id,
            body: Bytes::copy_from_slice(body),
            answer,
        };
        // Full: the server is behind. Closed: it has stopped.
        if self.requests.try_send(request).is_err() {
            return empty();
        }
        Handled::Later
    }

    /// The `BlockRangeUpdate` to send now, if the range to advertise changed since the peer was
    /// last told and [`RANGE_UPDATE_INTERVAL`] has passed. The tip moves with every block, so in
    /// practice one goes out every interval.
    pub(crate) fn range_update(&mut self) -> Option<Bytes> {
        if self.version < EthVersion::Eth69 || self.advertised_at.elapsed() < RANGE_UPDATE_INTERVAL
        {
            return None;
        }
        let range = self.range()?;
        if self.advertised == Some(range) {
            return None;
        }
        self.advertised = Some(range);
        self.advertised_at = Instant::now();
        let update = BlockRangeUpdate {
            earliest: range.earliest,
            latest: range.latest.number,
            latest_hash: range.latest.hash,
        };
        let mut out = Vec::with_capacity(update.length().saturating_add(1));
        out.push(wire::BLOCK_RANGE_UPDATE);
        update.encode(&mut out);
        Some(out.into())
    }

    /// Counts one request against the per-peer limit; `false` once the window is used up.
    fn within_rate(&mut self) -> bool {
        if self.window_start.elapsed() >= RATE_WINDOW {
            self.window_start = Instant::now();
            self.window_requests = 0;
        }
        if self.window_requests >= MAX_REQUESTS_PER_WINDOW {
            return false;
        }
        self.window_requests = self.window_requests.saturating_add(1);
        true
    }
}
