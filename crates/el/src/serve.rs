//! Serving: answers peers' requests for headers, bodies and receipts from the blocks this node
//! holds, so another node can sync from it.
//!
//! ```text
//! session driver ─ try_send ─▶ Server ─▶ BlockProvider (the binary: the local archive)
//!        ▲                        │
//!        └──── answer (bytes) ────┘
//! ```
//!
//! - [`BlockProvider`] is what the binary implements over its store; `el` does not know it.
//! - [`Server`] is the one task that reads from the provider. It also keeps the held range
//!   current; its first block starts the range the status and `BlockRangeUpdate` advertise.
//! - [`SessionServing`] is a session's side: it applies the per-peer limits and hands a
//!   request to the server without waiting.
//!
//! **Bytes.** A header and a body go on the wire exactly as the provider returns them: they
//! are copied into the response, never decoded. Receipts are held with their blooms (the form
//! up to eth/68) and eth/69 sends them without, so they are decoded and encoded again; see
//! [`without_blooms`] for what that drops.
//!
//! **The tip fetcher is never delayed.** A session driver does not wait for the provider: it
//! hands the request over with `try_send` and writes the answer when it arrives. The server is
//! its own task, with its own bound on reads in progress. What a session does share is its
//! connection: a peer that reads a large answer slowly holds up its own session only.
//!
//! Over a limit, and for anything not held, the peer gets an empty answer, which is how a node
//! says "not from me". Does not verify what it serves: the provider's blocks were verified
//! when they were stored.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{BlockNumber, Bytes};
use alloy_rlp::{Decodable, Encodable, Header};
use op_alloy_consensus::{OpReceipt, OpReceiptEnvelope};
use op_indexer_primitives::{BlockRead, BlockRef, BlockStart, ItemConvert, ReadLimits};
use reth_eth_wire_types::message::RequestPair;
use reth_eth_wire_types::{
    BlockHashOrNumber, BlockRangeUpdate, GetBlockBodies, GetBlockHeaders, GetReceipts,
    HeadersDirection,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::ElError;
use crate::metrics::{self, ServeKind, ServeOutcome};
use crate::wire;

/// Most headers, bodies or blocks of receipts in one response: what reth and geth serve.
const MAX_ITEMS: usize = 1024;

/// A response stops growing once it passes this size: the soft limit of the eth protocol, well
/// under the 10 MiB hard limit on a message, so the block that crosses it still fits.
const SOFT_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

/// Requests answered from the provider per peer per [`RATE_WINDOW`]; further ones get an empty
/// answer. Two a second, each up to [`SOFT_RESPONSE_BYTES`], is more than a syncing node asks
/// of one peer, and bounds what one peer can make this node read.
const MAX_REQUESTS_PER_WINDOW: u32 = 120;

/// The window of the per-peer request limit.
const RATE_WINDOW: Duration = Duration::from_mins(1);

/// Requests of one peer being answered at once; further ones get an empty answer. A syncing
/// node keeps a few requests open per peer.
const MAX_IN_FLIGHT_PER_PEER: usize = 4;

/// Requests of all peers waiting for the server; further ones get an empty answer. A few
/// seconds of work at most, so no peer waits into its own request timeout.
const MAX_QUEUED: usize = 64;

/// Requests read from the provider at once. Each read runs on a blocking thread in the
/// provider; this keeps serving to a few of them whatever the number of peers.
const MAX_CONCURRENT: usize = 4;

/// How often the held range is read from the provider.
const RANGE_REFRESH: Duration = Duration::from_secs(10);

/// Largest request body read: a request for [`MAX_ITEMS`] hashes (33 bytes each as RLP), its
/// list headers and its request id. A larger request asks for more than is ever answered, and
/// is refused before it is copied or decoded: a 10 MiB request holds 300,000 hashes.
const MAX_REQUEST_BYTES: usize = MAX_ITEMS * 33 + 32;

/// Shortest time between two `BlockRangeUpdate`s to one peer. The range moves with every
/// block; peers only need it roughly (reth announces once per epoch, about six minutes).
const RANGE_UPDATE_INTERVAL: Duration = Duration::from_mins(1);

/// The first and the last block held.
pub(crate) type HeldRange = (BlockRef, BlockRef);

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

/// The blocks this node can serve: one contiguous range, each block in its consensus encoding.
///
/// The binary implements it over its local archive. The bytes it returns are sent to peers as
/// they are (receipts without their blooms), so they must be the block's original encoding.
pub trait BlockProvider: fmt::Debug + Send + Sync + 'static {
    /// Why a read failed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Reads a run of headers, bodies or receipts (each block's receipts as an RLP list, every
    /// receipt in network encoding with its bloom), each item as the RLP held, ending at the
    /// first block not held and at `limits`. One call answers one request, so it should be one
    /// read of the store, off the async runtime.
    ///
    /// With `convert`, each item is passed through it, on the thread of the read, before it
    /// counts against the limits and is returned; an item it returns `None` for ends the run.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> impl Future<Output = Result<Vec<Bytes>, Self::Error>> + Send;

    /// Returns the first and the last block held, or `None` if nothing is held.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn range(&self) -> impl Future<Output = Result<Option<HeldRange>, Self::Error>> + Send;
}

/// A peer's request on its way to the server.
#[derive(Debug)]
struct Request {
    kind: ServeKind,
    id: u64,
    /// The request without its message id byte.
    body: Bytes,
    /// Where the answer goes: a reserved place in the session's answer channel.
    answer: mpsc::OwnedPermit<Bytes>,
}

/// What every session needs to serve: the way to the server and the range it holds.
#[derive(Debug, Clone)]
pub(crate) struct Serving {
    requests: mpsc::Sender<Request>,
    range: watch::Receiver<Option<HeldRange>>,
}

impl Serving {
    /// The serving side of one new session, and the channel its answers arrive on. `tip`
    /// follows the newest block the node knows. The session is taken to advertise the current
    /// range in its status.
    pub(crate) fn session(
        &self,
        tip: watch::Receiver<Option<BlockRef>>,
    ) -> (SessionServing, mpsc::Receiver<Bytes>) {
        let (answers_tx, answers_rx) = mpsc::channel(MAX_IN_FLIGHT_PER_PEER);
        let now = Instant::now();
        let mut session = SessionServing {
            requests: self.requests.clone(),
            answers: answers_tx,
            held: self.range.clone(),
            tip,
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
        let held = *self.held.borrow();
        Some(held.map_or(
            AdvertisedRange {
                earliest: tip.number,
                latest: tip,
            },
            |(first, last)| AdvertisedRange {
                earliest: first.number,
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
            metrics::served(kind, ServeOutcome::Malformed);
            return empty();
        }
        if !self.within_rate() {
            metrics::served(kind, ServeOutcome::RateLimited);
            return empty();
        }
        // No free place means the peer already has its share of requests being answered.
        let Ok(answer) = self.answers.clone().try_reserve_owned() else {
            metrics::served(kind, ServeOutcome::Busy);
            return empty();
        };
        let request = Request {
            kind,
            id: request_id,
            body: Bytes::copy_from_slice(body),
            answer,
        };
        // Full: the server is behind. Closed: it has stopped.
        if self.requests.try_send(request).is_err() {
            metrics::served(kind, ServeOutcome::Busy);
            return empty();
        }
        Handled::Later
    }

    /// The `BlockRangeUpdate` to send now, if the range to advertise changed since the peer was
    /// last told and [`RANGE_UPDATE_INTERVAL`] has passed. The tip moves with every block, so in
    /// practice one goes out every interval.
    pub(crate) fn range_update(&mut self) -> Option<Bytes> {
        if self.advertised_at.elapsed() < RANGE_UPDATE_INTERVAL {
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

/// The task that answers requests from the provider and keeps the held range current.
#[derive(Debug)]
pub(crate) struct Server<P> {
    /// `None` when the node holds no blocks to serve: every request is answered empty.
    provider: Option<Arc<P>>,
    requests: mpsc::Receiver<Request>,
    range: watch::Sender<Option<HeldRange>>,
    /// Whether what is advertised has been logged once.
    logged: bool,
}

/// Builds the server over `provider` (`None`: nothing to serve) and what sessions use to
/// reach it. The held range is unknown (nothing held) until the server runs.
pub(crate) fn new<P: BlockProvider>(provider: Option<P>) -> (Server<P>, Serving) {
    let (requests_tx, requests_rx) = mpsc::channel(MAX_QUEUED);
    let (range_tx, range_rx) = watch::channel(None);
    let server = Server {
        provider: provider.map(Arc::new),
        requests: requests_rx,
        range: range_tx,
        logged: false,
    };
    let serving = Serving {
        requests: requests_tx,
        range: range_rx,
    };
    (server, serving)
}

impl<P: BlockProvider> Server<P> {
    /// Answers requests until `cancel` fires. Requests being answered then are dropped: their
    /// sessions are ending too.
    ///
    /// # Errors
    ///
    /// Returns [`ElError::Task`] if answering a request panicked.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), ElError> {
        let mut refresh = interval(RANGE_REFRESH);
        refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut answering = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(joined) = answering.join_next(), if !answering.is_empty() => {
                    if let Err(source) = joined {
                        return Err(ElError::Task { task: "serve request", source });
                    }
                }
                _ = refresh.tick() => self.refresh_range().await,
                request = self.requests.recv(), if answering.len() < MAX_CONCURRENT => {
                    // Closed: every session and the context are gone.
                    let Some(request) = request else { return Ok(()) };
                    answering.spawn(answer(self.provider.clone(), request));
                }
            }
        }
    }

    /// Reads the held range from the provider. A failed read keeps the last one.
    async fn refresh_range(&mut self) {
        let held = match &self.provider {
            Some(provider) => provider.range().await,
            None => Ok(None),
        };
        match held {
            Ok(held) => {
                let before = self.range.send_replace(held);
                // The last block moves with every promotion; what is worth a line is the
                // kind of range advertised and where it starts.
                let start = |range: Option<HeldRange>| range.map(|(first, _)| first.number);
                if !self.logged || start(before) != start(held) {
                    self.logged = true;
                    if let Some((first, last)) = held {
                        info!(
                            earliest = first.number,
                            latest = last.number,
                            hash = %last.hash,
                            "execution peers are told the range of blocks held, with its last \
                             block as the head"
                        );
                    } else {
                        info!(
                            "no blocks are held to serve; execution peers are told the tip alone"
                        );
                    }
                }
            }
            Err(err) => warn!(%err, "could not read the range of blocks to serve"),
        }
    }
}

/// Why a request got no items.
#[derive(Debug)]
enum Fault<E> {
    /// The request could not be decoded.
    Malformed(alloy_rlp::Error),
    /// The provider failed.
    Provider(E),
}

/// The message id of the response to a request of `kind`.
const fn response_id(kind: ServeKind) -> u8 {
    match kind {
        ServeKind::Headers => wire::BLOCK_HEADERS,
        ServeKind::Bodies => wire::BLOCK_BODIES,
        ServeKind::Receipts => wire::RECEIPTS,
    }
}

/// Answers one request from `provider` and hands the answer to its session.
async fn answer<P: BlockProvider>(provider: Option<Arc<P>>, request: Request) {
    let Request {
        kind,
        id,
        body,
        answer,
    } = request;
    let items = match provider {
        Some(provider) => gather(&*provider, kind, &body).await,
        None => Ok(Vec::new()),
    };
    let (items, outcome) = match items {
        Ok(items) if items.is_empty() => (items, ServeOutcome::Empty),
        Ok(items) => (items, ServeOutcome::Answered),
        Err(Fault::Malformed(err)) => {
            debug!(?kind, %err, "malformed request from an execution peer");
            (Vec::new(), ServeOutcome::Malformed)
        }
        Err(Fault::Provider(err)) => {
            warn!(?kind, %err, "could not read blocks to serve");
            (Vec::new(), ServeOutcome::Failed)
        }
    };
    let bytes = items
        .iter()
        .fold(0_usize, |sum, item| sum.saturating_add(item.len()));
    metrics::served(kind, outcome);
    metrics::served_items(kind, items.len(), bytes);
    // If the session has ended, the answer is dropped with its channel.
    drop(answer.send(response(response_id(kind), id, &items)));
}

/// Reads the items answering the request in `body`: one call of the provider, which applies
/// the limits and ends the run at the first block that is not held (a response is a run of
/// blocks, not a selection).
async fn gather<P: BlockProvider>(
    provider: &P,
    kind: ServeKind,
    mut body: &[u8],
) -> Result<Vec<Bytes>, Fault<P::Error>> {
    let mut limits = ReadLimits {
        items: MAX_ITEMS,
        bytes: SOFT_RESPONSE_BYTES,
    };
    let (read, convert): (BlockRead, Option<ItemConvert>) = match kind {
        ServeKind::Headers => {
            let request = RequestPair::<GetBlockHeaders>::decode(&mut body)
                .map_err(Fault::Malformed)?
                .message;
            // The peer's limit, within ours.
            limits.items =
                usize::try_from(request.limit).map_or(MAX_ITEMS, |limit| limit.min(MAX_ITEMS));
            let read = BlockRead::Headers {
                start: match request.start_block {
                    BlockHashOrNumber::Hash(hash) => BlockStart::Hash(hash),
                    BlockHashOrNumber::Number(number) => BlockStart::Number(number),
                },
                // The held blocks are one chain, so walking it is walking the numbers.
                step: u64::from(request.skip).saturating_add(1),
                rising: matches!(request.direction, HeadersDirection::Rising),
            };
            (read, None)
        }
        ServeKind::Bodies => {
            let request = RequestPair::<GetBlockBodies>::decode(&mut body);
            let hashes = request.map_err(Fault::Malformed)?.message.0;
            (BlockRead::Bodies(hashes), None)
        }
        ServeKind::Receipts => {
            let request = RequestPair::<GetReceipts>::decode(&mut body);
            let hashes = request.map_err(Fault::Malformed)?.message.0;
            (BlockRead::Receipts(hashes), Some(without_blooms))
        }
    };
    provider
        .read(read, limits, convert)
        .await
        .map_err(Fault::Provider)
}

/// Converts a block's receipts from the held form to the eth/69 one ([EIP-7642]); `None` if
/// the held list cannot be decoded. Called by the provider, inside its read.
///
/// Held: an RLP list of receipts in network encoding, `status, cumulative-gas, bloom, logs`,
/// and for a deposit receipt its nonce and version when it has them. Sent: per receipt
/// `[tx-type, status, cumulative-gas, logs]`, followed by the same deposit nonce and version.
/// The only field dropped is the bloom, which the receiver rebuilds from the logs; the type
/// (legacy included, as type 0), the status or post-state root, the gas, every log and the
/// deposit fields are carried over as they are.
///
/// [EIP-7642]: https://eips.ethereum.org/EIPS/eip-7642
fn without_blooms(stored: &[u8]) -> Option<Bytes> {
    let receipts = Vec::<OpReceiptEnvelope>::decode(&mut &*stored)
        .inspect_err(|err| warn!(%err, "held receipts could not be decoded; not served"))
        .ok()?;
    let receipts: Vec<OpReceipt> = receipts.into_iter().map(OpReceipt::from).collect();
    // Without the blooms the list is smaller than it was.
    let mut out = Vec::with_capacity(stored.len());
    alloy_rlp::encode_list(&receipts, &mut out);
    Some(out.into())
}

/// Encodes a response: the message id, then `[request-id, [item, ...]]` with each item copied
/// in as the RLP it already is.
fn response(message_id: u8, request_id: u64, items: &[Bytes]) -> Bytes {
    let items_length = items
        .iter()
        .fold(0_usize, |sum, item| sum.saturating_add(item.len()));
    let list = Header {
        list: true,
        payload_length: items_length,
    };
    let pair = Header {
        list: true,
        payload_length: request_id
            .length()
            .saturating_add(list.length_with_payload()),
    };
    let mut out = Vec::with_capacity(pair.length_with_payload().saturating_add(1));
    out.push(message_id);
    pair.encode(&mut out);
    request_id.encode(&mut out);
    list.encode(&mut out);
    for item in items {
        out.extend_from_slice(item);
    }
    out.into()
}
