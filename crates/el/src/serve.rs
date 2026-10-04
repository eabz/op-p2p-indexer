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

use alloy_primitives::{BlockHash, BlockNumber, Bytes};
use alloy_rlp::{Decodable, Encodable, Header};
use op_alloy_consensus::{OpReceipt, OpReceiptEnvelope};
use op_indexer_primitives::BlockRef;
use reth_eth_wire_types::message::RequestPair;
use reth_eth_wire_types::{
    BlockHashOrNumber, BlockRangeUpdate, GetBlockBodies, GetBlockHeaders, GetReceipts,
    HeadersDirection,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

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

/// Shortest time between two `BlockRangeUpdate`s to one peer. The range moves with every
/// block; peers only need it roughly (reth announces once per epoch, about six minutes).
const RANGE_UPDATE_INTERVAL: Duration = Duration::from_mins(1);

/// The first and the last block held.
pub(crate) type HeldRange = (BlockRef, BlockRef);

/// The range a session tells its peer, in the status and in `BlockRangeUpdate`: from the first
/// block held to the tip the node knows (the tip alone when nothing is held).
///
/// It is honest at both ends, and complete once the blocks held reach the tip. Until then
/// (during an import, and for the few newest blocks, which are not yet committed) blocks
/// inside it are not held, and requests for them get empty answers. The end is the tip rather
/// than the last block held because peers end a session whose status does not look like a
/// live node's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdvertisedRange {
    pub(crate) earliest: BlockNumber,
    pub(crate) latest: BlockRef,
}

/// The blocks this node can serve: one contiguous range, each block in its consensus encoding.
///
/// The binary implements it over its local archive. The bytes it returns are sent to peers as
/// they are (receipts without their blooms), so they must be the block's original encoding.
/// Implemented for `Option<P>`, where `None` holds nothing.
pub trait BlockProvider: fmt::Debug + Send + Sync + 'static {
    /// Why a read failed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the RLP of the header of block `number`, or `None` if it is not held.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn header(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

    /// Returns the RLP of the body of block `number` (transactions, ommers, optional
    /// withdrawals: one `BlockBodies` entry), or `None` if it is not held.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn body(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

    /// Returns the receipts of block `number` as an RLP list, each receipt in network encoding
    /// with its bloom, or `None` if the block or its receipts are not held.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn receipts(
        &self,
        number: BlockNumber,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

    /// Returns the number of the held block with this hash.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn number_of(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<BlockNumber>, Self::Error>> + Send;

    /// Returns the first and the last block held, or `None` if nothing is held.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn range(&self) -> impl Future<Output = Result<Option<HeldRange>, Self::Error>> + Send;
}

impl<P: BlockProvider> BlockProvider for Option<P> {
    type Error = P::Error;

    async fn header(&self, number: BlockNumber) -> Result<Option<Bytes>, Self::Error> {
        match self {
            Some(provider) => provider.header(number).await,
            None => Ok(None),
        }
    }

    async fn body(&self, number: BlockNumber) -> Result<Option<Bytes>, Self::Error> {
        match self {
            Some(provider) => provider.body(number).await,
            None => Ok(None),
        }
    }

    async fn receipts(&self, number: BlockNumber) -> Result<Option<Bytes>, Self::Error> {
        match self {
            Some(provider) => provider.receipts(number).await,
            None => Ok(None),
        }
    }

    async fn number_of(&self, hash: BlockHash) -> Result<Option<BlockNumber>, Self::Error> {
        match self {
            Some(provider) => provider.number_of(hash).await,
            None => Ok(None),
        }
    }

    async fn range(&self) -> Result<Option<HeldRange>, Self::Error> {
        match self {
            Some(provider) => provider.range().await,
            None => Ok(None),
        }
    }
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
        let latest = (*self.tip.borrow())?;
        let held = *self.held.borrow();
        Some(AdvertisedRange {
            earliest: held.map_or(latest.number, |(first, _)| first.number.min(latest.number)),
            latest,
        })
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
    provider: Arc<P>,
    requests: mpsc::Receiver<Request>,
    range: watch::Sender<Option<HeldRange>>,
}

/// Builds the server over `provider` and what sessions use to reach it. The held range is
/// unknown (nothing held) until the server runs.
pub(crate) fn new<P: BlockProvider>(provider: P) -> (Server<P>, Serving) {
    let (requests_tx, requests_rx) = mpsc::channel(MAX_QUEUED);
    let (range_tx, range_rx) = watch::channel(None);
    let server = Server {
        provider: Arc::new(provider),
        requests: requests_rx,
        range: range_tx,
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
                    answering.spawn(answer(Arc::clone(&self.provider), request));
                }
            }
        }
    }

    /// Reads the held range from the provider. A failed read keeps the last one.
    async fn refresh_range(&self) {
        match self.provider.range().await {
            Ok(held) => {
                self.range.send_if_modified(|current| {
                    let changed = *current != held;
                    *current = held;
                    changed
                });
            }
            Err(err) => warn!(%err, "could not read the range of blocks to serve"),
        }
    }
}

/// Why gathering an answer stopped early. What was gathered before is still sent.
#[derive(Debug)]
enum Fault<E> {
    /// The request could not be decoded.
    Malformed(alloy_rlp::Error),
    /// The provider failed.
    Provider(E),
    /// Held receipts could not be decoded.
    Stored(alloy_rlp::Error),
}

/// The items of one response, with the limits on their number and size.
#[derive(Debug, Default)]
struct Items {
    list: Vec<Bytes>,
    bytes: usize,
}

impl Items {
    /// Adds an item. Returns whether the response may take another one.
    fn push(&mut self, item: Bytes) -> bool {
        self.bytes = self.bytes.saturating_add(item.len());
        self.list.push(item);
        self.list.len() < MAX_ITEMS && self.bytes < SOFT_RESPONSE_BYTES
    }
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
async fn answer<P: BlockProvider>(provider: Arc<P>, request: Request) {
    let Request {
        kind,
        id,
        body,
        answer,
    } = request;
    let mut items = Items::default();
    let outcome = match gather(&*provider, kind, &body, &mut items).await {
        Ok(()) if items.list.is_empty() => ServeOutcome::Empty,
        Ok(()) => ServeOutcome::Answered,
        Err(Fault::Malformed(err)) => {
            debug!(?kind, %err, "malformed request from an execution peer");
            ServeOutcome::Malformed
        }
        Err(Fault::Provider(err)) => {
            warn!(?kind, %err, "could not read blocks to serve");
            ServeOutcome::Failed
        }
        Err(Fault::Stored(err)) => {
            warn!(?kind, %err, "held receipts could not be decoded");
            ServeOutcome::Failed
        }
    };
    metrics::served(kind, outcome);
    metrics::served_items(kind, items.list.len(), items.bytes);
    // If the session has ended, the answer is dropped with its channel.
    drop(answer.send(response(response_id(kind), id, &items.list)));
}

/// Gathers the items answering the request in `body` into `items`, up to its limits. Stops at
/// the first block that is not held: a response is a run of blocks, not a selection.
async fn gather<P: BlockProvider>(
    provider: &P,
    kind: ServeKind,
    mut body: &[u8],
    items: &mut Items,
) -> Result<(), Fault<P::Error>> {
    let hashes = match kind {
        ServeKind::Headers => {
            let request = RequestPair::<GetBlockHeaders>::decode(&mut body)
                .map_err(Fault::Malformed)?
                .message;
            return headers(provider, &request, items)
                .await
                .map_err(Fault::Provider);
        }
        ServeKind::Bodies => {
            RequestPair::<GetBlockBodies>::decode(&mut body).map(|pair| pair.message.0)
        }
        ServeKind::Receipts => {
            RequestPair::<GetReceipts>::decode(&mut body).map(|pair| pair.message.0)
        }
    }
    .map_err(Fault::Malformed)?;
    for hash in hashes {
        let number = provider.number_of(hash).await.map_err(Fault::Provider)?;
        let Some(number) = number else { break };
        let item = if matches!(kind, ServeKind::Receipts) {
            let stored = provider.receipts(number).await.map_err(Fault::Provider)?;
            stored
                .map(|stored| without_blooms(&stored))
                .transpose()
                .map_err(Fault::Stored)?
        } else {
            provider.body(number).await.map_err(Fault::Provider)?
        };
        let Some(item) = item else { break };
        if !items.push(item) {
            break;
        }
    }
    Ok(())
}

/// Gathers the headers `request` asks for: from its start, by number or hash, every
/// `skip + 1`-th block in its direction.
async fn headers<P: BlockProvider>(
    provider: &P,
    request: &GetBlockHeaders,
    items: &mut Items,
) -> Result<(), P::Error> {
    let mut next = match request.start_block {
        BlockHashOrNumber::Hash(hash) => provider.number_of(hash).await?,
        BlockHashOrNumber::Number(number) => Some(number),
    };
    // The held blocks are one chain, so walking it is walking the numbers.
    let step = u64::from(request.skip).saturating_add(1);
    // `limit` is the peer's; `Items::push` ends the walk at ours.
    for _ in 0..request.limit {
        let Some(number) = next else { break };
        let Some(header) = provider.header(number).await? else {
            break;
        };
        if !items.push(header) {
            break;
        }
        next = match request.direction {
            HeadersDirection::Rising => number.checked_add(step),
            HeadersDirection::Falling => number.checked_sub(step),
        };
    }
    Ok(())
}

/// Converts a block's receipts from the held form to the eth/69 one ([EIP-7642]).
///
/// Held: an RLP list of receipts in network encoding, `status, cumulative-gas, bloom, logs`,
/// and for a deposit receipt its nonce and version when it has them. Sent: per receipt
/// `[tx-type, status, cumulative-gas, logs]`, followed by the same deposit nonce and version.
/// The only field dropped is the bloom, which the receiver rebuilds from the logs; the type
/// (legacy included, as type 0), the status or post-state root, the gas, every log and the
/// deposit fields are carried over as they are. A few milliseconds of CPU for the largest
/// response.
///
/// [EIP-7642]: https://eips.ethereum.org/EIPS/eip-7642
fn without_blooms(stored: &[u8]) -> alloy_rlp::Result<Bytes> {
    let receipts = Vec::<OpReceiptEnvelope>::decode(&mut &*stored)?;
    let receipts: Vec<OpReceipt> = receipts.into_iter().map(OpReceipt::from).collect();
    // Without the blooms the list is smaller than it was.
    let mut out = Vec::with_capacity(stored.len());
    alloy_rlp::encode_list(&receipts, &mut out);
    Ok(out.into())
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
