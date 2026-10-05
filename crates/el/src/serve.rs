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

mod provider;
mod session;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::{BlockNumber, Bytes};
use alloy_rlp::{Decodable, Encodable, Header};
use op_alloy_consensus::{OpReceipt, OpReceiptEnvelope};
use op_indexer_primitives::{BlockRead, BlockRef, BlockStart, ItemConvert, ReadLimits};
use reth_eth_wire_types::message::RequestPair;
use reth_eth_wire_types::{
    BlockHashOrNumber, EthVersion, GetBlockBodies, GetBlockHeaders, GetReceipts, HeadersDirection,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};

pub use self::provider::BlockProvider;
use self::provider::HeldRange;
pub(crate) use self::session::{Handled, Serving, SessionServing};
use crate::ElError;
use crate::warn_limit::WarnLimit;
use crate::wire;

/// Most headers, bodies or blocks of receipts in one response: what reth and geth serve.
const MAX_ITEMS: usize = 1024;

/// A response stops growing once it passes this size: the soft limit of the eth protocol, well
/// under the 10 MiB hard limit on a message, so the block that crosses it still fits.
const SOFT_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

/// Requests of all peers waiting for the server; further ones get an empty answer. A few
/// seconds of work at most, so no peer waits into its own request timeout.
const MAX_QUEUED: usize = 64;

/// Requests read from the provider at once. Each read runs on a blocking thread in the
/// provider; this keeps serving to a few of them whatever the number of peers.
const MAX_CONCURRENT: usize = 4;

/// Shortest time between two reads of the held range when the head moves: a block or two.
const HEAD_REFRESH: Duration = Duration::from_secs(2);
/// How often the held range is read from the provider.
const RANGE_REFRESH: Duration = Duration::from_secs(10);
/// Reads of the held range failed in a row after which requests are answered empty without
/// reading, until one succeeds again: about 80 s of a provider that cannot be read. Only that
/// read counts: it touches no particular block, so one corrupt stored item that peers keep
/// asking for fails their requests, not serving as a whole.
const MAX_FAILURES: u32 = 8;
/// Shortest time between two warnings about the same kind of failed read.
const FAILURE_WARN_INTERVAL: Duration = Duration::from_mins(1);

/// What this node served on one execution network between two status lines of its peer set
/// (one a minute): requests answered with blocks, by kind, and those that got none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionServed {
    /// `GetBlockHeaders` answered with at least one header.
    pub headers: u32,
    /// `GetBlockBodies` answered with at least one body.
    pub bodies: u32,
    /// `GetReceipts` answered with the receipts of at least one block.
    pub receipts: u32,
    /// Headers, bodies and blocks of receipts sent, in all.
    pub items: u32,
    /// Bytes of the answers with items.
    pub bytes: u64,
    /// Requests answered empty after a read: nothing asked for is held, the request did not
    /// decode, or the store could not be read.
    pub empty: u32,
    /// Requests answered empty without a read: over a per-peer or server limit, too large,
    /// for transactions (this node has no pool), or on a network the node does not serve.
    pub refused: u32,
    /// Peers with at least one request taken for reading.
    pub peers: u32,
}

impl ExecutionServed {
    /// Requests answered with blocks, of every kind.
    #[must_use]
    pub const fn requests(&self) -> u32 {
        self.headers
            .saturating_add(self.bodies)
            .saturating_add(self.receipts)
    }
}

/// The counts behind [`ExecutionServed`], kept as they happen: atomics, no lock. Shared by the
/// sessions, the server and the peer set, which takes them once a minute.
#[derive(Debug, Clone, Default)]
pub(crate) struct ServeCounters(Arc<Counters>);

#[derive(Debug, Default)]
struct Counters {
    /// Requests answered with items, by kind.
    headers: AtomicU32,
    bodies: AtomicU32,
    receipts: AtomicU32,
    items: AtomicU32,
    bytes: AtomicU64,
    empty: AtomicU32,
    refused: AtomicU32,
    peers: AtomicU32,
    /// Advances at each [`ServeCounters::take`]: a session counts itself in [`Self::peers`]
    /// once per value.
    minute: AtomicU64,
    /// The last minute taken.
    last: watch::Sender<ExecutionServed>,
}

impl ServeCounters {
    /// A request answered after a read: with `items` and `bytes`, or empty.
    fn answered(&self, kind: ServeKind, items: usize, bytes: usize) {
        let counters = &self.0;
        if items == 0 {
            counters.empty.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let answered = match kind {
            ServeKind::Headers => &counters.headers,
            ServeKind::Bodies => &counters.bodies,
            ServeKind::Receipts => &counters.receipts,
        };
        answered.fetch_add(1, Ordering::Relaxed);
        let items = u32::try_from(items).unwrap_or(u32::MAX);
        counters.items.fetch_add(items, Ordering::Relaxed);
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        counters.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// A request answered empty without a read.
    pub(crate) fn refused(&self) {
        self.0.refused.fetch_add(1, Ordering::Relaxed);
    }

    /// A request taken for reading from a session, which last counted itself as a peer
    /// served in the minute `counted`: counts it once per minute.
    fn taken(&self, counted: &mut Option<u64>) {
        let minute = self.0.minute.load(Ordering::Relaxed);
        if *counted != Some(minute) {
            *counted = Some(minute);
            self.0.peers.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The counts since the last call, which start again from zero; also published to
    /// [`Self::subscribe`].
    pub(crate) fn take(&self) -> ExecutionServed {
        let counters = &self.0;
        counters.minute.fetch_add(1, Ordering::Relaxed);
        let take = |count: &AtomicU32| count.swap(0, Ordering::Relaxed);
        let served = ExecutionServed {
            headers: take(&counters.headers),
            bodies: take(&counters.bodies),
            receipts: take(&counters.receipts),
            items: take(&counters.items),
            bytes: counters.bytes.swap(0, Ordering::Relaxed),
            empty: take(&counters.empty),
            refused: take(&counters.refused),
            peers: take(&counters.peers),
        };
        counters.last.send_replace(served);
        served
    }

    /// The last minute taken, kept current.
    pub(crate) fn subscribe(&self) -> watch::Receiver<ExecutionServed> {
        self.0.last.subscribe()
    }
}

/// How the provider has been doing, shared by the server and the reads it spawns.
#[derive(Debug, Default)]
struct Health {
    /// Reads of the held range failed in a row.
    failures: AtomicU32,
    /// One warning limit per kind of request.
    warned: [WarnLimit; 3],
    /// The warning limit for reads of the held range.
    range_warned: WarnLimit,
}

impl Health {
    /// Whether the held range could not be read [`MAX_FAILURES`] times in a row.
    fn is_failing(&self) -> bool {
        self.failures.load(Ordering::Relaxed) >= MAX_FAILURES
    }

    fn succeeded(&self) {
        self.failures.store(0, Ordering::Relaxed);
    }

    /// Warns about a failed read, at most once a minute per cause (`kind`, or `None` for the
    /// held range, which is also counted towards [`MAX_FAILURES`]).
    fn failed(&self, kind: Option<ServeKind>, err: &dyn std::fmt::Display) {
        let (warned, failures) = if let Some(kind) = kind {
            (
                self.warned.get(kind as usize),
                self.failures.load(Ordering::Relaxed),
            )
        } else {
            let failures = self.failures.fetch_add(1, Ordering::Relaxed);
            (Some(&self.range_warned), failures.saturating_add(1))
        };
        if let Some(held_back) = warned.and_then(|warned| warned.allow(FAILURE_WARN_INTERVAL)) {
            let what = kind.map_or("the range of blocks held", ServeKind::as_str);
            warn!(
                %err,
                held_back,
                failures,
                "could not read {what} to serve; after {MAX_FAILURES} failed reads of the held \
                 range in a row, peers are answered empty until the provider recovers"
            );
        }
    }
}

/// A peer's request on its way to the server.
#[derive(Debug)]
struct Request {
    kind: ServeKind,
    /// Blocks below this are not served to the peer: answered as not held.
    lowest: BlockNumber,
    /// The session's eth version, whose receipts format the answer uses.
    version: EthVersion,
    id: u64,
    /// The request without its message id byte.
    body: Bytes,
    /// Where the answer goes: a reserved place in the session's answer channel.
    answer: mpsc::OwnedPermit<Bytes>,
}

/// The task that answers requests from the provider and keeps the held range current.
#[derive(Debug)]
pub(crate) struct Server<P> {
    provider: Arc<P>,
    /// The newest block the node knows: the held range is read again when it moves.
    head: watch::Receiver<Option<BlockRef>>,
    requests: mpsc::Receiver<Request>,
    range: watch::Sender<Option<HeldRange>>,
    /// Whether what is advertised has been logged once.
    logged: bool,
    health: Arc<Health>,
    counters: ServeCounters,
}

/// Builds the server over `provider` and what sessions use to reach it. The held range is
/// unknown (nothing held) until the server runs, and is read again whenever `head` moves.
pub(crate) fn new<P: BlockProvider>(
    provider: P,
    head: watch::Receiver<Option<BlockRef>>,
) -> (Server<P>, Serving) {
    let (requests_tx, requests_rx) = mpsc::channel(MAX_QUEUED);
    let (range_tx, range_rx) = watch::channel(None);
    let counters = ServeCounters::default();
    let server = Server {
        provider: Arc::new(provider),
        head,
        requests: requests_rx,
        range: range_tx,
        logged: false,
        health: Arc::default(),
        counters: counters.clone(),
    };
    let serving = Serving {
        requests: requests_tx,
        range: range_rx,
        enabled: true,
        counters,
    };
    (server, serving)
}

/// What sessions use when nothing serves: every request is answered empty and no range is
/// held, so the tip alone is advertised.
pub(crate) fn disabled() -> Serving {
    // The receivers are dropped at once: a request finds the queue closed, and the range
    // keeps its initial "nothing held".
    let (requests, _) = mpsc::channel(1);
    let (_, range) = watch::channel(None);
    Serving {
        requests,
        range,
        enabled: false,
        counters: ServeCounters::default(),
    }
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
        let mut last_read = tokio::time::Instant::now();
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(joined) = answering.join_next(), if !answering.is_empty() => {
                    if let Err(source) = joined {
                        return Err(ElError::Task { task: "serve request", source });
                    }
                }
                _ = refresh.tick() => {
                    self.refresh_range().await;
                    last_read = tokio::time::Instant::now();
                }
                // The held range ends at the head: it is read again as the head moves, at
                // most once per `HEAD_REFRESH`. A closed head means the binary is stopping;
                // the periodic refresh carries on.
                Ok(()) = self.head.changed(), if last_read.elapsed() >= HEAD_REFRESH => {
                    self.refresh_range().await;
                    last_read = tokio::time::Instant::now();
                }
                request = self.requests.recv(), if answering.len() < MAX_CONCURRENT => {
                    // Closed: every session and the context are gone.
                    let Some(request) = request else { return Ok(()) };
                    let (provider, health) = (Arc::clone(&self.provider), Arc::clone(&self.health));
                    let counters = self.counters.clone();
                    answering.spawn(answer(provider, health, counters, request).in_current_span());
                }
            }
        }
    }

    /// Reads the held range from the provider. A failed read keeps the last one: nothing new
    /// is advertised while the provider fails.
    async fn refresh_range(&mut self) {
        let held = self.provider.range().await;
        match held {
            Ok(held) => {
                self.health.succeeded();
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
            Err(err) => self.health.failed(None, &err),
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

/// What a peer asked for.
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

/// The message id of the response to a request of `kind`.
const fn response_id(kind: ServeKind) -> u8 {
    match kind {
        ServeKind::Headers => wire::BLOCK_HEADERS,
        ServeKind::Bodies => wire::BLOCK_BODIES,
        ServeKind::Receipts => wire::RECEIPTS,
    }
}

/// Answers one request from `provider` and hands the answer to its session.
/// While the provider is failing, answers empty without reading.
async fn answer<P: BlockProvider>(
    provider: Arc<P>,
    health: Arc<Health>,
    counters: ServeCounters,
    request: Request,
) {
    let Request {
        kind,
        lowest,
        version,
        id,
        body,
        answer,
    } = request;
    let items = if health.is_failing() {
        Ok(Vec::new())
    } else {
        gather(&*provider, kind, lowest, version, &body).await
    };
    let items = match items {
        Ok(items) => items,
        Err(Fault::Malformed(err)) => {
            debug!(?kind, %err, "malformed request from an execution peer");
            Vec::new()
        }
        Err(Fault::Provider(err)) => {
            health.failed(Some(kind), &err);
            Vec::new()
        }
    };
    let response = response(response_id(kind), id, &items);
    counters.answered(kind, items.len(), response.len());
    // If the session has ended, the answer is dropped with its channel.
    drop(answer.send(response));
}

/// Reads the items answering the request in `body`: one call of the provider, which applies
/// the limits and ends the run at the first block that is not held, or is below `lowest` (a
/// response is a run of blocks, not a selection).
async fn gather<P: BlockProvider>(
    provider: &P,
    kind: ServeKind,
    lowest: BlockNumber,
    version: EthVersion,
    mut body: &[u8],
) -> Result<Vec<Bytes>, Fault<P::Error>> {
    let mut limits = ReadLimits {
        items: MAX_ITEMS,
        bytes: SOFT_RESPONSE_BYTES,
        lowest,
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
            // Held with their blooms, as eth/68 sends them; eth/69 drops the bloom.
            let convert = (version >= EthVersion::Eth69).then_some(without_blooms as ItemConvert);
            (BlockRead::Receipts(hashes), convert)
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
