//! A live session: the driver that owns the stream and the handle requests go through.
//!
//! The driver answers pings, passes the peer's requests to the server (see `serve`) and writes
//! its answers, announces the block range this node serves, follows the one the peer announces
//! and routes responses by request id. Does not open sessions
//! (see `handshake`) and verifies nothing a peer returns.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use alloy_primitives::{B256, Bytes};
use futures_util::{SinkExt, StreamExt};
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::P2PStream;
use reth_eth_wire::errors::P2PStreamError;
use reth_eth_wire_types::DisconnectReason;
use reth_network_peers::PeerId;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use super::handshake::PeerStatus;
use crate::serve::{Handled, SessionServing};
use crate::wire::{self, Request};

/// Limit for one request; receipts answered within a second in the viability test.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Limit for writing one message to a peer. A peer that asks for data and does not read it
/// would otherwise hold its session, and the answer, for ever.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Limit for telling a peer why we disconnect.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// How often the stream is flushed while idle, so pongs to the peer's pings go out.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Requests waiting to be written to one session. The fetcher sends one at a time, so this
/// only bounds a misbehaving caller.
const COMMAND_CAPACITY: usize = 8;

/// Largest message accepted from a peer. A block's receipts are well under this; reth's
/// stream allows 16 MiB.
const MAX_MESSAGE_BYTES: usize = 10 * 1024 * 1024;

type Stream = P2PStream<ECIESStream<TcpStream>>;

/// The blocks a peer says it serves. It covers headers and bodies; receipts may reach less far
/// back, which a peer does not announce.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockRange {
    /// The first block.
    pub earliest: u64,
    /// The last block.
    pub latest: u64,
}

/// Why a request got no usable answer.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// The peer did not answer in time.
    #[error("request timed out")]
    Timeout,
    /// The session ended before the answer.
    #[error("session closed")]
    SessionClosed,
    /// The answer could not be decoded.
    #[error("malformed response: {0}")]
    Malformed(String),
    /// The answer holds more items than were asked for: a peer fault.
    #[error("{got} items answered, {asked} asked for")]
    Excess {
        /// Items asked for.
        asked: usize,
        /// Items in the answer.
        got: usize,
    },
}

/// How a session ended.
#[derive(Debug)]
pub(crate) struct SessionEnd {
    pub(crate) peer_id: PeerId,
    pub(crate) reason: EndReason,
    /// How long the session lasted after the handshake.
    pub(crate) lasted: Duration,
}

/// Why a session ended.
#[derive(Debug)]
pub(crate) enum EndReason {
    /// The node is shutting down, or the session was ended through its handle.
    Cancelled,
    /// The peer told us why it left.
    PeerDisconnected(DisconnectReason),
    /// The peer closed the connection without a reason.
    Closed,
    /// The connection failed.
    Io(String),
    /// The peer broke the protocol (bad framing, oversized or malformed message).
    Protocol(String),
    /// The peer did not read what we sent within [`WRITE_TIMEOUT`].
    Stalled,
}

impl fmt::Display for EndReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("ended by us"),
            Self::PeerDisconnected(reason) => write!(f, "peer disconnected: {reason}"),
            Self::Closed => f.write_str("peer closed the connection"),
            Self::Io(err) => write!(f, "connection failed: {err}"),
            Self::Protocol(err) => write!(f, "protocol violation: {err}"),
            Self::Stalled => f.write_str("peer stopped reading"),
        }
    }
}

/// Builds the handle and the driver of a session whose handshake just completed on `stream`.
pub(super) fn new(
    peer: PeerStatus,
    stream: Stream,
    serving: SessionServing,
    answers: mpsc::Receiver<Bytes>,
) -> (SessionHandle, SessionDriver) {
    let (range_tx, range_rx) = watch::channel(BlockRange {
        earliest: peer.earliest.unwrap_or_default(),
        latest: peer.latest.unwrap_or_default(),
    });
    let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
    let driver = SessionDriver {
        peer_id: peer.peer_id,
        stream,
        commands: commands_rx,
        range: range_tx,
        pending: HashMap::new(),
        next_request_id: 1,
        serving,
        answers,
        established: Instant::now(),
    };
    let handle = SessionHandle {
        status: Arc::new(peer),
        commands: commands_tx,
        range: range_rx,
        used: Arc::new(Mutex::new(Instant::now())),
    };
    (handle, driver)
}

/// A live session, cheap to clone. Requests go through it; the `SessionDriver` does the I/O.
#[derive(Debug, Clone)]
pub struct SessionHandle {
    status: Arc<PeerStatus>,
    commands: mpsc::Sender<Command>,
    range: watch::Receiver<BlockRange>,
    /// When we last sent the peer a request, or the session opened: a session we have no use
    /// for is released.
    used: Arc<Mutex<Instant>>,
}

impl SessionHandle {
    /// What the peer told us in the handshake.
    pub(crate) fn status(&self) -> &PeerStatus {
        &self.status
    }

    /// The peer's id: its public key.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.status.peer_id
    }

    /// The blocks the peer currently says it serves: its status, then its range updates.
    #[must_use]
    pub fn range(&self) -> BlockRange {
        *self.range.borrow()
    }

    /// Requests the receipts of `blocks`, in request order. Each item is one block's receipts
    /// as the peer sent them, not decoded (decoding them is CPU work for a blocking thread). A peer may answer for fewer blocks than asked: the answer is
    /// then a prefix; an empty answer means the peer does not hold them. Nothing is verified.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError::Timeout`] after the request timeout (20 s), [`RequestError::SessionClosed`]
    /// if the session ended, [`RequestError::Malformed`] if the answer cannot be cut into
    /// items, and [`RequestError::Excess`] if it has more items than were asked for.
    pub async fn receipts(&self, blocks: Vec<B256>) -> Result<Vec<Bytes>, RequestError> {
        let asked = blocks.len();
        let body = self.request(Request::Receipts(blocks)).await?;
        items(&body, asked)
    }

    /// Requests up to `limit` headers going down from the block with hash `start`, inclusive.
    /// Each is the RLP the peer sent, not decoded: the caller hashes those bytes. An empty
    /// answer means the peer does not hold the block. Nothing is verified here.
    ///
    /// # Errors
    ///
    /// As [`Self::receipts`].
    pub async fn headers(&self, start: B256, limit: u64) -> Result<Vec<Bytes>, RequestError> {
        let body = self.request(Request::Headers { start, limit }).await?;
        items(&body, usize::try_from(limit).unwrap_or(usize::MAX))
    }

    /// Requests the bodies of `blocks`, in request order. Each is the RLP the peer sent (its
    /// transactions, ommers and optional withdrawals), not decoded. A peer may answer for fewer
    /// blocks than asked: the answer is then a prefix. Nothing is verified here.
    ///
    /// # Errors
    ///
    /// As [`Self::receipts`].
    pub async fn bodies(&self, blocks: Vec<B256>) -> Result<Vec<Bytes>, RequestError> {
        let asked = blocks.len();
        let body = self.request(Request::Bodies(blocks)).await?;
        items(&body, asked)
    }

    /// Time since we last sent the peer a request, or since the session opened.
    pub(crate) fn idle(&self) -> Duration {
        self.used
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed()
    }

    /// Sends `request` and waits for the body of its answer (the message without its id byte).
    async fn request(&self, request: Request) -> Result<Bytes, RequestError> {
        *self.used.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = Command::Request {
            request,
            reply: reply_tx,
        };
        let answer = async {
            self.commands
                .send(command)
                .await
                .map_err(|_closed| RequestError::SessionClosed)?;
            reply_rx
                .await
                .map_err(|_closed| RequestError::SessionClosed)
        };
        timeout(REQUEST_TIMEOUT, answer)
            .await
            .map_err(|_elapsed| RequestError::Timeout)?
    }

    /// Ends the session, telling the peer why. Does nothing if it has already ended.
    pub(crate) fn disconnect(&self, reason: DisconnectReason) {
        // A full queue or a closed channel both mean the driver is not going to read this; a
        // dropped handle ends the session anyway.
        let _sent = self.commands.try_send(Command::Disconnect(reason));
    }
}

/// What a handle asks its driver to do.
#[derive(Debug)]
enum Command {
    Request {
        request: Request,
        /// Receives the response body (the message without its id byte).
        reply: oneshot::Sender<Bytes>,
    },
    Disconnect(DisconnectReason),
}

/// Owns a session's stream. The peer set runs it in its own task set.
#[derive(Debug)]
pub(crate) struct SessionDriver {
    peer_id: PeerId,
    stream: Stream,
    commands: mpsc::Receiver<Command>,
    range: watch::Sender<BlockRange>,
    /// Requests awaiting their response, by request id, with the id of the message that
    /// answers them.
    pending: HashMap<u64, (u8, oneshot::Sender<Bytes>)>,
    next_request_id: u64,
    /// The peer's requests go through it to the server.
    serving: SessionServing,
    /// The server's answers to the peer's requests.
    answers: mpsc::Receiver<Bytes>,
    established: Instant,
}

impl SessionDriver {
    /// Runs the session until it ends, `cancel` fires, or every handle is dropped.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> SessionEnd {
        let mut flush = interval(FLUSH_INTERVAL);
        flush.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let reason = loop {
            // In this order on purpose: what the peer sends is read last, so a peer that
            // floods the session cannot keep its own answers, our requests or the flush from
            // running. The branches before it are bounded: a timer, our own requests, and at
            // most a few answers in flight.
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    self.say_goodbye(DisconnectReason::ClientQuitting).await;
                    break EndReason::Cancelled;
                }
                // Pongs to the peer's pings wait in the stream's buffer until it is flushed.
                _ = flush.tick() => {
                    self.pending.retain(|_, (_, reply)| !reply.is_closed());
                    if let Err(reason) = self.flush().await {
                        break reason;
                    }
                }
                command = self.commands.recv() => match command {
                    Some(Command::Request { request, reply }) => {
                        if let Err(reason) = self.send_request(&request, reply).await {
                            break reason;
                        }
                    }
                    Some(Command::Disconnect(reason)) => {
                        self.say_goodbye(reason).await;
                        break EndReason::Cancelled;
                    }
                    // Every handle is gone: nobody can use this session any more.
                    None => {
                        self.say_goodbye(DisconnectReason::DisconnectRequested).await;
                        break EndReason::Cancelled;
                    }
                },
                // Never closed: `serving` holds the sending side.
                Some(answer) = self.answers.recv() => {
                    if let Err(reason) = self.write(answer).await {
                        break reason;
                    }
                }
                message = self.stream.next() => match message {
                    Some(Ok(message)) => {
                        if let Err(reason) = self.on_message(&message).await {
                            break reason;
                        }
                    }
                    Some(Err(err)) => break end_reason(err),
                    None => break EndReason::Closed,
                },
            }
        };
        let lasted = self.established.elapsed();
        debug!(peer = %self.peer_id, %reason, ?lasted, "execution session ended");
        SessionEnd {
            peer_id: self.peer_id,
            reason,
            lasted,
        }
    }

    /// Refuses the session, telling the peer why, instead of running it.
    pub(crate) async fn reject(mut self, reason: DisconnectReason) {
        self.say_goodbye(reason).await;
    }

    async fn say_goodbye(&mut self, reason: DisconnectReason) {
        // Best effort: the session is ending either way.
        let _sent = timeout(DISCONNECT_TIMEOUT, self.stream.disconnect(reason)).await;
    }

    /// Writes one message, giving the peer [`WRITE_TIMEOUT`] to take it.
    async fn write(&mut self, message: Bytes) -> Result<(), EndReason> {
        match timeout(WRITE_TIMEOUT, self.stream.send(message.0)).await {
            Ok(sent) => sent.map_err(end_reason),
            Err(_elapsed) => Err(EndReason::Stalled),
        }
    }

    /// Announces our block range if it changed, and flushes the stream.
    async fn flush(&mut self) -> Result<(), EndReason> {
        let update = self.serving.range_update();
        let flushed = timeout(WRITE_TIMEOUT, async {
            if let Some(update) = update {
                self.stream.feed(update.0).await?;
            }
            self.stream.flush().await
        });
        match flushed.await {
            Ok(flushed) => flushed.map_err(end_reason),
            Err(_elapsed) => Err(EndReason::Stalled),
        }
    }

    async fn send_request(
        &mut self,
        request: &Request,
        reply: oneshot::Sender<Bytes>,
    ) -> Result<(), EndReason> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending
            .insert(request_id, (request.response_id(), reply));
        self.write(request.encode(request_id)).await
    }

    /// Handles one message from the peer: a response to route, a request to answer or pass
    /// to the server, a range update, or something this node has no use for.
    async fn on_message(&mut self, message: &[u8]) -> Result<(), EndReason> {
        if message.len() > MAX_MESSAGE_BYTES {
            self.say_goodbye(DisconnectReason::ProtocolBreach).await;
            return Err(EndReason::Protocol(format!(
                "message of {} bytes",
                message.len()
            )));
        }
        let Some((&message_id, body)) = message.split_first() else {
            return Ok(());
        };
        if matches!(
            message_id,
            wire::RECEIPTS | wire::BLOCK_HEADERS | wire::BLOCK_BODIES
        ) {
            // An answer of another kind than the request with its id asked for is dropped,
            // and the request times out.
            if let Some(id) = wire::request_id(body)
                && self
                    .pending
                    .get(&id)
                    .is_some_and(|(expected, _)| *expected == message_id)
                && let Some((_, reply)) = self.pending.remove(&id)
            {
                // The requester may have timed out and gone; that is not the peer's fault.
                let _delivered = reply.send(Bytes::copy_from_slice(body));
            }
        } else if message_id == wire::BLOCK_RANGE_UPDATE {
            match wire::decode_block_range(body) {
                Ok(update) => {
                    self.range.send_replace(BlockRange {
                        earliest: update.earliest,
                        latest: update.latest,
                    });
                }
                Err(err) => return Err(EndReason::Protocol(format!("block range update: {err}"))),
            }
        } else {
            match self.serving.request(message_id, body) {
                Handled::Now(response) => self.write(response).await?,
                Handled::Later => {}
                // Transaction and block announcements: this node does not follow them.
                Handled::NotARequest => trace!(message_id, "ignored execution peer message"),
            }
        }
        Ok(())
    }
}

/// Cuts an answer into its items, of which there may be at most `asked`.
fn items(body: &Bytes, asked: usize) -> Result<Vec<Bytes>, RequestError> {
    let items = wire::decode_items(body).map_err(|err| RequestError::Malformed(err.to_string()))?;
    if items.len() > asked {
        return Err(RequestError::Excess {
            asked,
            got: items.len(),
        });
    }
    Ok(items)
}

fn end_reason(err: P2PStreamError) -> EndReason {
    if let P2PStreamError::Disconnected(reason) = err {
        EndReason::PeerDisconnected(reason)
    } else if let P2PStreamError::Io(err) = err {
        EndReason::Io(err.to_string())
    } else {
        EndReason::Protocol(err.to_string())
    }
}
