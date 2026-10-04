//! A live session: the driver that owns the stream and the handle requests go through.
//!
//! The driver answers pings, passes the peer's requests to the server (see `serve`) and writes
//! its answers, announces the block range this node serves, follows the one the peer announces
//! and routes responses by request id. Does not open sessions
//! (see `handshake`) and verifies nothing a peer returns.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{B256, Bytes};
use futures_util::{SinkExt, StreamExt};
use op_alloy_consensus::OpReceiptEnvelope;
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
pub(crate) struct BlockRange {
    pub(crate) earliest: u64,
    pub(crate) latest: u64,
}

/// Why a request got no usable answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestError {
    #[error("request timed out")]
    Timeout,
    #[error("session closed")]
    SessionClosed,
    /// The answer could not be decoded: a peer fault.
    #[error("malformed response: {0}")]
    Malformed(String),
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
}

impl fmt::Display for EndReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("ended by us"),
            Self::PeerDisconnected(reason) => write!(f, "peer disconnected: {reason}"),
            Self::Closed => f.write_str("peer closed the connection"),
            Self::Io(err) => write!(f, "connection failed: {err}"),
            Self::Protocol(err) => write!(f, "protocol violation: {err}"),
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
    };
    (handle, driver)
}

/// A live session, cheap to clone. Requests go through it; the [`SessionDriver`] does the I/O.
#[derive(Debug, Clone)]
pub(crate) struct SessionHandle {
    status: Arc<PeerStatus>,
    commands: mpsc::Sender<Command>,
    range: watch::Receiver<BlockRange>,
}

impl SessionHandle {
    /// What the peer told us in the handshake.
    pub(crate) fn status(&self) -> &PeerStatus {
        &self.status
    }

    /// The blocks the peer currently says it serves: its status, then its range updates.
    pub(crate) fn range(&self) -> BlockRange {
        *self.range.borrow()
    }

    /// Requests the receipts of one block, in consensus form (bloom included). An empty answer
    /// means the peer does not hold them. Nothing is verified here.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError::Timeout`] after [`REQUEST_TIMEOUT`], [`RequestError::SessionClosed`]
    /// if the session ended, and [`RequestError::Malformed`] if the answer cannot be decoded.
    pub(crate) async fn receipts(
        &self,
        block: B256,
    ) -> Result<Vec<OpReceiptEnvelope>, RequestError> {
        let body = self.request(Request::Receipts(vec![block])).await?;
        wire::decode_receipts(&body).map_err(malformed)
    }

    /// Requests the receipts of several blocks, as [`Self::receipts`] returns them, in request
    /// order. A peer may answer for fewer blocks than asked: the answer is then a prefix.
    ///
    /// # Errors
    ///
    /// As [`Self::receipts`].
    pub(crate) async fn receipts_of(
        &self,
        blocks: Vec<B256>,
    ) -> Result<Vec<Vec<OpReceiptEnvelope>>, RequestError> {
        let body = self.request(Request::Receipts(blocks)).await?;
        wire::decode_block_receipts(&body).map_err(malformed)
    }

    /// Requests up to `limit` headers going down from the block with hash `start`, inclusive.
    /// Each is the RLP the peer sent, not decoded: the caller hashes those bytes. An empty
    /// answer means the peer does not hold the block. Nothing is verified here.
    ///
    /// # Errors
    ///
    /// As [`Self::receipts`].
    pub(crate) async fn headers(
        &self,
        start: B256,
        limit: u64,
    ) -> Result<Vec<Bytes>, RequestError> {
        let body = self.request(Request::Headers { start, limit }).await?;
        wire::decode_items(&body).map_err(malformed)
    }

    /// Requests the bodies of `blocks`, in request order. Each is the RLP the peer sent (its
    /// transactions, ommers and optional withdrawals), not decoded. A peer may answer for fewer
    /// blocks than asked: the answer is then a prefix. Nothing is verified here.
    ///
    /// # Errors
    ///
    /// As [`Self::receipts`].
    pub(crate) async fn bodies(&self, blocks: Vec<B256>) -> Result<Vec<Bytes>, RequestError> {
        let body = self.request(Request::Bodies(blocks)).await?;
        wire::decode_items(&body).map_err(malformed)
    }

    /// Sends `request` and waits for the body of its answer (the message without its id byte).
    async fn request(&self, request: Request) -> Result<Bytes, RequestError> {
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
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    self.say_goodbye(DisconnectReason::ClientQuitting).await;
                    break EndReason::Cancelled;
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
                message = self.stream.next() => match message {
                    Some(Ok(message)) => {
                        if let Err(reason) = self.on_message(&message).await {
                            break reason;
                        }
                    }
                    Some(Err(err)) => break end_reason(err),
                    None => break EndReason::Closed,
                },
                // Never closed: `serving` holds the sending side.
                Some(answer) = self.answers.recv() => {
                    if let Err(err) = self.stream.send(answer.0).await {
                        break end_reason(err);
                    }
                }
                // Pongs to the peer's pings wait in the stream's buffer until it is flushed.
                _ = flush.tick() => {
                    self.pending.retain(|_, (_, reply)| !reply.is_closed());
                    if let Some(update) = self.serving.range_update()
                        && let Err(err) = self.stream.feed(update.0).await
                    {
                        break end_reason(err);
                    }
                    if let Err(err) = self.stream.flush().await {
                        break end_reason(err);
                    }
                }
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

    async fn send_request(
        &mut self,
        request: &Request,
        reply: oneshot::Sender<Bytes>,
    ) -> Result<(), EndReason> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending
            .insert(request_id, (request.response_id(), reply));
        self.stream
            .send(request.encode(request_id).0)
            .await
            .map_err(end_reason)
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
                Handled::Now(response) => {
                    self.stream.send(response.0).await.map_err(end_reason)?;
                }
                Handled::Later => {}
                // Transaction and block announcements: this node does not follow them.
                Handled::NotARequest => trace!(message_id, "ignored execution peer message"),
            }
        }
        Ok(())
    }
}

fn malformed(err: alloy_rlp::Error) -> RequestError {
    RequestError::Malformed(err.to_string())
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
