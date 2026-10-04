//! The beacon network: one task that owns the libp2p swarm, keeps a few beacon peers that
//! serve light-client data, sends them the light client's requests and passes on what they
//! gossip.
//!
//! ```text
//! discovery (eth2 fork digest) ─▶ dial ─▶ identify (lists the light-client protocols?)
//!   ─▶ Status sent, peer usable
//! NetworkHandle::request ─▶ the peer that failed least and was asked longest ago,
//!   or the next one that connects
//! gossip (finality and optimistic updates) ─▶ Gossip
//! ```
//!
//! - Peers drop a node that answers none of their requests, so `Status`, `Ping` and
//!   `GetMetaData` are answered, and `Goodbye` is read.
//! - A peer that lists the protocols but answers without data a few times in a row is dropped
//!   and not dialed again; so is one whose answer is malformed, and one the light client
//!   reports with [`NetworkHandle::report_invalid`].
//! - Everything is bounded: connections, the peer table, candidates, the size and the number
//!   of chunks of a response, and the time a request may take.
//! - A gossip message is handed to the light client and forwarded to the mesh only once the
//!   light client reports it verified ([`NetworkHandle::report_gossip`]); one it has no room
//!   for is not forwarded.
//!
//! Decodes no light-client container and verifies nothing: see `client` and `verify`. The
//! fork digest is fixed at start: after a fork of the beacon chain the node has to restart.

mod behaviour;
mod handle;
mod peers;

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::Bytes;
use futures_util::StreamExt;
use libp2p::gossipsub::{self, MessageAcceptance, MessageId, TopicHash};
use libp2p::multiaddr::Protocol as AddrProtocol;
use libp2p::request_response::{self, OutboundFailure, OutboundRequestId};
use libp2p::swarm::SwarmEvent;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{Multiaddr, PeerId, Swarm, SwarmBuilder, identify, identity, noise, tcp, yamux};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

pub(super) use self::handle::{
    Gossip, NetworkHandle, Request, RequestError, Response, Topic, Verdict,
};

use self::behaviour::{Asked, Behaviour, BehaviourEvent, RpcEvent};
use self::handle::{COMMAND_CAPACITY, Command, Reply};
use self::peers::Peers;
use super::BeaconError;
use super::discovery::Discovery;
use super::rpc::{self, Chunk, Codec, StatusData};
use super::spec::ForkDigest;

/// Limit for one request to find a peer, and again for the peer to answer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a connection may sit unused. Requests take turns among the peers, so only a
/// peer that serves nothing sits idle.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// How often candidates are dialed.
const TICK: Duration = Duration::from_secs(1);
/// Candidates in transit from discovery, which drops what does not fit.
const CANDIDATE_CAPACITY: usize = 256;
/// Gossip messages waiting for the light client: two arrive per slot, so this is a minute of
/// them. What does not fit is dropped; the next slot brings newer ones.
const GOSSIP_CAPACITY: usize = 16;
/// A request and who gets its answer: waiting for a peer, or for the peer's answer.
#[derive(Debug)]
struct Waiting {
    request: Request,
    reply: Reply,
    since: Instant,
}

/// The swarm task.
pub(super) struct Network {
    listen_addr: SocketAddr,
    bootnodes: Vec<String>,
    digest: ForkDigest,
    swarm: Swarm<Behaviour>,
    status: watch::Receiver<StatusData>,
    commands: mpsc::Receiver<Command>,
    gossip: mpsc::Sender<Gossip>,
    finality_topic: TopicHash,
    optimistic_topic: TopicHash,
    peers: Peers,
    /// Bounded by the commands taken and [`REQUEST_TIMEOUT`]: every request sent ends in an
    /// answer or a failure event.
    pending: HashMap<(Asked, OutboundRequestId), Waiting>,
    /// Requests waiting for a peer to ask, oldest first; each at most [`REQUEST_TIMEOUT`].
    waiting: VecDeque<Waiting>,
}

impl std::fmt::Debug for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Network")
            .field("listen_addr", &self.listen_addr)
            .field("digest", &self.digest)
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

/// Creates the network, its handle and the stream of gossip messages. Binds the TCP
/// listener; [`Network::run`] binds discovery's UDP socket.
///
/// The node gets a new identity at each start. `digest` is the fork digest peers must be on;
/// `status` is what the node reports about itself in `Status`, and follows the light client's
/// store.
///
/// # Errors
///
/// Returns [`BeaconError::Transport`] or [`BeaconError::Gossip`] if the transport or
/// gossipsub cannot be set up, and [`BeaconError::Listen`] if the listen address cannot be
/// bound.
pub(super) fn new(
    listen_addr: SocketAddr,
    bootnodes: Vec<String>,
    digest: ForkDigest,
    status: watch::Receiver<StatusData>,
) -> Result<(Network, NetworkHandle, mpsc::Receiver<Gossip>), BeaconError> {
    let (gossipsub, finality_topic, optimistic_topic) = behaviour::gossip(digest)?;
    let mut swarm = SwarmBuilder::with_existing_identity(identity::Keypair::generate_secp256k1())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(BeaconError::Transport)?
        .with_behaviour(|key| Behaviour::new(key, gossipsub))
        .unwrap_or_else(|never| match never {})
        .with_swarm_config(|swarm| swarm.with_idle_connection_timeout(IDLE_TIMEOUT))
        .build();
    let listen = Multiaddr::from(listen_addr.ip()).with(AddrProtocol::Tcp(listen_addr.port()));
    swarm
        .listen_on(listen.clone())
        .map_err(|err| BeaconError::Listen(listen, err))?;
    let (commands_tx, commands) = mpsc::channel(COMMAND_CAPACITY);
    let (gossip, gossip_rx) = mpsc::channel(GOSSIP_CAPACITY);
    let network = Network {
        listen_addr,
        bootnodes,
        digest,
        swarm,
        status,
        commands,
        gossip,
        finality_topic,
        optimistic_topic,
        peers: Peers::default(),
        pending: HashMap::new(),
        waiting: VecDeque::new(),
    };
    let handle = NetworkHandle {
        commands: commands_tx,
    };
    Ok((network, handle, gossip_rx))
}

impl Network {
    /// Runs discovery and the swarm until `cancel` fires or every [`NetworkHandle`] is
    /// dropped. Requests waiting for an answer then end with [`RequestError::NoPeer`].
    ///
    /// # Errors
    ///
    /// Returns [`BeaconError::Discovery`] if the discovery socket cannot be bound.
    pub(super) async fn run(mut self, cancel: CancellationToken) -> Result<(), BeaconError> {
        let discovery = Discovery::start(self.listen_addr, &self.bootnodes, self.digest).await?;
        // The swarm ending stops discovery.
        let stop = cancel.child_token();
        let (found_tx, mut found) = mpsc::channel(CANDIDATE_CAPACITY);
        let mut tasks = JoinSet::new();
        tasks.spawn(discovery.run(found_tx, stop.clone()));

        let mut tick = interval(TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                command = self.commands.recv() => match command {
                    Some(Command::Request { request, reply }) => {
                        self.send(Waiting { request, reply, since: Instant::now() });
                    }
                    Some(Command::Invalid(peer)) => {
                        let agent = self.peers.agent(&peer);
                        warn!(%peer, agent, "beacon peer sent data that does not verify");
                        self.drop_peer(peer, "its data did not verify");
                    }
                    Some(Command::Gossip { id, peer, verdict }) => {
                        self.judge_gossip(&id, &peer, verdict);
                    }
                    None => break,
                },
                Some(candidate) = found.recv() => self.peers.found(candidate),
                event = self.swarm.select_next_some() => self.on_event(event),
                _ = tick.tick() => {
                    self.dial();
                    self.expire_waiting();
                }
            }
        }
        stop.cancel();
        // Discovery only returns; a panic there has nothing to add to the result.
        while tasks.join_next().await.is_some() {}
        Ok(())
    }

    /// Dials candidates while peers are wanted, and gives up dials that take too long.
    fn dial(&mut self) {
        for peer in self.peers.expired_dials() {
            // Not connected any more: nothing to close.
            let _closed = self.swarm.disconnect_peer_id(peer);
        }
        while let Some(candidate) = self.peers.next_dial() {
            let dial = DialOpts::peer_id(candidate.peer)
                .addresses(vec![candidate.addr.clone()])
                .build();
            if self.swarm.dial(dial).is_ok() {
                self.peers.dialed(candidate);
            }
        }
    }

    /// Sends a request to the peer that failed least and, among those, was asked longest
    /// ago. Without a peer the request waits for the next one that connects.
    fn send(&mut self, waiting: Waiting) {
        let bootstrap = matches!(waiting.request, Request::Bootstrap(_));
        let Some(peer) = self.peers.pick(bootstrap) else {
            // Every place may be taken by peers that lack the bootstrap: one of them is
            // closed so another node is dialed. It stays known.
            if let Some(peer) = self.peers.in_the_way() {
                self.peers.lost(peer);
                let _closed = self.swarm.disconnect_peer_id(peer);
            }
            if self.waiting.len() < COMMAND_CAPACITY {
                self.waiting.push_back(waiting);
            } else {
                // The light client stopped waiting: nothing to do.
                let _sent = waiting.reply.send(Err(RequestError::NoPeer));
            }
            return;
        };
        let (asked, payload) = match waiting.request {
            Request::Bootstrap(root) => (Asked::Bootstrap, root.to_vec()),
            Request::UpdatesByRange {
                start_period,
                count,
            } => {
                let count = count.min(rpc::MAX_UPDATES);
                (Asked::Updates, rpc::updates_by_range(start_period, count))
            }
            Request::FinalityUpdate => (Asked::Finality, Vec::new()),
            Request::OptimisticUpdate => (Asked::Optimistic, Vec::new()),
        };
        debug!(%peer, ?asked, "light-client request sent");
        let behaviour = self.swarm.behaviour_mut().requests(asked);
        let id = behaviour.send_request(&peer, payload);
        self.pending.insert((asked, id), waiting);
    }

    /// Sends the waiting requests: a peer connected.
    fn retry_waiting(&mut self) {
        for waiting in std::mem::take(&mut self.waiting) {
            self.send(waiting);
        }
    }

    /// Ends the waiting requests no peer turned up for in time.
    fn expire_waiting(&mut self) {
        while let Some(waiting) = self
            .waiting
            .pop_front_if(|waiting| waiting.since.elapsed() >= REQUEST_TIMEOUT)
        {
            // The light client stopped waiting: nothing to do.
            let _sent = waiting.reply.send(Err(RequestError::NoPeer));
        }
    }

    fn on_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        trace!(?event, "beacon swarm event");
        if let SwarmEvent::Behaviour(event) = event {
            self.on_behaviour(event);
        } else if let SwarmEvent::OutgoingConnectionError {
            peer_id: Some(peer),
            error,
            ..
        } = event
        {
            debug!(%peer, %error, "beacon peer dial failed");
            self.peers.dial_ended(&peer);
        } else if let SwarmEvent::ConnectionClosed { peer_id, cause, .. } = event {
            // Not dropped by us: worth another dial later.
            if self.peers.lost(peer_id) {
                let cause = cause.map(|cause| cause.to_string());
                debug!(peer = %peer_id, cause, "beacon peer disconnected");
            }
            self.peers.dial_ended(&peer_id);
        }
    }

    fn on_behaviour(&mut self, event: BehaviourEvent) {
        match event {
            BehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
                self.on_identified(peer_id, &info);
            }
            BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            }) => self.on_gossip(propagation_source, message_id, message),
            BehaviourEvent::Identify(_) | BehaviourEvent::Gossipsub(_) => {}
            BehaviourEvent::Limits(never) => match never {},
            BehaviourEvent::Status(event) => self.on_status(event),
            BehaviourEvent::Ping(event) => {
                self.answer(event, |behaviour| &mut behaviour.ping, rpc::ping());
            }
            BehaviourEvent::MetadataV2(event) => {
                let metadata = rpc::metadata(false);
                self.answer(event, |behaviour| &mut behaviour.metadata_v2, metadata);
            }
            BehaviourEvent::MetadataV3(event) => {
                let metadata = rpc::metadata(true);
                self.answer(event, |behaviour| &mut behaviour.metadata_v3, metadata);
            }
            BehaviourEvent::Goodbye(event) => {
                if let request_response::Event::Message { peer, message, .. } = event
                    && let request_response::Message::Request { request, .. } = message
                {
                    // Dropping the channel closes the stream: a goodbye has no answer.
                    let reason = request
                        .first_chunk::<8>()
                        .map(|code| u64::from_le_bytes(*code));
                    debug!(%peer, reason, "beacon peer said goodbye");
                }
            }
            BehaviourEvent::Bootstrap(event) => self.on_answer(Asked::Bootstrap, event),
            BehaviourEvent::Updates(event) => self.on_answer(Asked::Updates, event),
            BehaviourEvent::Finality(event) => self.on_answer(Asked::Finality, event),
            BehaviourEvent::Optimistic(event) => self.on_answer(Asked::Optimistic, event),
        }
    }

    fn on_identified(&mut self, peer: PeerId, info: &identify::Info) {
        let addr = self.peers.dial_ended(&peer);
        if self.peers.is_connected(&peer) {
            return;
        }
        let serves = info
            .protocols
            .iter()
            .any(|name| name.as_ref() == rpc::BOOTSTRAP);
        if !serves || self.peers.is_avoided(&peer) {
            self.drop_peer(peer, "it does not serve light-client data");
            return;
        }
        if self.peers.is_full() {
            // No room; it is not held against the peer.
            let _closed = self.swarm.disconnect_peer_id(peer);
            return;
        }
        debug!(%peer, agent = info.agent_version, "beacon peer connected");
        self.peers.connected(peer, info.agent_version.clone(), addr);
        let status = self.status_ssz();
        self.swarm
            .behaviour_mut()
            .status
            .send_request(&peer, status);
        // Usable at once: peers may hang up on a new connection within a second, so a
        // request has to leave together with the status, not after its answer.
        self.retry_waiting();
    }

    fn on_status(&mut self, event: RpcEvent) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request { channel, .. } => {
                    let status = self.status_ssz();
                    // The peer hung up before the answer: nothing to do.
                    let _sent = self
                        .swarm
                        .behaviour_mut()
                        .status
                        .send_response(channel, vec![Chunk::plain(status)]);
                }
                request_response::Message::Response { response, .. } => {
                    let theirs = response
                        .first()
                        .and_then(|chunk| rpc::status_digest(&chunk.ssz));
                    if theirs != Some(self.digest) {
                        self.drop_peer(peer, "it is on another fork digest");
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, error, .. } => {
                debug!(%peer, %error, "beacon peer did not answer the status");
                // A connection that closed says nothing about the peer: full peers hang up
                // before answering, and are dialed again later.
                if matches!(
                    error,
                    OutboundFailure::Timeout | OutboundFailure::UnsupportedProtocols
                ) {
                    self.drop_peer(peer, "it did not answer the status");
                }
            }
            request_response::Event::InboundFailure { .. }
            | request_response::Event::ResponseSent { .. } => {}
        }
    }

    /// The SSZ of the `Status` this node reports now.
    fn status_ssz(&self) -> Vec<u8> {
        rpc::status(self.digest, *self.status.borrow())
    }

    /// Answers a peer's request on one of the protocols that are only served.
    fn answer(
        &mut self,
        event: RpcEvent,
        protocol: impl FnOnce(&mut Behaviour) -> &mut request_response::Behaviour<Codec>,
        ssz: Vec<u8>,
    ) {
        if let request_response::Event::Message { message, .. } = event
            && let request_response::Message::Request { channel, .. } = message
        {
            let behaviour = protocol(self.swarm.behaviour_mut());
            // The peer hung up before the answer: nothing to do.
            let _sent = behaviour.send_response(channel, vec![Chunk::plain(ssz)]);
        }
    }

    /// Passes the answer to a light-client request, or its failure, to who asked, and
    /// remembers how the peer did.
    fn on_answer(&mut self, asked: Asked, event: RpcEvent) {
        let (id, peer, outcome) = if let request_response::Event::Message {
            peer,
            message:
                request_response::Message::Response {
                    request_id,
                    response,
                },
            ..
        } = event
        {
            (request_id, peer, Ok(response))
        } else if let request_response::Event::OutboundFailure {
            peer,
            request_id,
            error,
            ..
        } = event
        {
            (request_id, peer, Err(error))
        } else {
            return;
        };
        let Some(pending) = self.pending.remove(&(asked, id)) else {
            return;
        };
        let result = match outcome {
            Ok(chunks) => successful(peer, chunks),
            // The codec's verdict on bytes that are not the wire format.
            Err(OutboundFailure::Io(err)) if err.kind() == io::ErrorKind::InvalidData => {
                debug!(%peer, ?asked, %err, "malformed light-client answer");
                Err(RequestError::Malformed(peer))
            }
            Err(OutboundFailure::Timeout) => Err(RequestError::Unanswered(peer)),
            Err(error) => {
                // The connection went away, which says nothing about what the peer holds:
                // the request goes to another peer, as if it had not been sent.
                debug!(%peer, ?asked, %error, "light-client request lost with its connection");
                self.peers.lost(peer);
                if pending.since.elapsed() < REQUEST_TIMEOUT {
                    self.send(pending);
                    return;
                }
                Err(RequestError::Unanswered(peer))
            }
        };
        match &result {
            Ok(response) if !response.chunks.is_empty() => self.peers.answered(&peer),
            Err(RequestError::Malformed(_)) => self.drop_peer(peer, "its answer was malformed"),
            // An answer without data: it does not hold what was asked, or serves nothing
            // though it lists the protocol.
            Ok(_) | Err(RequestError::Refused(..)) => {
                self.failed(peer, asked == Asked::Bootstrap);
            }
            Err(_) => self.failed(peer, false),
        }
        // The light client stopped waiting: nothing to do.
        let _sent = pending.reply.send(result);
    }

    /// Hands a gossip message to the light client, whose verdict decides whether it is
    /// forwarded. Without room there, it is not.
    fn on_gossip(&mut self, peer: PeerId, id: MessageId, message: gossipsub::Message) {
        let topic = if message.topic == self.finality_topic {
            Topic::Finality
        } else if message.topic == self.optimistic_topic {
            Topic::Optimistic
        } else {
            self.judge_gossip(&id, &peer, MessageAcceptance::Ignore);
            return;
        };
        let data = Bytes::from(message.data);
        let gossip = Gossip {
            id,
            peer,
            topic,
            data,
        };
        if let Err(full) = self.gossip.try_send(gossip) {
            debug!(%peer, ?topic, "light-client gossip dropped: the light client is busy");
            let id = full.into_inner().id;
            self.judge_gossip(&id, &peer, MessageAcceptance::Ignore);
        }
    }

    /// Tells gossipsub what a message it holds back was found to be.
    fn judge_gossip(&mut self, id: &MessageId, peer: &PeerId, acceptance: MessageAcceptance) {
        let gossipsub = &mut self.swarm.behaviour_mut().gossipsub;
        // Not in gossipsub's cache any more: the verdict came too late to forward it.
        let _known = gossipsub.report_message_validation_result(id, peer, acceptance);
    }

    /// Counts a request the peer did not answer with data; drops the peer after a few in a
    /// row.
    fn failed(&mut self, peer: PeerId, lacks_bootstrap: bool) {
        if self.peers.failed(&peer, lacks_bootstrap) {
            self.drop_peer(peer, "it serves no light-client data");
        }
    }

    /// Disconnects a peer and does not dial it again.
    fn drop_peer(&mut self, peer: PeerId, reason: &'static str) {
        debug!(%peer, reason, "beacon peer dropped");
        self.peers.avoid(peer);
        // Not connected any more: nothing to close.
        let _closed = self.swarm.disconnect_peer_id(peer);
    }
}

/// The successful chunks of an answer, or the error code it starts with.
fn successful(peer: PeerId, chunks: Vec<Chunk>) -> Result<Response, RequestError> {
    if let Some(refusal) = chunks.first().filter(|chunk| chunk.code != rpc::SUCCESS) {
        return Err(RequestError::Refused(peer, refusal.code));
    }
    let chunks = chunks
        .into_iter()
        .take_while(|chunk| chunk.code == rpc::SUCCESS)
        .map(|chunk| (chunk.context, Bytes::from(chunk.ssz)))
        .collect();
    Ok(Response { peer, chunks })
}
