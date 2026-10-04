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
//! - Gossip messages are never forwarded: forwarding is for verified messages, and
//!   verification is not this module's. They are handed over and reported as ignored.
//!
//! Decodes no light-client container and verifies nothing: see `client` and `verify`. The
//! fork digest is fixed at start: after a fork of the beacon chain the node has to restart.

mod behaviour;
mod handle;

use std::collections::{HashMap, HashSet, VecDeque};
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

pub(super) use self::handle::{Gossip, NetworkHandle, Request, RequestError, Response, Topic};

use self::behaviour::{Asked, Behaviour, BehaviourEvent, RpcEvent};
use self::handle::{COMMAND_CAPACITY, Command, Reply};
use super::BeaconError;
use super::discovery::{Candidate, Discovery};
use super::rpc::{self, Chunk, Codec, StatusData};
use super::spec::ForkDigest;

/// Peers serving light-client data to stay connected to.
const TARGET_PEERS: usize = 6;
/// Peers kept at most, counting those that dialed us.
const MAX_PEERS: usize = 12;
/// Dials in progress at once.
const MAX_DIALS: usize = 4;
/// Limit for a dial, the handshake and the peer's identify answer.
const DIAL_TIMEOUT: Duration = Duration::from_secs(20);
/// Limit for one request, from sending it to the end of its answer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a connection may sit unused. Requests take turns among the peers, so only a
/// peer that serves nothing sits idle.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// How often candidates are dialed.
const TICK: Duration = Duration::from_secs(1);
/// Requests in a row a peer may leave unanswered or answer without data before it is dropped.
const MAX_FAILURES: u32 = 3;
/// Peers remembered as not worth dialing; the set is emptied when it is full.
const MAX_AVOIDED: usize = 4096;
/// Candidates waiting to be dialed; discovery finds more when these are used up.
const MAX_CANDIDATES: usize = 512;
/// Peers that served and hung up, waiting to be dialed again.
const MAX_REDIALS: usize = 64;
/// How long a peer that hung up is left alone. Peers with no room say so and close; their
/// room changes over minutes.
const REDIAL_AFTER: Duration = Duration::from_secs(120);
/// Candidates in transit from discovery, which drops what does not fit.
const CANDIDATE_CAPACITY: usize = 256;
/// Gossip messages waiting for the light client: two arrive per slot, so this is a minute of
/// them. What does not fit is dropped; the next slot brings newer ones.
const GOSSIP_CAPACITY: usize = 16;
/// A connected peer that lists the light-client protocols.
#[derive(Debug)]
struct Peer {
    agent: String,
    /// Where it was dialed; `None` if it dialed us.
    addr: Option<Multiaddr>,
    /// Requests in a row it left unanswered or answered without data.
    failures: u32,
    /// Whether it answered a bootstrap request without data.
    lacks_bootstrap: bool,
    /// When it was asked last, as a count of requests; 0 if never.
    asked_at: u64,
}

/// A request no peer could be asked yet.
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
    peers: HashMap<PeerId, Peer>,
    /// Dials in progress: when each began, and the address.
    dialing: HashMap<PeerId, (Instant, Multiaddr)>,
    candidates: VecDeque<Candidate>,
    /// Peers that were not dropped but hung up, oldest first, with when each may be dialed
    /// again: discovery reports a node once, so without them the supply of peers runs dry.
    redials: VecDeque<(Instant, Candidate)>,
    avoided: HashSet<PeerId>,
    /// Bounded by the commands taken and [`REQUEST_TIMEOUT`]: every request sent ends in an
    /// answer or a failure event.
    pending: HashMap<(Asked, OutboundRequestId), Reply>,
    /// Requests waiting for a peer to ask, oldest first; each at most [`REQUEST_TIMEOUT`].
    waiting: VecDeque<Waiting>,
    /// Requests sent so far.
    asked: u64,
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

/// Creates the network, its handle and the stream of gossip messages. Binds nothing yet:
/// [`Network::run`] does.
///
/// The node gets a new identity at each start. `digest` is the fork digest peers must be on;
/// `status` is what the node reports about itself in `Status`, and follows the light client's
/// store.
///
/// # Errors
///
/// Returns [`BeaconError::Transport`] or [`BeaconError::Gossip`] if the transport or
/// gossipsub cannot be set up, and [`BeaconError::Listen`] if the listen address is not one
/// the transport takes.
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
        peers: HashMap::new(),
        dialing: HashMap::new(),
        candidates: VecDeque::new(),
        redials: VecDeque::new(),
        avoided: HashSet::new(),
        pending: HashMap::new(),
        waiting: VecDeque::new(),
        asked: 0,
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
                        let agent = self.peers.get(&peer).map(|state| state.agent.clone());
                        warn!(%peer, agent, "beacon peer sent data that does not verify");
                        self.drop_peer(peer, "its data did not verify");
                    }
                    None => break,
                },
                Some(candidate) = found.recv() => {
                    if self.candidates.len() < MAX_CANDIDATES {
                        self.candidates.push_back(candidate);
                    }
                }
                event = self.swarm.select_next_some() => self.on_event(event),
                _ = tick.tick() => {
                    self.dial();
                    self.retry_waiting();
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
        let Self {
            dialing,
            peers,
            swarm,
            ..
        } = self;
        dialing.retain(|peer, (since, _)| {
            let waiting = since.elapsed() < DIAL_TIMEOUT;
            if !waiting && !peers.contains_key(peer) {
                // Connected without saying what it serves, or not connected: nothing to keep.
                let _closed = swarm.disconnect_peer_id(*peer);
            }
            waiting
        });
        while self.dialing.len() < MAX_DIALS
            && self.peers.len().saturating_add(self.dialing.len()) < TARGET_PEERS
            && let Some(candidate) = self.next_candidate()
        {
            let peer = candidate.peer;
            if self.avoided.contains(&peer)
                || self.peers.contains_key(&peer)
                || self.dialing.contains_key(&peer)
            {
                continue;
            }
            let dial = DialOpts::peer_id(peer)
                .addresses(vec![candidate.addr.clone()])
                .build();
            if self.swarm.dial(dial).is_ok() {
                self.dialing.insert(peer, (Instant::now(), candidate.addr));
            }
        }
    }

    /// The next node to dial: one discovery found, else a peer whose time to be dialed
    /// again has come.
    fn next_candidate(&mut self) -> Option<Candidate> {
        if let Some(candidate) = self.candidates.pop_front() {
            return Some(candidate);
        }
        let (due, _) = self.redials.front()?;
        if *due > Instant::now() {
            return None;
        }
        self.redials.pop_front().map(|(_, candidate)| candidate)
    }

    /// Sends a request to the peer that failed least and, among those, was asked longest
    /// ago. Without a peer the request waits for one, until it is [`REQUEST_TIMEOUT`] old.
    fn send(&mut self, waiting: Waiting) {
        let bootstrap = matches!(waiting.request, Request::Bootstrap(_));
        let picked = self
            .peers
            .iter_mut()
            .filter(|(_, state)| !(bootstrap && state.lacks_bootstrap))
            .min_by_key(|(_, state)| (state.failures, state.asked_at));
        let Some((peer, state)) = picked else {
            if waiting.since.elapsed() < REQUEST_TIMEOUT && self.waiting.len() < COMMAND_CAPACITY {
                self.waiting.push_back(waiting);
            } else {
                // The light client stopped waiting: nothing to do.
                let _sent = waiting.reply.send(Err(RequestError::NoPeer));
            }
            return;
        };
        let peer = *peer;
        self.asked = self.asked.saturating_add(1);
        state.asked_at = self.asked;
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
        self.pending.insert((asked, id), waiting.reply);
    }

    /// Tries the waiting requests again: a peer may have connected, or their time is up.
    fn retry_waiting(&mut self) {
        for waiting in std::mem::take(&mut self.waiting) {
            self.send(waiting);
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
            self.dialing.remove(&peer);
        } else if let SwarmEvent::ConnectionClosed { peer_id, cause, .. } = event {
            if let Some(state) = self.peers.remove(&peer_id) {
                let cause = cause.map(|cause| cause.to_string());
                debug!(peer = %peer_id, cause, "beacon peer disconnected");
                // Not dropped by us: worth another dial later.
                if let Some(addr) = state.addr
                    && self.redials.len() < MAX_REDIALS
                {
                    let candidate = Candidate {
                        peer: peer_id,
                        addr,
                    };
                    self.redials
                        .push_back((Instant::now() + REDIAL_AFTER, candidate));
                }
            }
            self.dialing.remove(&peer_id);
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
            }) => self.on_gossip(propagation_source, &message_id, message),
            BehaviourEvent::Identify(_) | BehaviourEvent::Gossipsub(_) => {}
            BehaviourEvent::Limits(never) => match never {},
            BehaviourEvent::Status(event) => self.on_status(event),
            BehaviourEvent::Ping(event) => {
                // The answer is our metadata sequence number, which never changes.
                self.answer(event, |behaviour| &mut behaviour.ping, vec![0; 8]);
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
        let addr = self.dialing.remove(&peer).map(|(_, addr)| addr);
        if self.peers.contains_key(&peer) {
            return;
        }
        let serves = info
            .protocols
            .iter()
            .any(|name| name.as_ref() == rpc::BOOTSTRAP);
        if !serves || self.avoided.contains(&peer) {
            self.drop_peer(peer, "it does not serve light-client data");
            return;
        }
        if self.peers.len() >= MAX_PEERS {
            // Not avoided: it may be wanted later.
            let _closed = self.swarm.disconnect_peer_id(peer);
            return;
        }
        debug!(%peer, agent = info.agent_version, "beacon peer connected");
        let state = Peer {
            agent: info.agent_version.clone(),
            addr,
            failures: 0,
            lacks_bootstrap: false,
            asked_at: 0,
        };
        self.peers.insert(peer, state);
        let status = rpc::status(self.digest, *self.status.borrow());
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
                    let status = rpc::status(self.digest, *self.status.borrow());
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
                self.drop_peer(peer, "it did not answer the status");
            }
            request_response::Event::InboundFailure { .. }
            | request_response::Event::ResponseSent { .. } => {}
        }
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
            Err(error) => {
                debug!(%peer, ?asked, %error, "light-client request failed");
                Err(RequestError::Unanswered(peer))
            }
        };
        match &result {
            Ok(response) if !response.chunks.is_empty() => {
                if let Some(state) = self.peers.get_mut(&peer) {
                    state.failures = 0;
                }
            }
            Err(RequestError::Malformed(_)) => self.drop_peer(peer, "its answer was malformed"),
            // An answer without data: it does not hold what was asked, or serves nothing
            // though it lists the protocol.
            Ok(_) | Err(RequestError::Refused(..)) => {
                self.failed(peer, asked == Asked::Bootstrap);
            }
            // A connection that closed says nothing about what the peer holds.
            Err(_) => self.failed(peer, false),
        }
        // The light client stopped waiting: nothing to do.
        let _sent = pending.send(result);
    }

    /// Hands a gossip message to the light client, if it has room, and tells gossipsub not
    /// to forward it.
    fn on_gossip(&mut self, peer: PeerId, id: &MessageId, message: gossipsub::Message) {
        let behaviour = self.swarm.behaviour_mut();
        let _known = behaviour.gossipsub.report_message_validation_result(
            id,
            &peer,
            MessageAcceptance::Ignore,
        );
        let topic = if message.topic == self.finality_topic {
            Topic::Finality
        } else if message.topic == self.optimistic_topic {
            Topic::Optimistic
        } else {
            return;
        };
        let data = Bytes::from(message.data);
        if self.gossip.try_send(Gossip { peer, topic, data }).is_err() {
            debug!(%peer, ?topic, "light-client gossip dropped: the light client is busy");
        }
    }

    /// Counts a request the peer did not answer with data; drops the peer after a few in a
    /// row.
    fn failed(&mut self, peer: PeerId, bootstrap: bool) {
        let Some(state) = self.peers.get_mut(&peer) else {
            return;
        };
        state.lacks_bootstrap |= bootstrap;
        state.failures = state.failures.saturating_add(1);
        if state.failures >= MAX_FAILURES {
            self.drop_peer(peer, "it serves no light-client data");
        }
    }

    /// Disconnects a peer and does not dial it again.
    fn drop_peer(&mut self, peer: PeerId, reason: &'static str) {
        debug!(%peer, reason, "beacon peer dropped");
        if self.avoided.len() >= MAX_AVOIDED {
            self.avoided.clear();
        }
        self.avoided.insert(peer);
        self.peers.remove(&peer);
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
