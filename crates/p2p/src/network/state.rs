//! Mutable state of the running node and its handlers for swarm events and validation results.
//!
//! Kept apart from the swarm so handlers can borrow both; [`super::Network::run`] owns one
//! [`State`] and one swarm and drives them from its event loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use alloy_primitives::BlockNumber;
use libp2p::gossipsub::{self, MessageAcceptance, MessageId, TopicHash};
use libp2p::multiaddr::Protocol;
use libp2p::request_response;
use libp2p::swarm::SwarmEvent;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::{Multiaddr, PeerId, Swarm};
use op_indexer_primitives::{PayloadVersion, UnsafeBlock};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{debug, info, trace, warn};

use super::{Behaviour, BehaviourEvent, answer};
use crate::block::{BlockError, BlockValidator, SeenBlocks};
use crate::peers::ConnectedPeers;
use crate::sync::Server;
use crate::{NodeStore, StoreError};

/// Minimum time between dials to the same peer, so unreachable peers aren't re-dialed every lookup.
const DIAL_BACKOFF: Duration = Duration::from_secs(120);
/// Most peers in dial backoff at once. Expired entries are pruned when it fills; if it is still
/// full, further dials wait for entries to expire.
const MAX_DIAL_BACKOFF_ENTRIES: usize = 1024;
/// How long an evicted peer is not dialed again; discovery keeps reporting it.
const EVICTED_PEER_BACKOFF: Duration = Duration::from_secs(600);
/// Blocks arrive every 1 or 2 s, depending on the chain, and duplicates are dropped by
/// gossipsub before validation, so a few in flight is normal; beyond this, messages are
/// ignored rather than queued without bound.
const MAX_PENDING_VALIDATIONS: usize = 32;
/// Minimum time between warnings that the local clock looks slow.
const CLOCK_SKEW_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// How long a peer whose score fell below the graylist threshold is banned.
const BAN_DURATION: Duration = Duration::from_secs(3600);
/// Score below which a peer is banned: op-node's default (`p2p.ban.threshold`). Below the
/// graylist threshold (-40) gossipsub already ignores the peer's messages; a ban takes more.
const BAN_THRESHOLD: f64 = -100.0;
/// Most peers banned at once; past it a peer is only disconnected.
const MAX_BANNED_PEERS: usize = 4096;
/// Distinct blocks the sequencer signed that this build cannot read, within
/// [`PROTOCOL_CHANGE_WINDOW`], before the node stops: one could be a fluke of a single
/// message; several close together mean the protocol changed under this build.
const PROTOCOL_CHANGE_BLOCKS: usize = 3;
/// How long such a block counts: rare flukes over months do not add up to a stop.
const PROTOCOL_CHANGE_WINDOW: Duration = Duration::from_secs(600);

/// Mutable state of the running node, separate from the swarm so handlers can borrow both.
pub(super) struct State {
    topics: HashMap<TopicHash, PayloadVersion>,
    validator: BlockValidator,
    blocks: mpsc::Sender<UnsafeBlock>,
    pub(super) validations: JoinSet<Validated>,
    /// Valid blocks seen per height, for the per-height limit and duplicates.
    seen: SeenBlocks,
    /// Connection times, to evict peers that never subscribe to our block topics.
    peers: ConnectedPeers,
    /// Earliest time each recently dialed peer may be dialed again. Pruned when it grows.
    next_dial: HashMap<PeerId, Instant>,
    /// Messages ignored because too many validations were already in flight.
    ignored_overload: u64,
    /// Accepted blocks dropped because the consumer channel was full.
    dropped_blocks: u64,
    /// Messages of blocks the sequencer signed that this build cannot read, with when they
    /// came, within [`PROTOCOL_CHANGE_WINDOW`] (by message id, which is the content's: the same
    /// block from several peers counts once).
    unreadable_blocks: VecDeque<(Instant, MessageId)>,
    /// The error that made them enough to stop, once they are.
    protocol_change: Option<String>,
    /// Set when the block consumer dropped its receiver; the node then shuts down.
    consumer_closed: bool,
    /// Known good peers from the node store still to dial, most recently seen first; drained as
    /// pending-connection slots free up.
    known_peers: VecDeque<Multiaddr>,
    /// When the slow-clock warning was last logged.
    clock_skew_warned: Option<Instant>,
    store: Arc<NodeStore>,
    /// Dial addresses of connected outbound peers not yet saved as known good (inbound peers
    /// have ephemeral ports, so they cannot be re-dialed).
    outbound: HashMap<PeerId, Multiaddr>,
    pub(super) persists: JoinSet<Result<(), StoreError>>,
    /// Connected peers subscribed to our block topics, read by discovery to pace itself.
    peer_count: watch::Sender<usize>,
    /// Highest accepted block number in this run; the first block sets it, so a restart is not
    /// seen as a gap.
    highest: Option<BlockNumber>,
    /// Highest L2 block committed to L1. Missing blocks at or below it are not a gap. Until an L1
    /// source feeds it, it stays 0 and every in-process gap counts as unsafe.
    safe_head: watch::Receiver<BlockNumber>,
    /// The `payload_by_number` server.
    pub(super) server: Server,
    /// Banned peers and when their ban ends.
    banned: HashMap<PeerId, Instant>,
}

/// Result of validating one message on a blocking thread.
pub(super) struct Validated {
    id: MessageId,
    source: PeerId,
    result: Result<UnsafeBlock, BlockError>,
}

impl State {
    pub(super) fn new(
        topics: HashMap<TopicHash, PayloadVersion>,
        validator: BlockValidator,
        blocks: mpsc::Sender<UnsafeBlock>,
        store: Arc<NodeStore>,
        peer_count: watch::Sender<usize>,
        safe_head: watch::Receiver<BlockNumber>,
        server: Server,
    ) -> Self {
        Self {
            topics,
            validator,
            blocks,
            validations: JoinSet::new(),
            seen: SeenBlocks::default(),
            peers: ConnectedPeers::default(),
            next_dial: HashMap::new(),
            ignored_overload: 0,
            dropped_blocks: 0,
            unreadable_blocks: VecDeque::new(),
            protocol_change: None,
            consumer_closed: false,
            known_peers: VecDeque::new(),
            clock_skew_warned: None,
            store,
            outbound: HashMap::new(),
            persists: JoinSet::new(),
            peer_count,
            highest: None,
            safe_head,
            server,
            banned: HashMap::new(),
        }
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "SwarmEvent and gossipsub::Event are non_exhaustive; the rest are only logged"
    )]
    pub(super) fn on_swarm_event(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        event: SwarmEvent<BehaviourEvent>,
    ) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            })) => {
                let Some(&version) = self.topics.get(&message.topic) else {
                    return;
                };
                if let Err(err) = BlockValidator::precheck(version, &message.data) {
                    self.report_failed(swarm, &message_id, propagation_source, &err);
                    return;
                }
                if self.validations.len() >= MAX_PENDING_VALIDATIONS {
                    self.ignored_overload += 1;
                    // Logged at 1, 2, 4, 8, ... so a flooding peer can't control our log volume.
                    if self.ignored_overload.is_power_of_two() {
                        warn!(
                            ignored_total = self.ignored_overload,
                            "validation backlog full, ignoring block message"
                        );
                    }
                    report(
                        swarm,
                        &message_id,
                        &propagation_source,
                        MessageAcceptance::Ignore,
                    );
                    return;
                }
                let validator = self.validator;
                let now = unix_now_secs();
                self.validations.spawn_blocking(move || Validated {
                    id: message_id,
                    source: propagation_source,
                    result: validator.validate(version, message.data, now),
                });
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                debug!(peer = %peer_id, addr = %endpoint.get_remote_address(), "peer connected");
                self.peers.connected(peer_id, Instant::now());
                if endpoint.is_dialer()
                    && let Ok(addr) = endpoint.get_remote_address().clone().with_p2p(peer_id)
                {
                    self.outbound.insert(peer_id, addr);
                }
                self.dial_queued_known_peers(swarm);
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                debug!(peer = %peer_id, ?cause, "peer disconnected");
                self.outbound.remove(&peer_id);
                self.peers.disconnected(&peer_id);
                self.update_peer_count(swarm);
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(
                gossipsub::Event::Subscribed { .. } | gossipsub::Event::Unsubscribed { .. },
            )) => self.update_peer_count(swarm),
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                debug!(peer = ?peer_id, err = %error, "dial failed");
                self.dial_queued_known_peers(swarm);
            }
            SwarmEvent::Behaviour(BehaviourEvent::Payloads(request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Request {
                        request_id,
                        request,
                        channel,
                    },
                ..
            })) => {
                if let Some(channel) = self.server.on_request(peer, request_id, request, channel) {
                    answer(swarm, channel, crate::sync::throttled());
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Payloads(
                request_response::Event::ResponseSent { request_id, .. }
                | request_response::Event::InboundFailure { request_id, .. },
            )) => self.server.written(request_id),
            SwarmEvent::NewListenAddr { address, .. } => info!(addr = %address, "listening"),
            other => trace!(event = ?other, "swarm event"),
        }
    }

    pub(super) fn on_validated(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        Validated { id, source, result }: Validated,
    ) {
        // A valid block is still rejected past the per-height limit; one already seen is `None`.
        let result = result.and_then(|block| {
            let is_new = self.seen.observe(block.number(), block.hash)?;
            Ok(is_new.then_some(block))
        });
        match result {
            Ok(None) => {
                report(swarm, &id, &source, MessageAcceptance::Ignore);
                debug!(peer = %source, "ignored duplicate block");
            }
            Ok(Some(block)) => {
                report(swarm, &id, &source, MessageAcceptance::Accept);
                debug!(number = block.number(), hash = %block.hash, version = ?block.version, "received unsafe block");
                self.check_gap(block.number());
                self.remember(source, block.timestamp_secs());
                match self.blocks.try_send(block) {
                    Ok(()) => {}
                    Err(TrySendError::Full(block)) => {
                        self.dropped_blocks += 1;
                        // Logged at 1, 2, 4, 8, ... drops, like the validation backlog warning.
                        if self.dropped_blocks.is_power_of_two() {
                            warn!(
                                number = block.number(),
                                hash = %block.hash,
                                dropped_total = self.dropped_blocks,
                                "block consumer is not keeping up, dropped block"
                            );
                        }
                    }
                    Err(TrySendError::Closed(_)) => self.consumer_closed = true,
                }
            }
            // The error says how to treat the message: a REJECT for every spec rule, an
            // IGNORE when the fault is ours.
            Err(err) => {
                if let Some(ahead_secs) = err.local_clock_lag_secs() {
                    self.warn_clock_skew(ahead_secs);
                }
                if err.is_protocol_change() {
                    self.unreadable(&id, &err);
                }
                self.report_failed(swarm, &id, source, &err);
            }
        }
    }

    /// Warns once when accepted block `number` skips unsafe blocks: heights above both the
    /// highest accepted block and the L2 safe head. Blocks at or below the highest (reorgs, late
    /// delivery) and heights L1 already has are not a gap. Nothing is fetched here: with the
    /// execution network on, the pipeline asks it for the missed blocks.
    fn check_gap(&mut self, number: BlockNumber) {
        let previous = self.highest;
        if previous.is_some_and(|highest| number <= highest) {
            return;
        }
        self.highest = Some(number);
        let Some(highest) = previous else {
            return;
        };
        let from = highest.max(*self.safe_head.borrow()).saturating_add(1);
        if from >= number {
            return;
        }
        let missed = number.saturating_sub(from);
        warn!(
            from,
            to = number.saturating_sub(1),
            missed,
            "missed unsafe blocks"
        );
    }

    /// Counts a block the sequencer signed that this build cannot read (`err` says how). At
    /// [`PROTOCOL_CHANGE_BLOCKS`] distinct ones within [`PROTOCOL_CHANGE_WINDOW`] the protocol
    /// has changed under this build, and [`Self::protocol_change`] says so.
    fn unreadable(&mut self, id: &MessageId, err: &BlockError) {
        let now = Instant::now();
        while self
            .unreadable_blocks
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > PROTOCOL_CHANGE_WINDOW)
        {
            self.unreadable_blocks.pop_front();
        }
        if self.protocol_change.is_some()
            || self.unreadable_blocks.iter().any(|(_, seen)| seen == id)
        {
            return;
        }
        self.unreadable_blocks.push_back((now, id.clone()));
        warn!(
            %err,
            seen = self.unreadable_blocks.len(),
            stop_at = PROTOCOL_CHANGE_BLOCKS,
            "the sequencer signed a block this build cannot read: the chain may have activated a \
             change this build does not know"
        );
        if self.unreadable_blocks.len() >= PROTOCOL_CHANGE_BLOCKS {
            self.protocol_change = Some(err.to_string());
        }
    }

    /// Warns, at most once per [`CLOCK_SKEW_WARN_INTERVAL`], that the local clock looks slow: the
    /// sequencer signed a block dated `ahead_secs` in our future. Such blocks are rejected, which
    /// penalizes the honest peers that relay them, so a slow clock ends with no peers.
    fn warn_clock_skew(&mut self, ahead_secs: u64) {
        let now = Instant::now();
        let due = self
            .clock_skew_warned
            .is_none_or(|at| now.duration_since(at) >= CLOCK_SKEW_WARN_INTERVAL);
        if due {
            self.clock_skew_warned = Some(now);
            warn!(
                ahead_secs,
                "sequencer block is dated in the future, check the local clock"
            );
        }
    }

    /// Whether the block consumer dropped its receiver, so the node should shut down.
    pub(super) const fn consumer_closed(&self) -> bool {
        self.consumer_closed
    }

    /// The last of [`PROTOCOL_CHANGE_BLOCKS`] blocks the sequencer signed that this build
    /// cannot read, once there are that many: the node must stop.
    pub(super) fn protocol_change(&mut self) -> Option<String> {
        self.protocol_change.take()
    }

    /// Loads the peers that delivered valid blocks before and starts dialing them, to reconnect
    /// without waiting for discovery.
    pub(super) async fn dial_known_peers(&mut self, swarm: &mut Swarm<Behaviour>) {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || store.known_peers()).await {
            Ok(Ok(peers)) => {
                info!(known_peers = peers.len(), "dialing known peers");
                self.known_peers = peers.into();
                self.dial_queued_known_peers(swarm);
            }
            Ok(Err(err)) => warn!(%err, "failed to load known peers"),
            Err(err) => warn!(%err, "known peers task failed"),
        }
    }

    /// Dials queued known peers while pending outgoing connections are below the limit, so peers
    /// past the first [`super::MAX_PENDING_CONNECTIONS`] are dialed as slots free up instead of
    /// being refused.
    pub(super) fn dial_queued_known_peers(&mut self, swarm: &mut Swarm<Behaviour>) {
        while swarm
            .network_info()
            .connection_counters()
            .num_pending_outgoing()
            < super::MAX_PENDING_CONNECTIONS
        {
            let Some(addr) = self.known_peers.pop_front() else {
                return;
            };
            self.dial(swarm, addr);
        }
    }

    /// Publishes the number of connected peers subscribed to our block topics.
    fn update_peer_count(&self, swarm: &Swarm<Behaviour>) {
        let subscribed = self.subscribed_peers(swarm).count();
        self.peer_count.send_replace(subscribed);
    }

    /// Connected peers subscribed to at least one of our block topics.
    fn subscribed_peers<'a>(
        &'a self,
        swarm: &'a Swarm<Behaviour>,
    ) -> impl Iterator<Item = &'a PeerId> {
        swarm
            .behaviour()
            .gossipsub
            .all_peers()
            .filter(|(_, topics)| topics.iter().any(|topic| self.topics.contains_key(topic)))
            .map(|(peer, _)| peer)
    }

    /// Reports that the message `id` from `source` failed validation, as the error says to
    /// treat it. After a rejection, bans `source` once its score falls below
    /// [`BAN_THRESHOLD`]; never for a block outside the time window, which may be our clock's
    /// fault.
    fn report_failed(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        id: &MessageId,
        source: PeerId,
        err: &BlockError,
    ) {
        let acceptance = err.acceptance();
        let rejected = matches!(acceptance, MessageAcceptance::Reject);
        debug!(peer = %source, %err, rejected, "block message failed validation");
        report(swarm, id, &source, acceptance);
        let score = swarm.behaviour().gossipsub.peer_score(&source);
        if rejected && !err.is_time_window() && score.is_some_and(|score| score < BAN_THRESHOLD) {
            self.ban(swarm, source);
        }
    }

    /// Bans `peer` for [`BAN_DURATION`]: its connections close, it may not connect again and
    /// is not dialed until then ([peer management]: "Peers may be banned if their performance
    /// score is too low"). Past [`MAX_BANNED_PEERS`] the peer is only disconnected.
    ///
    /// [peer management]: https://specs.optimism.io/protocol/rollup-node-p2p.html#peer-management
    fn ban(&mut self, swarm: &mut Swarm<Behaviour>, peer: PeerId) {
        debug!(%peer, "banning a peer below the ban threshold");
        let now = Instant::now();
        if self.banned.len() < MAX_BANNED_PEERS {
            self.banned.insert(peer, now + BAN_DURATION);
            swarm.behaviour_mut().bans.block_peer(peer);
        }
        self.back_off(peer, now + BAN_DURATION, now);
        // `Err` only means the peer was already disconnected, which is what we want.
        let _disconnected = swarm.disconnect_peer_id(peer);
    }

    /// Lifts the bans that have run out.
    fn lift_bans(&mut self, swarm: &mut Swarm<Behaviour>, now: Instant) {
        let bans = &mut swarm.behaviour_mut().bans;
        self.banned.retain(|peer, until| {
            let banned = *until > now;
            if !banned {
                bans.unblock_peer(*peer);
            }
            banned
        });
    }

    /// Disconnects peers that hold a slot without subscribing to our block topics (see
    /// [`ConnectedPeers::idle`]) and keeps them out of dialing for [`EVICTED_PEER_BACKOFF`].
    /// Lifts the bans that have run out, too.
    pub(super) fn evict_idle_peers(&mut self, swarm: &mut Swarm<Behaviour>) {
        let now = Instant::now();
        self.lift_bans(swarm, now);
        let subscribed: HashSet<PeerId> = self.subscribed_peers(swarm).copied().collect();
        for peer in self.peers.idle(now, |peer| subscribed.contains(peer)) {
            debug!(%peer, "disconnecting peer not subscribed to block topics");
            // `Err` only means the peer was already disconnected, which is what we want.
            let _disconnected = swarm.disconnect_peer_id(peer);
            self.back_off(peer, now + EVICTED_PEER_BACKOFF, now);
        }
    }

    /// Saves `peer` as known good when it first delivers a valid block on a connection we dialed.
    fn remember(&mut self, peer: PeerId, seen_secs: u64) {
        if let Some(addr) = self.outbound.remove(&peer) {
            let store = Arc::clone(&self.store);
            self.persists
                .spawn_blocking(move || store.save_peer(&addr, seen_secs));
        }
    }

    /// Dials `addr` unless its peer is connected, being dialed, in backoff, or the backoff map is
    /// full. A dial refused up front (e.g. by connection limits) sets no backoff, so the peer is
    /// retried when discovery reports it again.
    pub(super) fn dial(&mut self, swarm: &mut Swarm<Behaviour>, addr: Multiaddr) {
        let Some(Protocol::P2p(peer)) = addr.iter().last() else {
            return;
        };
        let now = Instant::now();
        if self.next_dial.get(&peer).is_some_and(|&next| next > now) || !self.has_backoff_room(now)
        {
            return;
        }
        let opts = DialOpts::peer_id(peer)
            .addresses(vec![addr])
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .build();
        match swarm.dial(opts) {
            Ok(()) => {
                self.back_off(peer, now + DIAL_BACKOFF, now);
            }
            Err(err) => {
                debug!(%peer, %err, "skipped dial");
            }
        }
    }

    /// Keeps `peer` out of dialing until `until`, unless the backoff map is full even after
    /// pruning expired entries.
    fn back_off(&mut self, peer: PeerId, until: Instant, now: Instant) {
        if self.next_dial.contains_key(&peer) || self.has_backoff_room(now) {
            self.next_dial.insert(peer, until);
        }
    }

    /// Whether the backoff map can take another peer, pruning expired entries if it is full.
    fn has_backoff_room(&mut self, now: Instant) -> bool {
        if self.next_dial.len() >= MAX_DIAL_BACKOFF_ENTRIES {
            self.next_dial.retain(|_, next| *next > now);
        }
        self.next_dial.len() < MAX_DIAL_BACKOFF_ENTRIES
    }
}

fn report(
    swarm: &mut Swarm<Behaviour>,
    id: &MessageId,
    source: &PeerId,
    acceptance: MessageAcceptance,
) {
    swarm
        .behaviour_mut()
        .gossipsub
        .report_message_validation_result(id, source, acceptance);
}

/// Current Unix time in seconds; protocol timestamps are wall-clock.
pub(crate) fn unix_now_secs() -> u64 {
    UNIX_EPOCH.elapsed().map_or(0, |elapsed| elapsed.as_secs())
}
