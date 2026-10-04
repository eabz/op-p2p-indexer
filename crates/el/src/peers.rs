//! The peer set: which execution peers to dial, how many sessions to keep, and which peers to
//! stay away from.
//!
//! Execution peers of our chain are few (about 25 on our fork) and mostly full: a dial usually
//! ends with "too many peers" or a dropped handshake, and a slot opens only when a full peer's
//! own sessions churn. So getting a session is a matter of asking every known peer and asking
//! again soon. While it has fewer outbound sessions than its target the peer set dials every
//! peer that is due, several at once; at the target it stops dialing. A session won by a dial
//! is always kept, so a burst of dials can end slightly above the target.
//!
//! How soon a peer is dialed again depends on why the last dial failed:
//!
//! | Last dial | Next dial after |
//! |---|---|
//! | "too many peers", or a session the peer ended with it | [`FULL_PEER_RETRY`], 60 to 90 s |
//! | dropped during the encrypted handshake (what a full reth node does) | the same, doubling with each drop in a row, up to 8 to 12 min |
//! | TCP failure, timeout, a failed hello or status | the redial interval (5 to 7.5 min), doubling up to 40 to 60 min |
//! | a session that ended for another reason (closed, I/O error, other disconnect) | the redial interval |
//! | another fork or chain, no shared protocol, it called us useless, it broke the protocol | [`LONG_BACKOFF`], 1 to 1.5 h |
//! | its data failed verification | not for [`BAN_DURATION`] |
//!
//! It is never a tight loop: no peer is dialed more often than once per [`FULL_PEER_RETRY`],
//! at most [`MAX_DIALS_IN_FLIGHT`] dials run at once, and at most [`MAX_DIALS_PER_MINUTE`]
//! start in any minute.
//!
//! Nothing is dialed or accepted until the node knows a block to advertise as its tip (the
//! fetcher sets it in the session context when the first request arrives): peers end a session
//! at once with a node whose status says it is at genesis.
//!
//! Does not discover peers (candidates arrive on a channel), run the handshake or the listener
//! (`session`), or decide which peer serves a request (`fetch`). `fetch` sees the open sessions
//! through [`Peers`] and reports peers that sent bad data or stopped answering.
//!
//! Everything a peer can make us remember is bounded: known peers and bans are capped maps
//! that evict their least useful entry.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use op_indexer_primitives::ExecutionPeer;
use reth_eth_wire_types::DisconnectReason;
use reth_network_peers::PeerId;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::ElError;
use crate::config::PeerSetConfig;
use crate::discovery::Candidate;
use crate::metrics::{self, DialOutcome, DropReason, EndLabel};
use crate::session::{
    Accepted, Direction, EndReason, Session, SessionContext, SessionDriver, SessionEnd,
    SessionError, SessionHandle, unix_now,
};

/// How often the peer set looks for peers that have become due for a dial. New candidates and
/// finished dials are acted on at once; this only catches waits that ran out.
const DIAL_TICK: Duration = Duration::from_secs(1);
/// Most dials in progress at once, while below the session target. Enough to ask every peer
/// of our fork within a few seconds of knowing it.
const MAX_DIALS_IN_FLIGHT: usize = 8;
/// Most dials started in any minute. With every known peer waiting at least
/// [`FULL_PEER_RETRY`], about 25 peers stay under it; it bounds what a flood of node records
/// can make us dial.
const MAX_DIALS_PER_MINUTE: usize = 30;
/// The window [`MAX_DIALS_PER_MINUTE`] is counted over.
const DIAL_WINDOW: Duration = Duration::from_secs(60);
/// Shortest wait before the same peer is dialed again, and the wait after "too many peers" or
/// a dropped handshake: a full peer's slots churn, so asking again soon is what gets one. The
/// jitter adds up to half, so 60 to 90 s.
const FULL_PEER_RETRY: Duration = Duration::from_secs(60);
/// Most peers remembered for dialing. Discovery finds a few dozen peers of our fork; the cap
/// only bounds what a flood of node records can make us hold.
const MAX_KNOWN_PEERS: usize = 1024;
/// Most bans remembered. Beyond it the ban that expires first is forgotten.
const MAX_BANNED_PEERS: usize = 1024;
/// How long a peer whose data failed verification is neither dialed nor accepted.
const BAN_DURATION: Duration = Duration::from_hours(6);
/// Wait before dialing again a peer that cannot serve us for a reason that does not change
/// soon: another fork or chain, no shared protocol version, or it called us useless.
const LONG_BACKOFF: Duration = Duration::from_hours(1);
/// Failures in a row double the wait this many times at most (so up to 8 times the base).
const MAX_BACKOFF_DOUBLINGS: u32 = 3;
/// Reports from the fetcher waiting to be handled. Reports are rare; a full queue drops one,
/// and the peer is reported again on its next failure.
const REPORTS_CAPACITY: usize = 64;
/// How long shutdown waits for sessions to say goodbye before dropping them.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

/// The open sessions, as the fetcher sees them, and its way to report a peer.
#[derive(Debug)]
pub(crate) struct Peers {
    sessions: watch::Receiver<Arc<[SessionHandle]>>,
    reports: mpsc::Sender<Report>,
}

/// What the fetcher tells the peer set about a peer.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Report {
    /// The peer's answer failed verification or could not be decoded: drop and remember it.
    BadData(PeerId),
    /// The peer gave its first verified answer of this session: worth saving for a restart.
    Served(PeerId),
    /// The peer stopped answering requests: drop it, it may be dialed again later.
    Unresponsive(PeerId),
}

/// The peer set. [`PeerSet::run`] is its task.
#[derive(Debug)]
pub(crate) struct PeerSet {
    config: PeerSetConfig,
    ctx: Arc<SessionContext>,
    candidates: mpsc::Receiver<Candidate>,
    accepted: mpsc::Receiver<Accepted>,
    reports: mpsc::Receiver<Report>,
    /// Where peers worth saving for the next start are reported.
    served: mpsc::Sender<ExecutionPeer>,
    /// The open sessions, published to the fetcher on every change.
    published: watch::Sender<Arc<[SessionHandle]>>,
    /// Whether dialing has started: the session context had a tip at some dial tick.
    released: bool,
    /// Peers discovery told us about, by id. At most [`MAX_KNOWN_PEERS`].
    known: HashMap<PeerId, Known>,
    /// Peers not to dial or accept, with when the ban ends. At most [`MAX_BANNED_PEERS`].
    banned: HashMap<PeerId, Instant>,
    sessions: HashMap<PeerId, Live>,
    dialing: HashSet<PeerId>,
    /// When the dials of the last [`DIAL_WINDOW`] started, oldest first.
    recent_dials: VecDeque<Instant>,
    /// Dials in progress, running sessions and refusals being sent.
    tasks: JoinSet<Done>,
    /// Numbers sessions, so the end of an old session cannot remove a newer one of that peer.
    next_generation: u64,
}

/// A peer that can be dialed.
#[derive(Debug)]
struct Known {
    candidate: Candidate,
    /// Not dialed before this.
    next_dial: Instant,
    /// Failed dials since the last session.
    failures: u32,
    /// When discovery last reported it; the longest unseen is evicted first.
    last_seen: Instant,
}

/// An open session.
#[derive(Debug)]
struct Live {
    handle: SessionHandle,
    generation: u64,
}

/// What a task of the peer set finished with.
#[derive(Debug)]
enum Done {
    Dialed {
        peer: PeerId,
        result: Box<Result<(SessionHandle, SessionDriver), SessionError>>,
    },
    /// A dial abandoned because the node is shutting down.
    DialCancelled,
    Ended {
        end: SessionEnd,
        generation: u64,
    },
    /// A session we refused has been told why.
    Refused,
}

impl Peers {
    /// The sessions open now.
    pub(crate) fn sessions(&self) -> Arc<[SessionHandle]> {
        Arc::clone(&self.sessions.borrow())
    }

    /// Waits until a session opens or ends. Returns `false` once the peer set has stopped.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: a change not yet seen is reported by the next call.
    pub(crate) async fn changed(&mut self) -> bool {
        self.sessions.changed().await.is_ok()
    }

    /// Reports a peer to the peer set. Never waits: if the peer set is busy or gone the report
    /// is dropped, and the peer is reported again when it fails again.
    pub(crate) fn report(&self, report: Report) {
        if let Err(err) = self.reports.try_send(report) {
            debug!(%err, "peer report dropped");
        }
    }
}

impl PeerSet {
    /// Creates the peer set and the handle the fetcher uses. Dials nothing until
    /// [`Self::run`].
    pub(crate) fn new(
        config: PeerSetConfig,
        ctx: Arc<SessionContext>,
        candidates: mpsc::Receiver<Candidate>,
        accepted: mpsc::Receiver<Accepted>,
        saved: &[ExecutionPeer],
        served: mpsc::Sender<ExecutionPeer>,
    ) -> (Self, Peers) {
        // Saved peers are known from the start, so they are dialed as soon as a tip is known,
        // before discovery has found anyone.
        let now = Instant::now();
        let known = saved
            .iter()
            .take(MAX_KNOWN_PEERS)
            .map(|peer| {
                let candidate = Candidate {
                    peer_id: peer.id,
                    addr: peer.addr,
                };
                let known = Known {
                    candidate,
                    next_dial: now,
                    failures: 0,
                    last_seen: now,
                };
                (peer.id, known)
            })
            .collect();
        let (published, sessions) = watch::channel(Arc::from(Vec::new()));
        let (reports_tx, reports) = mpsc::channel(REPORTS_CAPACITY);
        let peer_set = Self {
            config,
            ctx,
            candidates,
            accepted,
            reports,
            served,
            published,
            released: false,
            known,
            banned: HashMap::new(),
            sessions: HashMap::new(),
            dialing: HashSet::new(),
            recent_dials: VecDeque::new(),
            tasks: JoinSet::new(),
            next_generation: 0,
        };
        let peers = Peers {
            sessions,
            reports: reports_tx,
        };
        (peer_set, peers)
    }

    /// Runs until `cancel` fires: learns candidates, dials when below the target, keeps or
    /// refuses inbound sessions, and drops reported peers. On shutdown the sessions get a
    /// short time to tell their peers.
    ///
    /// # Errors
    ///
    /// Returns [`ElError::ChannelClosed`] if discovery or the listener stopped while the node
    /// was running, and [`ElError::Task`] if a session task panicked.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), ElError> {
        let mut dial_tick = interval(DIAL_TICK);
        dial_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let outcome = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break Ok(()),
                Some(joined) = self.tasks.join_next() => match joined {
                    Ok(done) => self.finished(done, &cancel),
                    Err(source) if source.is_panic() => {
                        break Err(ElError::Task { task: "session", source });
                    }
                    // Aborted: only happens during shutdown.
                    Err(_aborted) => {}
                },
                candidate = self.candidates.recv() => match candidate {
                    Some(candidate) => self.learn(candidate),
                    None => break closed("candidates", &cancel),
                },
                accepted = self.accepted.recv() => match accepted {
                    Some(accepted) => self.accept(accepted, &cancel),
                    None => break closed("accepted sessions", &cancel),
                },
                Some(report) = self.reports.recv() => self.reported(report),
                _ = dial_tick.tick() => {}
            }
            // After every event: a new candidate or a finished dial may allow another dial.
            self.start_dials(&cancel);
        };
        self.shutdown().await;
        outcome
    }

    /// Lets running sessions end on the cancelled token, then drops whatever is left.
    async fn shutdown(&mut self) {
        let drained = timeout(SHUTDOWN_GRACE, async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            self.tasks.shutdown().await;
        }
        self.sessions.clear();
        metrics::sessions_alive(0);
    }

    /// Remembers a peer discovery found, or refreshes what is known about it.
    fn learn(&mut self, candidate: Candidate) {
        let now = Instant::now();
        if let Some(known) = self.known.get_mut(&candidate.peer_id) {
            known.candidate = candidate;
            known.last_seen = now;
            return;
        }
        if self.known.len() >= MAX_KNOWN_PEERS {
            // Evict the peer discovery has not mentioned for the longest time.
            let stale = self
                .known
                .iter()
                .filter(|(id, _)| !self.sessions.contains_key(*id) && !self.dialing.contains(*id))
                .min_by_key(|(_, known)| known.last_seen)
                .map(|(id, _)| *id);
            let Some(stale) = stale else {
                return;
            };
            self.known.remove(&stale);
        }
        debug!(peer = %candidate.peer_id, addr = %candidate.addr, "learned execution peer");
        self.known.insert(
            candidate.peer_id,
            Known {
                candidate,
                next_dial: now,
                failures: 0,
                last_seen: now,
            },
        );
    }

    /// Dials peers that are due, as many as outbound sessions are missing. Dials nothing until
    /// a tip is known.
    fn start_dials(&mut self, cancel: &CancellationToken) {
        if !self.released {
            if !self.ctx.has_tip() {
                return;
            }
            self.released = true;
            info!(
                known_peers = self.known.len(),
                "the node has a tip to advertise; dialing execution peers"
            );
        }
        if self.count(Direction::Outbound) >= self.config.target_sessions {
            return;
        }
        let now = Instant::now();
        while self
            .recent_dials
            .front()
            .is_some_and(|started| now.duration_since(*started) >= DIAL_WINDOW)
        {
            self.recent_dials.pop_front();
        }
        let wanted = MAX_DIALS_IN_FLIGHT
            .saturating_sub(self.dialing.len())
            .min(MAX_DIALS_PER_MINUTE.saturating_sub(self.recent_dials.len()));
        if wanted == 0 {
            return;
        }
        // Peers that failed least first, then those waiting longest.
        let mut due: Vec<(u32, Instant, PeerId)> = self
            .known
            .iter()
            .filter(|(id, known)| {
                known.next_dial <= now
                    && !self.sessions.contains_key(*id)
                    && !self.dialing.contains(*id)
                    && !self.is_banned(id, now)
            })
            .map(|(id, known)| (known.failures, known.next_dial, *id))
            .collect();
        due.sort_unstable();
        for (_, _, peer) in due.into_iter().take(wanted) {
            let Some(known) = self.known.get_mut(&peer) else {
                continue;
            };
            // Set before the dial ends, so no path can dial a peer twice within the floor.
            known.next_dial = now + jittered(FULL_PEER_RETRY);
            self.recent_dials.push_back(now);
            let candidate = known.candidate.clone();
            self.dialing.insert(peer);
            let (ctx, cancel) = (Arc::clone(&self.ctx), cancel.clone());
            self.tasks.spawn(async move {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => Done::DialCancelled,
                    result = Session::connect(&ctx, &candidate) => Done::Dialed {
                        peer,
                        result: Box::new(result),
                    },
                }
            });
        }
    }

    /// Handles a finished dial, session or refusal.
    fn finished(&mut self, done: Done, cancel: &CancellationToken) {
        match done {
            Done::Dialed { peer, result } => {
                self.dialing.remove(&peer);
                match *result {
                    Ok((handle, driver)) => {
                        metrics::dial(DialOutcome::Connected);
                        if self.sessions.contains_key(&peer) {
                            // The peer dialed us while we dialed it.
                            self.refuse(driver, DisconnectReason::AlreadyConnected);
                        } else {
                            self.keep(handle, driver, cancel);
                        }
                    }
                    Err(err) => self.dial_failed(peer, &err),
                }
            }
            Done::Ended { end, generation } => self.ended(&end, generation),
            Done::DialCancelled | Done::Refused => {}
        }
    }

    /// Records a failed dial and when the peer may be dialed again.
    fn dial_failed(&mut self, peer: PeerId, err: &SessionError) {
        let interval = self.config.redial_interval;
        // The outcome, the wait after a first failure, and whether failures in a row double it.
        let (outcome, base, grows) = match err {
            // A full peer: its slots churn, so ask again soon, every time.
            SessionError::Hello(Some(DisconnectReason::TooManyPeers))
            | SessionError::Status {
                reason: Some(DisconnectReason::TooManyPeers),
            } => (DialOutcome::TooManyPeers, FULL_PEER_RETRY, false),
            // How a full reth node refuses: the same, but back off if it keeps happening.
            SessionError::Ecies => (DialOutcome::HandshakeDropped, FULL_PEER_RETRY, true),
            SessionError::Tcp(_) => (DialOutcome::Unreachable, interval, true),
            SessionError::Timeout { .. } => (DialOutcome::Timeout, interval, true),
            SessionError::Hello(_) | SessionError::Status { .. } => {
                (DialOutcome::Failed, interval, true)
            }
            SessionError::ForkMismatch { .. } | SessionError::WrongChain => {
                (DialOutcome::WrongFork, LONG_BACKOFF, false)
            }
            SessionError::NoSharedEth => (DialOutcome::Incompatible, LONG_BACKOFF, false),
        };
        metrics::dial(outcome);
        debug!(%peer, %err, "dial failed");
        let Some(known) = self.known.get_mut(&peer) else {
            return;
        };
        let wait = if grows {
            let doublings = known.failures.min(MAX_BACKOFF_DOUBLINGS);
            known.failures = known.failures.saturating_add(1);
            base.saturating_mul(1_u32.checked_shl(doublings).unwrap_or(u32::MAX))
        } else {
            base
        };
        known.next_dial = Instant::now() + jittered(wait);
    }

    /// Keeps an inbound session if there is room for it and the peer is welcome.
    fn accept(&mut self, accepted: Accepted, cancel: &CancellationToken) {
        let Accepted { handle, driver } = accepted;
        let peer = handle.status().peer_id;
        let full = self.count(Direction::Inbound) >= self.config.max_inbound;
        // Before dialing is released the handshake advertised genesis: the peer would leave.
        if !self.released
            || full
            || self.sessions.contains_key(&peer)
            || self.is_banned(&peer, Instant::now())
        {
            metrics::inbound_refused();
            self.refuse(driver, DisconnectReason::TooManyPeers);
            return;
        }
        self.keep(handle, driver, cancel);
    }

    /// Adds a session to the set and runs its driver.
    fn keep(&mut self, handle: SessionHandle, driver: SessionDriver, cancel: &CancellationToken) {
        let status = handle.status();
        let (peer, direction) = (status.peer_id, status.direction);
        info!(
            %peer,
            addr = %status.addr,
            client = %status.client,
            direction = ?status.direction,
            eth = status.eth_version,
            latest = ?status.latest,
            "execution session opened"
        );
        debug!(%peer, fork_id = ?status.fork_id, head = %status.head_hash, "peer status");
        if let Some(known) = self.known.get_mut(&peer) {
            known.failures = 0;
        }
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        self.sessions.insert(peer, Live { handle, generation });
        let cancel = cancel.clone();
        self.tasks.spawn(async move {
            Done::Ended {
                end: driver.run(cancel).await,
                generation,
            }
        });
        metrics::session_opened(direction);
        self.publish();
    }

    /// Tells a peer why its session is not kept, without blocking the peer set.
    fn refuse(&mut self, driver: SessionDriver, reason: DisconnectReason) {
        self.tasks.spawn(async move {
            driver.reject(reason).await;
            Done::Refused
        });
    }

    /// Removes an ended session and decides when its peer may be dialed again.
    fn ended(&mut self, end: &SessionEnd, generation: u64) {
        let peer = end.peer_id;
        if self
            .sessions
            .get(&peer)
            .is_some_and(|live| live.generation == generation)
        {
            self.sessions.remove(&peer);
            self.publish();
        }
        let interval = self.config.redial_interval;
        let (label, wait, detail) = match &end.reason {
            EndReason::Cancelled => (EndLabel::Cancelled, interval, None),
            EndReason::PeerDisconnected(DisconnectReason::TooManyPeers) => {
                (EndLabel::TooManyPeers, FULL_PEER_RETRY, None)
            }
            EndReason::PeerDisconnected(DisconnectReason::UselessPeer) => {
                (EndLabel::UselessPeer, LONG_BACKOFF, None)
            }
            EndReason::PeerDisconnected(_) => (EndLabel::Disconnected, interval, None),
            EndReason::Closed => (EndLabel::Closed, interval, None),
            EndReason::Io(err) => (EndLabel::Io, interval, Some(err.as_str())),
            EndReason::Protocol(err) => (EndLabel::Protocol, LONG_BACKOFF, Some(err.as_str())),
        };
        metrics::session_ended(label, end.lasted);
        info!(
            %peer,
            reason = ?end.reason,
            detail,
            lasted = ?end.lasted,
            "execution session ended"
        );
        if let Some(known) = self.known.get_mut(&peer) {
            known.next_dial = Instant::now() + jittered(wait);
        }
    }

    /// Acts on a report from the fetcher: saves a peer that served, drops one that failed.
    fn reported(&mut self, report: Report) {
        let (peer, reason, tell) = match report {
            Report::Served(peer) => return self.save(peer),
            Report::BadData(peer) => (peer, DropReason::BadData, DisconnectReason::ProtocolBreach),
            Report::Unresponsive(peer) => (
                peer,
                DropReason::Unresponsive,
                DisconnectReason::UselessPeer,
            ),
        };
        if matches!(reason, DropReason::BadData) {
            self.ban(peer);
        }
        // The session is removed now, so the fetcher stops using it at once; its driver ends
        // on the disconnect and reports that later.
        let Some(live) = self.sessions.remove(&peer) else {
            return;
        };
        live.handle.disconnect(tell);
        metrics::peer_dropped(reason);
        warn!(%peer, ?reason, client = %live.handle.status().client, "dropped execution peer");
        self.publish();
    }

    /// Reports a peer that served us, so the binary can save it for the next start. Only a
    /// peer we dialed: an inbound session's address is the peer's outgoing port, which cannot
    /// be dialed.
    fn save(&self, peer: PeerId) {
        let Some(live) = self.sessions.get(&peer) else {
            return;
        };
        let status = live.handle.status();
        if status.direction != Direction::Outbound {
            return;
        }
        let served = ExecutionPeer {
            id: peer,
            addr: status.addr,
            last_served_secs: unix_now(),
        };
        // Never waits: a full or closed channel only costs this peer its place in the store.
        if let Err(err) = self.served.try_send(served) {
            debug!(%peer, %err, "served peer not reported");
        }
    }

    /// Remembers not to dial or accept `peer` for [`BAN_DURATION`].
    fn ban(&mut self, peer: PeerId) {
        let now = Instant::now();
        if self.banned.len() >= MAX_BANNED_PEERS && !self.banned.contains_key(&peer) {
            let first_to_expire = self
                .banned
                .iter()
                .min_by_key(|(_, until)| **until)
                .map(|(id, _)| *id);
            if let Some(first_to_expire) = first_to_expire {
                self.banned.remove(&first_to_expire);
            }
        }
        self.banned.insert(peer, now + BAN_DURATION);
    }

    fn is_banned(&self, peer: &PeerId, now: Instant) -> bool {
        self.banned.get(peer).is_some_and(|until| *until > now)
    }

    /// Sessions open in one direction.
    fn count(&self, direction: Direction) -> usize {
        self.sessions
            .values()
            .filter(|live| live.handle.status().direction == direction)
            .count()
    }

    /// Publishes the open sessions to the fetcher.
    fn publish(&self) {
        let handles: Vec<SessionHandle> = self
            .sessions
            .values()
            .map(|live| live.handle.clone())
            .collect();
        metrics::sessions_alive(handles.len());
        self.published.send_replace(Arc::from(handles));
    }
}

/// A channel a component needs has closed: an error unless the node is shutting down.
pub(crate) fn closed(channel: &'static str, cancel: &CancellationToken) -> Result<(), ElError> {
    if cancel.is_cancelled() {
        Ok(())
    } else {
        Err(ElError::ChannelClosed { channel })
    }
}

/// `wait` lengthened by up to half, so peers that failed together are not dialed together and
/// no wait is shorter than asked.
fn jittered(wait: Duration) -> Duration {
    wait.saturating_add((wait / 2).mul_f64(fastrand::f64()))
}
