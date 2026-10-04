//! The peer set: which execution peers to dial, how many sessions to keep, and which peers to
//! stay away from.
//!
//! Execution peers of our chain are few (about 25 on our fork) and mostly full: a dial usually
//! ends with "too many peers" or a dropped handshake, and a slot opens only when a full peer's
//! own sessions churn. So getting a session is a matter of asking every known peer and asking
//! again soon. While it has fewer outbound sessions than its target (`max_sessions`) the peer
//! set dials peers that are due, at most as many at once as sessions are missing, so dialing
//! never opens more than the target.
//!
//! How soon a peer is dialed again depends on why the last dial failed:
//!
//! | Last dial | Next dial after |
//! |---|---|
//! | "too many peers", or dropped during the encrypted handshake (how a full reth node refuses) | [`FULL_PEER_RETRY`], 60 to 90 s, doubling with each refusal in a row, up to 8 to 12 min |
//! | a session the peer ended with "too many peers" | [`FULL_PEER_RETRY`] |
//! | TCP failure, timeout, a failed hello or status | the redial interval (5 to 7.5 min), doubling up to 40 to 60 min |
//! | a session that ended for another reason (closed, I/O error, other disconnect) | the redial interval |
//! | another fork or chain, no shared protocol, it called us useless, it broke the protocol | [`LONG_BACKOFF`], 1 to 1.5 h |
//! | its data failed verification | not for [`BAN_DURATION`] |
//!
//! It is never a tight loop: no peer is dialed more often than once per [`FULL_PEER_RETRY`],
//! at most [`MAX_DIALS_IN_FLIGHT`] dials run at once, and at most 30 start in any minute.
//!
//! On a network op-p2p-indexers share (`NetworkSpec::indexers_only_below`), one outbound slot,
//! and one inbound, beyond `max_sessions` is kept for an indexer; otherwise indexers compete
//! like any peer.
//! Outbound sessions we have not used for [`IDLE_RELEASE`] are closed, keeping [`KEEP_IDLE`]
//! for the receipts of new blocks; the outbound target then drops to what is kept, and rises
//! back to `max_sessions` once every kept session has been in use for a while. A released peer
//! waits [`RELEASED_REDIAL`] before it is dialed again.
//!
//! Nothing is dialed or accepted until the node knows a block to advertise as its tip (the
//! binary provides it): peers end a session at once with a node whose status says it is at
//! genesis.
//!
//! Does not discover peers (candidates arrive on a channel), run the handshake or the listener
//! (`session`), or decide which peer serves a request (`fetch`). `fetch` sees the open sessions
//! through [`Peers`] and reports peers that sent bad data or stopped answering.
//!

mod schedule;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use op_indexer_primitives::ExecutionPeer;
use reth_eth_wire_types::DisconnectReason;
use reth_network_peers::PeerId;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};

use self::schedule::{
    BAN_DURATION, DialTally, FULL_PEER_RETRY, LONG_BACKOFF, REDIAL_INTERVAL, Schedule,
};
use crate::ElError;
use crate::discovery::Candidate;
use crate::metrics::{self, DialOutcome, DropReason, EndLabel};
use crate::network::PeerConfig;
use crate::session::{
    self, Accepted, Direction, EndReason, SessionContext, SessionDriver, SessionEnd, SessionError,
    SessionHandle, host, unix_now,
};
use crate::warn_limit::WarnLimit;

/// Outbound sessions kept open while unused: enough for the receipts of new blocks. Others
/// that go unused for [`IDLE_RELEASE`] are closed, so the slot goes back to the full node that
/// lent it. Sessions with op-p2p-indexers and sessions peers opened are not closed for it.
const KEEP_IDLE: usize = 2;
/// Shortest time between two warnings about dropped peer reports.
const DROP_WARN_INTERVAL: Duration = Duration::from_mins(1);
/// How long an outbound session may go without a request of ours before it is released.
const IDLE_RELEASE: Duration = Duration::from_mins(10);
/// A kept session used within this long counts as busy.
const BUSY: Duration = Duration::from_mins(1);
/// Status ticks in a row (a minute each) with every kept session busy before the outbound
/// target lowered by a release goes back to `max_sessions`.
const BUSY_TICKS: u32 = 5;
/// Wait before a peer whose session we released unused is dialed again: much longer than an
/// ordinary redial, so releasing does not turn into dialing the same peers in a loop.
const RELEASED_REDIAL: Duration = Duration::from_mins(30);
/// How often the peer set looks for peers that have become due for a dial. New candidates and
/// finished dials are acted on at once; this only catches waits that ran out.
const DIAL_TICK: Duration = Duration::from_secs(1);
/// Most dials in progress at once, while below the session target. Enough to ask every peer
/// of our fork within a few seconds of knowing it.
const MAX_DIALS_IN_FLIGHT: usize = 8;
/// Reports from the fetcher waiting to be handled. Reports are rare; a full queue drops one,
/// and the peer is reported again on its next failure.
const REPORTS_CAPACITY: usize = 64;
/// How long shutdown waits for sessions to say goodbye before dropping them.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);
/// How often the peer set says how it is doing, when something changed since the last line.
const STATUS_INTERVAL: Duration = Duration::from_mins(1);

/// The open sessions, as a requester (the tip fetcher, range sync) sees them, and its way to
/// report a peer. A clone has its own view of what changed.
#[derive(Debug, Clone)]
pub struct Peers {
    sessions: watch::Receiver<Arc<[SessionHandle]>>,
    reports: mpsc::Sender<Report>,
    /// Limits the warning about dropped reports, shared by every clone.
    dropped: Arc<WarnLimit>,
}

/// What a requester tells the peer set about a peer.
#[derive(Debug, Clone, Copy)]
pub enum Report {
    /// The peer's answer failed verification: drop it and ban it.
    BadData(PeerId),
    /// The peer's answer could not be decoded: drop it for a long while, without a ban. It may
    /// be honest and this build behind.
    Undecodable(PeerId),
    /// The peer gave its first verified answer of this session: worth saving for a restart.
    Served(PeerId),
    /// The peer stopped answering requests: drop it, it may be dialed again later.
    Unresponsive(PeerId),
    /// The indexer says it holds blocks before Bedrock but answers "not held" for them again
    /// and again: drop it for a long while, so another indexer can take the slot.
    NotHolding(PeerId),
}

/// The peer set. [`PeerSet::run`] is its task.
#[derive(Debug)]
pub(crate) struct PeerSet {
    ctx: Arc<SessionContext>,
    candidates: mpsc::Receiver<Candidate>,
    accepted: mpsc::Receiver<Accepted>,
    reports: mpsc::Receiver<Report>,
    /// Where peers worth saving for the next start are reported.
    served: mpsc::Sender<ExecutionPeer>,
    /// The open sessions, published to the fetcher on every change.
    published: watch::Sender<Arc<[SessionHandle]>>,
    /// Whom to dial and when, and whom to stay away from.
    schedule: Schedule,
    sessions: HashMap<PeerId, Live>,
    dialing: HashSet<PeerId>,
    /// Dials in progress, running sessions and refusals being sent.
    tasks: JoinSet<Option<Done>>,
    /// Numbers sessions, so the end of an old session cannot remove a newer one of that peer.
    next_generation: u64,
    /// Sessions kept in each direction (`PeerConfig::max_sessions`); one more is dialed for an
    /// op-p2p-indexer on a network they share.
    max_sessions: usize,
    /// The dial for the indexer slot, while it is in [`Self::dialing`].
    slot_dial: Option<PeerId>,
    /// Outbound sessions dialed for (the indexer slot aside): `max_sessions`, or [`KEEP_IDLE`]
    /// after a session was released unused, until the kept ones are busy for a while.
    outbound_target: usize,
    /// Status ticks in a row with every kept session busy.
    busy_ticks: u32,
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
    Ended {
        end: SessionEnd,
        generation: u64,
    },
}

impl Peers {
    /// The handle, with the ends the peer set keeps: the reports it receives and where it
    /// publishes the open sessions.
    pub(crate) fn new() -> (
        Self,
        mpsc::Receiver<Report>,
        watch::Sender<Arc<[SessionHandle]>>,
    ) {
        let (published, sessions) = watch::channel(Arc::from(Vec::new()));
        let (reports_tx, reports) = mpsc::channel(REPORTS_CAPACITY);
        let peers = Self {
            sessions,
            reports: reports_tx,
            dropped: Arc::default(),
        };
        (peers, reports, published)
    }

    /// The sessions open now.
    #[must_use]
    pub fn sessions(&self) -> Arc<[SessionHandle]> {
        Arc::clone(&self.sessions.borrow())
    }

    /// Waits until a session opens or ends. Returns `false` once the peer set has stopped.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: a change not yet seen is reported by the next call.
    pub async fn changed(&mut self) -> bool {
        self.sessions.changed().await.is_ok()
    }

    /// Reports a peer to the peer set. Never waits: if the peer set is busy or gone the report
    /// is dropped, and the peer is reported again when it fails again.
    pub fn report(&self, report: Report) {
        if let Err(err) = self.reports.try_send(report)
            && let Some(held_back) = self.dropped.allow(DROP_WARN_INTERVAL)
        {
            warn!(%err, held_back, "execution peer report dropped: the peer set is busy");
        }
    }
}

impl PeerSet {
    /// Creates the peer set, with the ends of the requesters' handle ([`Peers::new`]). Dials
    /// nothing until [`Self::run`].
    pub(crate) fn new(
        ctx: Arc<SessionContext>,
        candidates: mpsc::Receiver<Candidate>,
        accepted: mpsc::Receiver<Accepted>,
        config: &PeerConfig,
        served: mpsc::Sender<ExecutionPeer>,
        reports: mpsc::Receiver<Report>,
        published: watch::Sender<Arc<[SessionHandle]>>,
    ) -> Self {
        let schedule = Schedule::new(ctx.spec().label, &config.saved_peers);
        Self {
            ctx,
            candidates,
            accepted,
            reports,
            served,
            published,
            schedule,
            sessions: HashMap::new(),
            dialing: HashSet::new(),
            tasks: JoinSet::new(),
            next_generation: 0,
            max_sessions: config.max_sessions,
            outbound_target: config.max_sessions,
            busy_ticks: 0,
            slot_dial: None,
        }
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
        let mut status_tick = interval(STATUS_INTERVAL);
        status_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_status = None;
        let outcome = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break Ok(()),
                Some(joined) = self.tasks.join_next() => match joined {
                    Ok(Some(done)) => self.finished(done, &cancel),
                    // A dial abandoned at shutdown, or a refusal that has been sent.
                    Ok(None) => {}
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
                _ = status_tick.tick() => {
                    self.log_status(&mut last_status);
                    self.release_idle();
                }
            }
            // After every event: a new candidate or a finished dial may allow another dial.
            self.start_dials(&cancel);
        };
        self.shutdown().await;
        outcome
    }

    /// Says how many sessions are open, how many peers are known and what the dials since
    /// the last line came to; only when that changed, so an idle node stays quiet.
    fn log_status(&mut self, last: &mut Option<(usize, usize)>) {
        let tally = self.schedule.take_tally();
        let now = (self.sessions.len(), self.schedule.known());
        if tally == DialTally::default() && *last == Some(now) {
            return;
        }
        *last = Some(now);
        info!(
            sessions = now.0,
            inbound = self.count(Direction::Inbound),
            known_peers = now.1,
            dials = tally.tried,
            full = tally.full,
            timed_out = tally.timed_out,
            failed = tally.other,
            "execution peers"
        );
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
    }

    /// Remembers a peer discovery found, or refreshes what is known about it.
    fn learn(&mut self, candidate: Candidate) {
        let (sessions, dialing) = (&self.sessions, &self.dialing);
        self.schedule.learn(candidate, |peer| {
            sessions.contains_key(peer) || dialing.contains(peer)
        });
    }

    /// Dials peers that are due: at most as many as outbound sessions are missing from
    /// `max_sessions`, counting the dials in flight, so no more than that many sessions are
    /// ever opened by dialing. On a network op-p2p-indexers share, one more slot is kept for an
    /// indexer (see [`Self::holds_slot`]). Dials nothing until a tip is known.
    fn start_dials(&mut self, cancel: &CancellationToken) {
        if !self.ctx.has_tip() {
            return;
        }
        let below = self.ctx.spec().indexers_only_below;
        // The indexer slot is extra: its session, or the dial for it, is not counted against
        // the ordinary slots.
        let slot_held = self.slot_holder(Direction::Outbound).is_some();
        let slot_dialing = self
            .slot_dial
            .is_some_and(|peer| self.dialing.contains(&peer));
        let outbound = self
            .count(Direction::Outbound)
            .saturating_add(self.dialing.len())
            .saturating_sub(usize::from(slot_held) + usize::from(slot_dialing));
        let in_flight = MAX_DIALS_IN_FLIGHT.saturating_sub(self.dialing.len());
        let (sessions, dialing, ctx) = (&self.sessions, &self.dialing, &self.ctx);
        let in_use = |peer: &PeerId| sessions.contains_key(peer) || dialing.contains(peer);
        let for_anyone = self.outbound_target.saturating_sub(outbound).min(in_flight);
        let mut due = self.schedule.take_due(for_anyone, in_use, |_| true);
        // One slot beyond the ordinary ones is kept for an indexer, only one discovery saw
        // carrying the flag in this run: a peer calling itself one takes that slot at most.
        // Not on a chain with nothing before Bedrock, and not past the ordinary slots: an
        // indexer that turned out to hold nothing before Bedrock counts as an ordinary peer.
        if below.is_some_and(|bedrock| bedrock > 0)
            && !slot_held
            && !slot_dialing
            && outbound <= self.max_sessions
            && due.len() < in_flight
        {
            let indexer = self
                .schedule
                .take_due(1, in_use, |peer| ctx.is_indexer(peer));
            self.slot_dial = indexer.first().map(|candidate| candidate.peer_id);
            due.extend(indexer);
        }
        for candidate in due {
            let peer = candidate.peer_id;
            self.dialing.insert(peer);
            let (ctx, cancel) = (Arc::clone(&self.ctx), cancel.clone());
            self.tasks.spawn(
                async move {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => None,
                        result = session::connect(&ctx, &candidate) => Some(Done::Dialed {
                            peer,
                            result: Box::new(result),
                        }),
                    }
                }
                .in_current_span(),
            );
        }
    }

    /// Handles a finished dial, session or refusal.
    fn finished(&mut self, done: Done, cancel: &CancellationToken) {
        match done {
            Done::Dialed { peer, result } => {
                self.dialing.remove(&peer);
                match *result {
                    Ok((handle, driver)) => {
                        metrics::dial(self.ctx.spec().label, DialOutcome::Connected);
                        if self.sessions.contains_key(&peer) {
                            // The peer dialed us while we dialed it.
                            self.refuse(driver, DisconnectReason::AlreadyConnected);
                        } else {
                            self.keep(handle, driver, cancel);
                        }
                    }
                    Err(err) => self.schedule.dial_failed(peer, &err),
                }
            }
            Done::Ended { end, generation } => self.ended(&end, generation),
        }
    }

    /// Keeps an inbound session if there is room for it and the peer is welcome.
    fn accept(&mut self, accepted: Accepted, cancel: &CancellationToken) {
        let Accepted { handle, driver } = accepted;
        let peer = handle.status().peer_id;
        // The indexer slot is extra: its session is not counted against the ordinary slots,
        // and an indexer with blocks before Bedrock may take it when it is free.
        let slot_held = self.slot_holder(Direction::Inbound).is_some();
        let ordinary = self
            .count(Direction::Inbound)
            .saturating_sub(usize::from(slot_held));
        let takes_slot = !slot_held && self.holds_slot(&handle);
        // One session per address (a /64 for IPv6): one host cannot take every slot.
        let from = host(handle.status().addr.ip());
        let same_host = self.sessions.values().any(|live| {
            let status = live.handle.status();
            status.direction == Direction::Inbound && host(status.addr.ip()) == from
        });
        let full = (ordinary >= self.max_sessions && !takes_slot) || same_host;
        // Without a tip the handshake advertised genesis: the peer would leave.
        if !self.ctx.has_tip()
            || full
            || self.sessions.contains_key(&peer)
            || self.schedule.is_banned(&peer)
        {
            metrics::inbound_refused(self.ctx.spec().label);
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
            eth = %status.version,
            latest = ?status.latest,
            "execution session opened"
        );
        debug!(%peer, fork_id = ?status.fork_id, head = %status.head_hash, "peer status");
        self.schedule.connected(&peer);
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        self.sessions.insert(peer, Live { handle, generation });
        let cancel = cancel.clone();
        self.tasks.spawn(
            async move {
                Some(Done::Ended {
                    // The driver's future is large; keep it off the task's stack frame.
                    end: Box::pin(driver.run(cancel)).await,
                    generation,
                })
            }
            .in_current_span(),
        );
        metrics::session_opened(self.ctx.spec().label, direction);
        self.publish();
    }

    /// Tells a peer why its session is not kept, without blocking the peer set.
    fn refuse(&mut self, driver: SessionDriver, reason: DisconnectReason) {
        self.tasks.spawn(
            async move {
                driver.reject(reason).await;
                None
            }
            .in_current_span(),
        );
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
        let (label, wait, detail) = match &end.reason {
            EndReason::Cancelled => (EndLabel::Cancelled, REDIAL_INTERVAL, None),
            EndReason::PeerDisconnected(DisconnectReason::TooManyPeers) => {
                (EndLabel::TooManyPeers, FULL_PEER_RETRY, None)
            }
            EndReason::PeerDisconnected(DisconnectReason::UselessPeer) => {
                (EndLabel::UselessPeer, LONG_BACKOFF, None)
            }
            EndReason::PeerDisconnected(_) => (EndLabel::Disconnected, REDIAL_INTERVAL, None),
            EndReason::Closed => (EndLabel::Closed, REDIAL_INTERVAL, None),
            EndReason::Io(err) => (EndLabel::Io, REDIAL_INTERVAL, Some(err.as_str())),
            EndReason::Protocol(err) => (EndLabel::Protocol, LONG_BACKOFF, Some(err.as_str())),
            // It asked for data and did not read it.
            EndReason::Stalled => (EndLabel::Stalled, LONG_BACKOFF, None),
        };
        metrics::session_ended(self.ctx.spec().label, label, end.lasted);
        info!(
            %peer,
            reason = ?end.reason,
            detail,
            lasted = ?end.lasted,
            "execution session ended"
        );
        // Never earlier than a wait already set, by a report that dropped the peer.
        self.schedule.wait(&peer, wait);
    }

    /// Acts on a report from a requester: saves a peer that served, drops one that failed.
    fn reported(&mut self, report: Report) {
        let (peer, reason, tell, wait) = match report {
            Report::Served(peer) => return self.save(peer),
            Report::BadData(peer) => {
                self.schedule.ban(peer);
                let tell = DisconnectReason::ProtocolBreach;
                (peer, DropReason::BadData, tell, BAN_DURATION)
            }
            Report::Undecodable(peer) => {
                let tell = DisconnectReason::UselessPeer;
                (peer, DropReason::Undecodable, tell, LONG_BACKOFF)
            }
            Report::NotHolding(peer) => {
                let tell = DisconnectReason::UselessPeer;
                (peer, DropReason::NotHolding, tell, LONG_BACKOFF)
            }
            Report::Unresponsive(peer) => {
                let tell = DisconnectReason::UselessPeer;
                (peer, DropReason::Unresponsive, tell, REDIAL_INTERVAL)
            }
        };
        // Set here, not when the driver ends: the peer would otherwise be due at once and be
        // dialed again in this same turn of the loop.
        self.schedule.wait(&peer, wait);
        // The session is removed now, so requesters stop using it at once; its driver ends on
        // the disconnect and reports that later.
        let Some(live) = self.sessions.remove(&peer) else {
            return;
        };
        live.handle.disconnect(tell);
        metrics::peer_dropped(self.ctx.spec().label, reason);
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

    /// Closes outbound sessions we have not sent a request on for [`IDLE_RELEASE`], keeping
    /// the [`KEEP_IDLE`] used most recently and the op-p2p-indexer used most recently: a full
    /// node's slot is not held for nothing. The outbound target then drops to [`KEEP_IDLE`],
    /// so no other peer is dialed in their place; it goes back to `max_sessions` once every
    /// kept session has been busy (used within [`BUSY`]) for [`BUSY_TICKS`] ticks in a row.
    fn release_idle(&mut self) {
        let mut ours: Vec<&SessionHandle> = self
            .sessions
            .values()
            .map(|live| &live.handle)
            .filter(|handle| handle.status().direction == Direction::Outbound)
            .collect();
        ours.sort_unstable_by_key(|handle| handle.idle());
        // The indexer slot's session is not released for being unused; other indexers are
        // ordinary peers here.
        let kept_indexer = ours
            .iter()
            .find(|handle| self.holds_slot(handle))
            .map(|handle| handle.peer_id());
        let mut released = false;
        for handle in ours
            .iter()
            .filter(|handle| Some(handle.peer_id()) != kept_indexer)
            .skip(KEEP_IDLE)
        {
            if handle.idle() >= IDLE_RELEASE {
                debug!(peer = %handle.peer_id(), "releasing an unused execution session");
                handle.disconnect(DisconnectReason::DisconnectRequested);
                // However the session then ends, the later wait holds.
                self.schedule.wait(&handle.peer_id(), RELEASED_REDIAL);
                released = true;
            }
        }
        if released {
            self.outbound_target = KEEP_IDLE.min(self.max_sessions);
            self.busy_ticks = 0;
        } else if ours
            .iter()
            .filter(|handle| Some(handle.peer_id()) != kept_indexer)
            .all(|handle| handle.idle() < BUSY)
        {
            // Busy for a while, not one burst: only then is the target raised again.
            self.busy_ticks = self.busy_ticks.saturating_add(1);
            if self.busy_ticks >= BUSY_TICKS {
                self.outbound_target = self.max_sessions;
            }
        } else {
            self.busy_ticks = 0;
        }
    }

    /// Whether `handle`'s peer can hold the indexer slot: an op-p2p-indexer that says it holds
    /// blocks before Bedrock, the ones only indexers share.
    fn holds_slot(&self, handle: &SessionHandle) -> bool {
        handle.status().indexer
            && self
                .ctx
                .spec()
                .indexers_only_below
                .is_some_and(|bedrock| handle.range().earliest < bedrock)
    }

    /// The session holding the indexer slot in `direction`, if any: the first that can.
    fn slot_holder(&self, direction: Direction) -> Option<PeerId> {
        self.sessions
            .values()
            .map(|live| &live.handle)
            .find(|handle| handle.status().direction == direction && self.holds_slot(handle))
            .map(SessionHandle::peer_id)
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
