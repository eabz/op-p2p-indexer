//! The peer set: which execution peers to dial, how many sessions to keep, and which peers to
//! stay away from.
//!
//! Execution peers of our chain are few (about 25 on our fork) and mostly full: a dial usually
//! ends with "too many peers" or a dropped handshake, and a slot opens only when a full peer's
//! own sessions churn. So getting a session is a matter of asking every known peer and asking
//! again soon. While it has fewer outbound sessions than its target ([`MAX_SESSIONS`]) the peer
//! set dials peers that are due, at most as many at once as sessions are missing, so dialing
//! never opens more than the target.
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
//! at most [`MAX_DIALS_IN_FLIGHT`] dials run at once, and at most 30 start in any minute. The
//! waits, the bans and the choice of whom to dial next are in `schedule`.
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
use tracing::{debug, info, warn};

use self::schedule::{BAN_DURATION, FULL_PEER_RETRY, LONG_BACKOFF, REDIAL_INTERVAL, Schedule};
use crate::ElError;
use crate::discovery::Candidate;
use crate::metrics::{self, DialOutcome, DropReason, EndLabel};
use crate::session::{
    self, Accepted, Direction, EndReason, SessionContext, SessionDriver, SessionEnd, SessionError,
    SessionHandle, unix_now,
};

/// Sessions the node keeps in each direction: this many it dials, and as many again it
/// accepts. A handful is enough for a node that asks slowly.
const MAX_SESSIONS: usize = 8;
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

/// The open sessions, as a requester (the tip fetcher, range sync) sees them, and its way to
/// report a peer. A clone has its own view of what changed.
#[derive(Debug, Clone)]
pub(crate) struct Peers {
    sessions: watch::Receiver<Arc<[SessionHandle]>>,
    reports: mpsc::Sender<Report>,
}

/// What the fetcher tells the peer set about a peer.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Report {
    /// The peer's answer failed verification: drop it and ban it.
    BadData(PeerId),
    /// The peer's answer could not be decoded: drop it for a long while, without a ban. It may
    /// be honest and this build behind.
    Undecodable(PeerId),
    /// The peer gave its first verified answer of this session: worth saving for a restart.
    Served(PeerId),
    /// The peer stopped answering requests: drop it, it may be dialed again later.
    Unresponsive(PeerId),
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
        ctx: Arc<SessionContext>,
        candidates: mpsc::Receiver<Candidate>,
        accepted: mpsc::Receiver<Accepted>,
        saved: &[ExecutionPeer],
        served: mpsc::Sender<ExecutionPeer>,
    ) -> (Self, Peers) {
        let (published, sessions) = watch::channel(Arc::from(Vec::new()));
        let (reports_tx, reports) = mpsc::channel(REPORTS_CAPACITY);
        let peer_set = Self {
            ctx,
            candidates,
            accepted,
            reports,
            served,
            published,
            schedule: Schedule::new(saved),
            sessions: HashMap::new(),
            dialing: HashSet::new(),
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
    }

    /// Remembers a peer discovery found, or refreshes what is known about it.
    fn learn(&mut self, candidate: Candidate) {
        let (sessions, dialing) = (&self.sessions, &self.dialing);
        self.schedule.learn(candidate, |peer| {
            sessions.contains_key(peer) || dialing.contains(peer)
        });
    }

    /// Dials peers that are due: at most as many as outbound sessions are missing from
    /// [`MAX_SESSIONS`], counting the dials in flight, so no more than that many sessions are
    /// ever opened by dialing. Dials nothing until a tip is known.
    fn start_dials(&mut self, cancel: &CancellationToken) {
        if !self.ctx.has_tip() {
            return;
        }
        let missing = MAX_SESSIONS
            .saturating_sub(self.count(Direction::Outbound))
            .saturating_sub(self.dialing.len());
        if missing == 0 {
            return;
        }
        let wanted = missing.min(MAX_DIALS_IN_FLIGHT.saturating_sub(self.dialing.len()));
        let (sessions, dialing) = (&self.sessions, &self.dialing);
        let due = self.schedule.take_due(wanted, |peer| {
            sessions.contains_key(peer) || dialing.contains(peer)
        });
        for candidate in due {
            let peer = candidate.peer_id;
            self.dialing.insert(peer);
            let (ctx, cancel) = (Arc::clone(&self.ctx), cancel.clone());
            self.tasks.spawn(async move {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    result = session::connect(&ctx, &candidate) => Some(Done::Dialed {
                        peer,
                        result: Box::new(result),
                    }),
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
        let full = self.count(Direction::Inbound) >= MAX_SESSIONS;
        // Without a tip the handshake advertised genesis: the peer would leave.
        if !self.ctx.has_tip()
            || full
            || self.sessions.contains_key(&peer)
            || self.schedule.is_banned(&peer)
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
            latest = ?status.latest,
            "execution session opened"
        );
        debug!(%peer, fork_id = ?status.fork_id, head = %status.head_hash, "peer status");
        self.schedule.connected(&peer);
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        self.sessions.insert(peer, Live { handle, generation });
        let cancel = cancel.clone();
        self.tasks.spawn(async move {
            Some(Done::Ended {
                // The driver's future is large; keep it off the task's stack frame.
                end: Box::pin(driver.run(cancel)).await,
                generation,
            })
        });
        metrics::session_opened(direction);
        self.publish();
    }

    /// Tells a peer why its session is not kept, without blocking the peer set.
    fn refuse(&mut self, driver: SessionDriver, reason: DisconnectReason) {
        self.tasks.spawn(async move {
            driver.reject(reason).await;
            None
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
        metrics::session_ended(label, end.lasted);
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
