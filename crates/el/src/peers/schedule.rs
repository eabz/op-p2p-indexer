//! The dial schedule: the peers that can be dialed, when each may be dialed again, and the
//! peers to stay away from.
//!
//! Does not dial or hold sessions (the peer set does): it answers "whom next" and remembers
//! how the last attempt ended. Everything a peer can make it remember is bounded: known peers
//! and bans are capped maps that evict their least useful entry.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use op_indexer_primitives::ExecutionPeer;
use reth_eth_wire_types::DisconnectReason;
use reth_network_peers::PeerId;
use tokio::time::Instant;
use tracing::debug;

use crate::discovery::Candidate;
use crate::metrics::{self, DialOutcome};
use crate::session::SessionError;

/// Base wait before dialing a peer again after a TCP failure, a timeout, a failed hello or
/// status, or a session that ended for an ordinary reason. Full peers are retried sooner.
pub(super) const REDIAL_INTERVAL: Duration = Duration::from_mins(5);
/// Most dials started in any minute: one a second. It bounds what a flood of node records can
/// make us dial. On a network with few peers every one of them waits at least
/// [`FULL_PEER_RETRY`], so it is never reached; on one with hundreds of candidates, most of
/// them full, it is what decides how fast a free slot is found.
const MAX_DIALS_PER_MINUTE: usize = 60;
/// The window [`MAX_DIALS_PER_MINUTE`] is counted over.
const DIAL_WINDOW: Duration = Duration::from_secs(60);
/// Shortest wait before the same peer is dialed again, and the wait after "too many peers" or
/// a dropped handshake: a full peer's slots churn, so asking again soon is what gets one. The
/// jitter adds up to half, so 60 to 90 s.
pub(super) const FULL_PEER_RETRY: Duration = Duration::from_secs(60);
/// Most peers remembered for dialing. Discovery finds a few dozen peers of our fork; the cap
/// only bounds what a flood of node records can make us hold.
const MAX_KNOWN_PEERS: usize = 1024;
/// Most bans remembered. Beyond it the ban that expires first is forgotten.
const MAX_BANNED_PEERS: usize = 1024;
/// How long a peer whose data failed verification is neither dialed nor accepted.
pub(super) const BAN_DURATION: Duration = Duration::from_hours(6);
/// Wait before dialing again a peer that cannot serve us for a reason that does not change
/// soon: another fork or chain, no shared protocol version, or it called us useless.
pub(super) const LONG_BACKOFF: Duration = Duration::from_hours(1);
/// Failures in a row double the wait this many times at most (so up to 8 times the base).
const MAX_BACKOFF_DOUBLINGS: u32 = 3;

/// Whom to dial and when.
#[derive(Debug)]
pub(super) struct Schedule {
    /// The network, as the metrics label.
    network: &'static str,
    /// Peers discovery told us about, by id. At most [`MAX_KNOWN_PEERS`].
    known: HashMap<PeerId, Known>,
    /// Peers not to dial or accept, with when the ban ends. At most [`MAX_BANNED_PEERS`].
    banned: HashMap<PeerId, Instant>,
    /// When the dials of the last [`DIAL_WINDOW`] started, oldest first.
    recent_dials: VecDeque<Instant>,
}

/// A peer that can be dialed.
#[derive(Debug)]
struct Known {
    candidate: Candidate,
    /// Not dialed before this.
    next_dial: Instant,
    /// Failed dials since the last session.
    failures: u32,
    /// Dials since the last session, whatever their end.
    attempts: u32,
    /// Whether a session with it has ever been open: a peer that exists and speaks our
    /// protocol, which fresh ids from discovery must not crowd out.
    proven: bool,
    /// When discovery last reported it; the longest unseen is evicted first.
    last_seen: Instant,
}

impl Schedule {
    /// A schedule that knows the peers `saved` from an earlier run, due at once: they are
    /// dialed as soon as a tip is known, before discovery has found anyone.
    pub(super) fn new(network: &'static str, saved: &[ExecutionPeer]) -> Self {
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
                    attempts: 0,
                    // Saved because it served us in an earlier run.
                    proven: true,
                    last_seen: now,
                };
                (peer.id, known)
            })
            .collect();
        Self {
            network,
            known,
            banned: HashMap::new(),
            recent_dials: VecDeque::new(),
        }
    }

    /// Remembers a peer discovery found, or refreshes what is known about it. `in_use` says
    /// whether a peer has a session or a dial in progress: such a peer is never evicted.
    pub(super) fn learn(&mut self, candidate: Candidate, in_use: impl Fn(&PeerId) -> bool) {
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
                .filter(|(id, _)| !in_use(id))
                // A proven peer goes only when no unproven one is left.
                .min_by_key(|(_, known)| (known.proven, known.last_seen))
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
                attempts: 0,
                proven: false,
                last_seen: now,
            },
        );
    }

    /// Picks up to `wanted` peers to dial now, fewer if [`MAX_DIALS_PER_MINUTE`] is used up,
    /// and counts them as dialed: none of them is due again before [`FULL_PEER_RETRY`].
    /// `in_use` says whether a peer has a session or a dial in progress.
    pub(super) fn take_due(
        &mut self,
        wanted: usize,
        in_use: impl Fn(&PeerId) -> bool,
    ) -> Vec<Candidate> {
        let now = Instant::now();
        while self
            .recent_dials
            .front()
            .is_some_and(|started| now.duration_since(*started) >= DIAL_WINDOW)
        {
            self.recent_dials.pop_front();
        }
        let wanted = wanted.min(MAX_DIALS_PER_MINUTE.saturating_sub(self.recent_dials.len()));
        if wanted == 0 {
            return Vec::new();
        }
        let mut due: Vec<(bool, u32, u32, Instant, PeerId)> = self
            .known
            .iter()
            .filter(|(id, known)| known.next_dial <= now && !in_use(id) && !self.is_banned(id))
            .map(|(id, known)| {
                let key = (!known.proven, known.failures, known.attempts);
                (key.0, key.1, key.2, known.next_dial, *id)
            })
            .collect();
        // Half of the dials go to proven peers first, so that discovery handing out fresh ids
        // without end (each with no failure yet) cannot take every dial. Within each half:
        // peers that failed least first, then those dialed least often, then those waiting
        // longest. Most peers are full, so a slot is found by asking many different peers
        // rather than the same few again: a peer not yet dialed goes before one that said
        // "too many peers" a minute ago.
        due.sort_unstable();
        let reserved = wanted.div_ceil(2);
        let proven = due.iter().take_while(|(unproven, ..)| !unproven).count();
        let from_proven = proven.min(reserved);
        let picked = due
            .iter()
            .take(from_proven)
            .chain(due.iter().skip(proven))
            .chain(due.iter().take(proven).skip(from_proven))
            .take(wanted);
        let mut candidates = Vec::with_capacity(wanted);
        for (.., peer) in picked {
            let Some(known) = self.known.get_mut(peer) else {
                continue;
            };
            // Set before the dial ends, so no path can dial a peer twice within the floor.
            known.next_dial = now + jittered(FULL_PEER_RETRY);
            known.attempts = known.attempts.saturating_add(1);
            self.recent_dials.push_back(now);
            candidates.push(known.candidate.clone());
        }
        candidates
    }

    /// Records a failed dial and when the peer may be dialed again.
    pub(super) fn dial_failed(&mut self, peer: PeerId, err: &SessionError) {
        // The outcome, the wait after a first failure, and whether failures in a row double it.
        let (outcome, base, grows) = match err {
            // A full peer: its slots churn, so ask again soon, every time.
            SessionError::Hello(Some(DisconnectReason::TooManyPeers))
            | SessionError::Status {
                reason: Some(DisconnectReason::TooManyPeers),
            } => (DialOutcome::TooManyPeers, FULL_PEER_RETRY, false),
            // How a full reth node refuses: the same, but back off if it keeps happening.
            SessionError::Ecies => (DialOutcome::HandshakeDropped, FULL_PEER_RETRY, true),
            SessionError::Tcp(_) => (DialOutcome::Unreachable, REDIAL_INTERVAL, true),
            SessionError::Timeout { .. } => (DialOutcome::Timeout, REDIAL_INTERVAL, true),
            SessionError::Hello(_) | SessionError::Status { .. } => {
                (DialOutcome::Failed, REDIAL_INTERVAL, true)
            }
            SessionError::ForkMismatch { .. } | SessionError::WrongChain => {
                (DialOutcome::WrongFork, LONG_BACKOFF, false)
            }
            SessionError::NoSharedEth => (DialOutcome::Incompatible, LONG_BACKOFF, false),
        };
        metrics::dial(self.network, outcome);
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

    /// A session with `peer` opened: it is proven, and its failures are forgotten.
    pub(super) fn connected(&mut self, peer: &PeerId) {
        if let Some(known) = self.known.get_mut(peer) {
            known.failures = 0;
            known.attempts = 0;
            known.proven = true;
        }
    }

    /// Does not dial `peer` for at least `wait` from now; never shortens a wait already set.
    pub(super) fn wait(&mut self, peer: &PeerId, wait: Duration) {
        if let Some(known) = self.known.get_mut(peer) {
            known.next_dial = known.next_dial.max(Instant::now() + jittered(wait));
        }
    }

    /// Remembers not to dial or accept `peer` for [`BAN_DURATION`].
    pub(super) fn ban(&mut self, peer: PeerId) {
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
        self.banned.insert(peer, Instant::now() + BAN_DURATION);
    }

    /// Whether `peer` is banned now.
    pub(super) fn is_banned(&self, peer: &PeerId) -> bool {
        let now = Instant::now();
        self.banned.get(peer).is_some_and(|until| *until > now)
    }
}

/// `wait` lengthened by up to half, so peers that failed together are not dialed together and
/// no wait is shorter than asked.
fn jittered(wait: Duration) -> Duration {
    wait.saturating_add((wait / 2).mul_f64(fastrand::f64()))
}
