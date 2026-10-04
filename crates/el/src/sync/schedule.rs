//! Which peers the range sync may ask now, and how long a peer is left alone after it failed.
//!
//! One job at a time per session and the usual pause between two (`pacing`), plus a longer
//! rest after a failure. Does not choose what a peer fetches (the syncer does) and does not
//! drop peers: it says what to report to the peer set.

use std::collections::HashMap;
use std::time::Duration;

use reth_network_peers::PeerId;
use tokio::time::Instant;

use super::Failure;
use crate::pacing::Pacing;
use crate::peers::Report;
use crate::session::SessionHandle;

/// How long a peer is left alone after it did not hold what was asked.
const NOT_HELD_REST: Duration = Duration::from_mins(1);
/// How long a peer is left alone after a timeout.
const TIMEOUT_REST: Duration = Duration::from_secs(10);

/// What the range sync knows about the peers of the open sessions.
#[derive(Debug, Default)]
pub(super) struct Schedule {
    peers: HashMap<PeerId, PeerState>,
}

#[derive(Debug)]
struct PeerState {
    pacing: Pacing,
    /// Not asked before this, after a failure.
    rest_until: Instant,
}

impl PeerState {
    /// When the peer may be given a job; `None` while it works on one.
    fn free_at(&self) -> Option<Instant> {
        Some(self.pacing.ready_at()?.max(self.rest_until))
    }
}

impl Schedule {
    /// Whether `peer` may be given a job now.
    pub(super) fn is_free(&mut self, peer: PeerId, now: Instant) -> bool {
        let state = self.peers.entry(peer).or_insert(PeerState {
            pacing: Pacing::new(now),
            rest_until: now,
        });
        state.free_at().is_some_and(|at| at <= now)
    }

    /// Marks `peer` as working on a job.
    pub(super) fn started(&mut self, peer: PeerId) {
        if let Some(state) = self.peers.get_mut(&peer) {
            state.pacing.started();
        }
    }

    /// Frees `peer` after a job, decides how long it rests, and returns what the peer set
    /// should hear about it, if anything.
    pub(super) fn finished(&mut self, peer: PeerId, failure: Option<&Failure>) -> Option<Report> {
        let state = self.peers.get_mut(&peer)?;
        let unresponsive = state
            .pacing
            .finished(!matches!(failure, Some(Failure::Timeout)));
        let (rest, report) = match failure {
            None | Some(Failure::Closed | Failure::Panicked(_) | Failure::Unsupported(_)) => {
                (Duration::ZERO, None)
            }
            Some(Failure::NotHeld) => (NOT_HELD_REST, None),
            // The peer set drops these peers; the rest only covers the time until it has.
            Some(Failure::Invalid(_) | Failure::Malformed(_)) => {
                (NOT_HELD_REST, Some(Report::BadData(peer)))
            }
            Some(Failure::Undecodable(_)) => (NOT_HELD_REST, Some(Report::Undecodable(peer))),
            Some(Failure::Timeout) => (TIMEOUT_REST, None),
        };
        state.rest_until = Instant::now() + rest;
        if unresponsive {
            return Some(Report::Unresponsive(peer));
        }
        report
    }

    /// Forgets the peers whose session is gone.
    pub(super) fn retain(&mut self, sessions: &[SessionHandle]) {
        self.peers.retain(|peer, _| {
            sessions
                .iter()
                .any(|session| session.status().peer_id == *peer)
        });
    }

    /// When the first peer that is pausing or resting may be asked again; `None` if none is.
    pub(super) fn next_wake(&self, now: Instant) -> Option<Instant> {
        self.peers
            .values()
            .filter_map(PeerState::free_at)
            .filter(|at| *at > now)
            .min()
    }

    /// Whether every known peer could be given a job right now.
    pub(super) fn all_free(&self, now: Instant) -> bool {
        self.peers
            .values()
            .all(|state| state.free_at().is_some_and(|at| at <= now))
    }

    /// Peers of open sessions.
    pub(super) fn len(&self) -> usize {
        self.peers.len()
    }
}
