//! What the network knows about beacon nodes: the peers in use, the dials in progress, the
//! nodes to dial next, the peers worth dialing again and those not worth dialing at all.
//!
//! Plain bookkeeping, every table bounded. Does not touch the swarm: `network` dials,
//! disconnects and sends, and records here what happened.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use libp2p::{Multiaddr, PeerId};
use tokio::time::Instant;

use super::behaviour::Asked;
use crate::beacon::discovery::Candidate;

/// Peers serving light-client data to stay connected to.
const TARGET_PEERS: usize = 6;
/// Peers kept at most, counting those that dialed us.
const MAX_PEERS: usize = 12;
/// Dials in progress at once.
const MAX_DIALS: usize = 4;
/// Limit for a dial, the handshake and the peer's identify answer.
const DIAL_TIMEOUT: Duration = Duration::from_secs(20);
/// Requests in a row a peer may leave unanswered or answer without data before it is dropped.
const MAX_FAILURES: u32 = 3;
/// Peers remembered as not worth dialing; the set is emptied when it is full.
const MAX_AVOIDED: usize = 4096;
/// Candidates waiting to be dialed; discovery finds more when these are used up.
const MAX_CANDIDATES: usize = 512;
/// Peers remembered as serving light-client data, to dial again after they hang up.
const MAX_KNOWN: usize = 64;
/// How long a peer that hung up is left alone. Peers with no room say so and close; their
/// room changes over minutes. Doubled after each dial that does not make it a peer again.
const REDIAL_AFTER: Duration = Duration::from_secs(120);
/// Dials in a row that may fail to make a known peer a peer again before it is forgotten.
const MAX_REDIALS: u32 = 5;

/// A connected peer that lists light-client protocols.
#[derive(Debug)]
struct Peer {
    agent: String,
    /// Where it was dialed; `None` if it dialed us.
    addr: Option<Multiaddr>,
    /// Requests in a row it left unanswered or answered without data.
    failures: u32,
    /// What it is not asked: the protocols it does not list or refused, and the bootstrap
    /// once it answered one without data.
    lacks: HashSet<Asked>,
    /// When it was asked last, as a count of requests; 0 if never.
    asked_at: u64,
}

/// A peer that hung up after being usable.
#[derive(Debug)]
struct Known {
    addr: Multiaddr,
    /// When it may be dialed again.
    due: Instant,
    /// Dials since it was last a peer.
    dials: u32,
}

/// The network's tables of beacon nodes.
#[derive(Debug, Default)]
pub(super) struct Peers {
    connected: HashMap<PeerId, Peer>,
    /// Dials in progress: when each began, and the address.
    dialing: HashMap<PeerId, (Instant, Multiaddr)>,
    candidates: VecDeque<Candidate>,
    /// Peers that were not dropped but hung up: discovery reports a node once, so without
    /// them the supply of peers runs dry.
    known: HashMap<PeerId, Known>,
    avoided: HashSet<PeerId>,
    /// Requests sent so far.
    asked: u64,
}

impl Peers {
    /// How many peers are connected.
    pub(super) fn len(&self) -> usize {
        self.connected.len()
    }

    pub(super) fn is_connected(&self, peer: &PeerId) -> bool {
        self.connected.contains_key(peer)
    }

    /// Whether there is no room for another peer.
    pub(super) fn is_full(&self) -> bool {
        self.connected.len() >= MAX_PEERS
    }

    pub(super) fn is_avoided(&self, peer: &PeerId) -> bool {
        self.avoided.contains(peer)
    }

    /// The client and version a connected peer names itself.
    pub(super) fn agent(&self, peer: &PeerId) -> Option<&str> {
        self.connected.get(peer).map(|state| state.agent.as_str())
    }

    /// Queues a node discovery found; dropped when the queue is full.
    pub(super) fn found(&mut self, candidate: Candidate) {
        if self.candidates.len() < MAX_CANDIDATES {
            self.candidates.push_back(candidate);
        }
    }

    /// Gives up the dials that take too long and returns their peers, to disconnect: they
    /// connected without saying what they serve, or not at all.
    pub(super) fn expired_dials(&mut self) -> Vec<PeerId> {
        let expired = self
            .dialing
            .extract_if(|_, (since, _)| since.elapsed() >= DIAL_TIMEOUT);
        expired.map(|(peer, _)| peer).collect()
    }

    /// The next node to dial, while peers are wanted: a known peer whose time has come, else
    /// one discovery found. The caller dials it and calls [`Self::dialed`].
    ///
    /// A known peer's next time is set here, later after each dial, so a dial that fails
    /// needs no bookkeeping; after [`MAX_REDIALS`] in a row it is forgotten.
    pub(super) fn next_dial(&mut self) -> Option<Candidate> {
        loop {
            if self.dialing.len() >= MAX_DIALS
                || self.connected.len().saturating_add(self.dialing.len()) >= TARGET_PEERS
            {
                return None;
            }
            let candidate = self.next_candidate()?;
            let peer = &candidate.peer;
            if !self.avoided.contains(peer)
                && !self.connected.contains_key(peer)
                && !self.dialing.contains_key(peer)
            {
                return Some(candidate);
            }
        }
    }

    fn next_candidate(&mut self) -> Option<Candidate> {
        let now = Instant::now();
        let due = self
            .known
            .iter_mut()
            .filter(|(peer, known)| known.due <= now && !self.connected.contains_key(*peer))
            .min_by_key(|(_, known)| known.due);
        let Some((peer, known)) = due else {
            return self.candidates.pop_front();
        };
        let candidate = Candidate {
            peer: *peer,
            addr: known.addr.clone(),
        };
        known.dials = known.dials.saturating_add(1);
        if known.dials > MAX_REDIALS {
            self.known.remove(&candidate.peer);
        } else {
            known.due = now + REDIAL_AFTER.saturating_mul(1 << known.dials);
        }
        Some(candidate)
    }

    /// Records a dial that began.
    pub(super) fn dialed(&mut self, candidate: Candidate) {
        self.dialing
            .insert(candidate.peer, (Instant::now(), candidate.addr));
    }

    /// Records that the dial of `peer`, if there was one, is over: it failed, or the peer
    /// identified itself or hung up. Returns the address dialed.
    pub(super) fn dial_ended(&mut self, peer: &PeerId) -> Option<Multiaddr> {
        self.dialing.remove(peer).map(|(_, addr)| addr)
    }

    /// Records a new peer; `addr` is where it was dialed, if we dialed; `lacks` what it is
    /// not to be asked.
    pub(super) fn connected(
        &mut self,
        peer: PeerId,
        agent: String,
        addr: Option<Multiaddr>,
        lacks: HashSet<Asked>,
    ) {
        let state = Peer {
            agent,
            addr,
            failures: 0,
            lacks,
            asked_at: 0,
        };
        self.connected.insert(peer, state);
    }

    /// Picks the peer to send `asked` to: of those that do not lack it, the one that failed
    /// least and, among those, was asked longest ago.
    pub(super) fn pick(&mut self, asked: Asked) -> Option<PeerId> {
        let (peer, state) = self
            .connected
            .iter_mut()
            .filter(|(_, state)| !state.lacks.contains(&asked))
            .min_by_key(|(_, state)| (state.failures, state.asked_at))?;
        self.asked = self.asked.saturating_add(1);
        state.asked_at = self.asked;
        Some(*peer)
    }

    /// A peer to close so that another node is dialed: one that lacks `asked`, when every
    /// place is taken.
    pub(super) fn in_the_way(&self, asked: Asked) -> Option<PeerId> {
        if self.connected.len() < TARGET_PEERS {
            return None;
        }
        let mut lacking = self
            .connected
            .iter()
            .filter(|(_, state)| state.lacks.contains(&asked));
        lacking.next().map(|(peer, _)| *peer)
    }

    /// Forgets a peer that hung up, keeping its address to dial it again later. Returns
    /// whether it was a peer.
    pub(super) fn lost(&mut self, peer: PeerId) -> bool {
        let Some(state) = self.connected.remove(&peer) else {
            return false;
        };
        if let Some(addr) = state.addr
            && (self.known.len() < MAX_KNOWN || self.known.contains_key(&peer))
        {
            let due = Instant::now() + REDIAL_AFTER;
            let known = Known {
                addr,
                due,
                dials: 0,
            };
            self.known.insert(peer, known);
        }
        true
    }

    /// Records an answer with data.
    pub(super) fn answered(&mut self, peer: &PeerId) {
        if let Some(state) = self.connected.get_mut(peer) {
            state.failures = 0;
        }
    }

    /// Counts a request the peer did not answer with data. Returns whether it failed
    /// [`MAX_FAILURES`] times in a row and is to be dropped.
    pub(super) fn failed(&mut self, peer: &PeerId) -> bool {
        let Some(state) = self.connected.get_mut(peer) else {
            return false;
        };
        state.failures = state.failures.saturating_add(1);
        state.failures >= MAX_FAILURES
    }

    /// Records that a peer does not serve `asked`, which it is not asked again.
    pub(super) fn lacks(&mut self, peer: &PeerId, asked: Asked) {
        if let Some(state) = self.connected.get_mut(peer) {
            state.lacks.insert(asked);
        }
    }

    /// Forgets a peer and remembers not to dial it again.
    pub(super) fn avoid(&mut self, peer: PeerId) {
        if self.avoided.len() >= MAX_AVOIDED {
            self.avoided.clear();
        }
        self.avoided.insert(peer);
        self.connected.remove(&peer);
        self.known.remove(&peer);
    }
}
