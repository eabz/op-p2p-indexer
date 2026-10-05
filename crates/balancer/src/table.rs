//! The table of servers registered now (`docs/serving.md` section 6.1), from their heartbeats,
//! and the [`Picker`] that hands them out by load.
//!
//! An entry lives as long as its server's `Register` call: the call's task adds it, updates it
//! on every heartbeat and removes it when the call ends, which is also when heartbeats stop
//! (`service`). So every server in the table was heard from in the last 15 s. Kept in memory
//! only: after a restart, servers register again. The table holds no chunk ranges: every
//! server reads the same bucket, so each serves every sealed chunk.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use alloy_primitives::BlockNumber;
use op_indexer_stream::ticket::Cap;

use crate::register::{PeerReport, ServedReport, SlotReport};

/// Picks so far, so ties between equally loaded servers go round-robin across requests.
static TURN: AtomicUsize = AtomicUsize::new(0);

/// What the table knows of a server, from its last heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Server {
    /// `host:port` of its stream and Flight services.
    pub(crate) address: String,
    /// `false` while it cannot serve: it is then given no work.
    pub(crate) healthy: bool,
    pub(crate) unsafe_head: Option<BlockNumber>,
    pub(crate) safe_head: Option<BlockNumber>,
    pub(crate) finalized_head: Option<BlockNumber>,
    /// How far it holds every block: `Heartbeat.contiguous_through` in the proto.
    pub(crate) contiguous_through: Option<BlockNumber>,
    pub(crate) requests_in_flight: u32,
    pub(crate) bytes_per_second: u64,
    pub(crate) peers: PeerReport,
    pub(crate) slots: SlotReport,
    /// What it served the networks in the last minute.
    pub(crate) served: ServedReport,
    /// Since when it has reported no consensus peer or no execution session, by the
    /// heartbeats; `None` while it has both. Kept by the [`Table`].
    pub(crate) peerless_since: Option<Instant>,
}

/// The stream limit a request takes a place in, on the server that serves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Slot {
    /// A Flight `DoGet`.
    Flight,
    /// A gRPC subscription.
    Subscription,
}

impl Server {
    /// Whether it has no free place of `slot` once `assigned` more jobs take theirs; `false`
    /// if it does not report its places (it is taken to have room).
    fn is_full(&self, slot: Slot, assigned: u64) -> bool {
        let slots = self.slots;
        let (max, in_use) = match slot {
            Slot::Flight => (slots.max_flights, slots.flights_in_use),
            Slot::Subscription => (slots.max_subscriptions, slots.subscriptions_in_use),
        };
        max.zip(in_use)
            .is_some_and(|(max, in_use)| u64::from(max.saturating_sub(in_use)) <= assigned)
    }

    /// Whether it reports no consensus peer or no execution session: gossip, or receipts and
    /// gap fill, cannot go on.
    const fn is_peerless(&self) -> bool {
        matches!(self.peers.consensus_peers, Some(0))
            || matches!(self.peers.execution_sessions, Some(0))
    }

    /// The last block it serves under `cap`: its head under the cap, but no further than it
    /// holds every block (a server without range sync has a gap above the sealed chunks).
    pub(crate) fn reach(&self, cap: Cap) -> Option<BlockNumber> {
        let head = match cap {
            Cap::Finalized => self.finalized_head,
            Cap::Safe => self.safe_head,
            Cap::Any => self.unsafe_head,
        };
        head.zip(self.contiguous_through)
            .map(|(head, through)| head.min(through))
    }
}

/// The table, cheap to clone: clones share it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Table {
    /// Server id to its registration (the call that holds it) and state.
    servers: Arc<Mutex<HashMap<String, (u64, Server)>>>,
}

impl Table {
    /// Adds `server` for registration `call`, replacing an older registration of its id.
    pub(crate) fn insert(&self, id: &str, call: u64, mut server: Server) {
        server.peerless_since = server.is_peerless().then(Instant::now);
        self.lock().insert(id.to_owned(), (call, server));
    }

    /// Records a heartbeat of registration `call`. `false` if a newer registration of the
    /// id replaced it, which then ends.
    pub(crate) fn update(&self, id: &str, call: u64, mut server: Server) -> bool {
        match self.lock().get_mut(id) {
            Some(entry) if entry.0 == call => {
                server.peerless_since = server
                    .is_peerless()
                    .then(|| entry.1.peerless_since.unwrap_or_else(Instant::now));
                entry.1 = server;
                true
            }
            Some(_) | None => false,
        }
    }

    /// Removes `id`, unless a newer registration than `call` holds it.
    pub(crate) fn remove(&self, id: &str, call: u64) {
        let mut servers = self.lock();
        if servers.get(id).is_some_and(|(holder, _)| *holder == call) {
            servers.remove(id);
        }
    }

    /// Every registered server with its id, healthy or not, ordered by id.
    pub(crate) fn snapshot(&self) -> Vec<(String, Server)> {
        let mut servers: Vec<_> = self
            .lock()
            .iter()
            .map(|(id, (_, server))| (id.clone(), server.clone()))
            .collect();
        servers.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        servers
    }

    /// A picker over the healthy servers now.
    pub(crate) fn picker(&self) -> Picker {
        let mut servers: Vec<Server> = self
            .lock()
            .values()
            .filter(|(_, server)| server.healthy)
            .map(|(_, server)| server.clone())
            .collect();
        // One order for the round-robin, whatever the map's.
        servers.sort_unstable_by(|a, b| a.address.cmp(&b.address));
        Picker {
            assigned: vec![0; servers.len()],
            servers,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (u64, Server)>> {
        // A panic while holding the lock leaves a whole map: every write is one statement.
        self.servers.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Hands out the healthy servers of one moment, least loaded first, for one request.
///
/// The load is a server's requests in flight plus the jobs this picker already gave it, then
/// its bytes per second: so the jobs of one large range spread over the servers, while a
/// server busy with other requests gets fewer of them. Ties go round-robin. A server whose
/// free places (of the kind the request takes, less the jobs this picker gave it) are used up
/// comes after every server with room, since it would refuse: first only when all are full,
/// and otherwise a place to try next.
#[derive(Debug)]
pub(crate) struct Picker {
    servers: Vec<Server>,
    /// Jobs given to each server so far.
    assigned: Vec<u64>,
}

impl Picker {
    /// The healthy servers.
    pub(crate) fn servers(&self) -> &[Server] {
        &self.servers
    }

    /// Up to `count` of the servers `covers` accepts, those with a free place of `slot` first,
    /// then least loaded first; the first is charged one job. Empty
    /// if `covers` accepts none.
    pub(crate) fn pick(
        &mut self,
        count: usize,
        slot: Slot,
        covers: impl Fn(&Server) -> bool,
    ) -> Vec<String> {
        let total = self.servers.len();
        let turn = TURN.fetch_add(1, Ordering::Relaxed);
        let mut ranked: Vec<_> = self
            .servers
            .iter()
            .zip(&self.assigned)
            .enumerate()
            .filter(|(_, (server, _))| covers(server))
            .map(|(index, (server, assigned))| {
                let load = u64::from(server.requests_in_flight).saturating_add(*assigned);
                // Equals in order from `turn` on, wrapping: the round-robin.
                let place = (index + total - turn % total) % total;
                // A full server would refuse: it comes after every one with room.
                let full = server.is_full(slot, *assigned);
                ((full, load, server.bytes_per_second, place), index)
            })
            .collect();
        ranked.sort_unstable();
        ranked.truncate(count);
        if let Some(assigned) = ranked
            .first()
            .and_then(|(_, first)| self.assigned.get_mut(*first))
        {
            *assigned = assigned.saturating_add(1);
        }
        ranked
            .into_iter()
            .filter_map(|(_, index)| self.servers.get(index))
            .map(|server| server.address.clone())
            .collect()
    }
}
