//! What every session of this node shares: its key, its chain, the head it follows, the peers
//! known to be op-p2p-indexers, and the "this build looks behind" warning.

use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use alloy_eip2124::{ForkFilter, ForkId};
use alloy_primitives::{BlockNumber, Bytes};
use op_indexer_primitives::BlockRef;
use reth_eth_wire_types::EthVersion;
use reth_network_peers::PeerId;
use secp256k1::SecretKey;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::network::NetworkSpec;
use crate::serve::{Serving, SessionServing};

/// Shortest time between two "this build looks behind" warnings.
const BEHIND_WARN_INTERVAL: Duration = Duration::from_mins(10);
/// Most peers remembered as indexers; the set starts over when full. Indexers are few: the
/// cap only bounds what a flood of node records can make us hold.
const MAX_INDEXERS: usize = 4096;

/// What every session of this node shares: its key, its chain and the head it follows.
#[derive(Debug)]
pub(crate) struct SessionContext {
    key: SecretKey,
    spec: NetworkSpec,
    listen_port: u16,
    /// The way to the server that answers peers' requests, and the range it holds.
    serving: Serving,
    /// The newest block the node knows, which the binary provides. `None` until it knows one.
    tip: watch::Receiver<Option<BlockRef>>,
    /// When the "build looks behind" warning was last logged.
    behind_warned: Mutex<Option<Instant>>,
    /// Peers whose node record says they are op-p2p-indexers, from discovery and the saved
    /// peers. At most [`MAX_INDEXERS`].
    indexers: Mutex<HashSet<PeerId>>,
}

impl SessionContext {
    pub(crate) fn new(
        key: SecretKey,
        spec: NetworkSpec,
        listen_port: u16,
        serving: Serving,
        tip: watch::Receiver<Option<BlockRef>>,
    ) -> Self {
        Self {
            key,
            spec,
            listen_port,
            serving,
            tip,
            behind_warned: Mutex::new(None),
            indexers: Mutex::new(HashSet::new()),
        }
    }

    /// Whether a tip has been set. Sessions are only opened once it has: peers end a session
    /// whose status advertises genesis as the head (seen live from op-reth, reth and op-geth).
    pub(crate) fn has_tip(&self) -> bool {
        self.tip().is_some()
    }

    pub(super) fn tip(&self) -> Option<BlockRef> {
        *self.tip.borrow()
    }

    /// The node's secp256k1 key, shared by discovery and sessions so peers can dial what they
    /// discover.
    pub(crate) const fn key(&self) -> &SecretKey {
        &self.key
    }

    /// The network these sessions are on.
    pub(crate) const fn spec(&self) -> &NetworkSpec {
        &self.spec
    }

    /// The port sessions and discovery listen on, advertised in the hello.
    pub(super) const fn listen_port(&self) -> u16 {
        self.listen_port
    }

    /// The serving side of one new session with a peer that is an indexer or not, and the
    /// channel its answers arrive on. It follows the tip, which is the end of the range the
    /// session advertises.
    pub(super) fn session_serving(
        &self,
        indexer: bool,
        version: EthVersion,
    ) -> (SessionServing, mpsc::Receiver<Bytes>) {
        self.serving
            .session(self.tip.clone(), self.lowest_for(indexer), version)
    }

    /// Whether this node serves blocks on this network: eth/68 is offered only then, for
    /// peers that cannot speak eth/69 to sync from us; we ask only eth/69 peers.
    pub(crate) const fn serves(&self) -> bool {
        self.serving.is_enabled()
    }

    /// The lowest block shared with a peer: every block for an indexer, from the network's
    /// [`NetworkSpec::indexers_only_below`] on for anyone else.
    fn lowest_for(&self, indexer: bool) -> BlockNumber {
        match self.spec.indexers_only_below {
            Some(below) if !indexer => below,
            Some(_) | None => 0,
        }
    }

    /// Remembers that `peer`'s node record says it is an op-p2p-indexer. Nothing on a network
    /// indexers do not share.
    pub(crate) fn mark_indexer(&self, peer: PeerId) {
        if self.spec.indexers_only_below.is_none() {
            return;
        }
        let mut indexers = self.indexers.lock().unwrap_or_else(PoisonError::into_inner);
        if indexers.len() >= MAX_INDEXERS {
            indexers.clear();
        }
        indexers.insert(peer);
    }

    /// Whether `peer` is known to be an op-p2p-indexer.
    pub(crate) fn is_indexer(&self, peer: &PeerId) -> bool {
        self.indexers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(peer)
    }

    /// The fork filter now: yields our fork id and validates a peer's.
    pub(crate) fn fork_filter(&self) -> ForkFilter {
        self.spec.fork_filter()
    }

    /// Warns, at most once per [`BEHIND_WARN_INTERVAL`], that peers are on a fork this build
    /// does not know: the fork activations in `chainspec` need updating.
    pub(crate) fn warn_build_behind(&self, remote: ForkId, seen_in: &'static str) {
        let now = Instant::now();
        let mut warned = self
            .behind_warned
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if warned.is_none_or(|at| now.duration_since(at) >= BEHIND_WARN_INTERVAL) {
            *warned = Some(now);
            warn!(
                remote_fork_hash = ?remote.hash,
                remote_fork_next = remote.next,
                seen_in,
                "execution peers are on a hardfork this build does not know; update the fork \
                 activations, the node will otherwise only reach peers that missed the upgrade"
            );
        }
    }
}

/// Current Unix time in seconds; fork activations are wall-clock.
pub(crate) fn unix_now() -> u64 {
    UNIX_EPOCH.elapsed().map_or(0, |elapsed| elapsed.as_secs())
}
