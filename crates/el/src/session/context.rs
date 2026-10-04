//! What every session of this node shares: its key, its chain, the tip it advertises, and
//! the "this build looks behind" warning.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use alloy_eip2124::{ForkFilter, ForkId};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::BlockRef;
use secp256k1::SecretKey;
use tracing::warn;

/// Shortest time between two "this build looks behind" warnings.
const BEHIND_WARN_INTERVAL: Duration = Duration::from_mins(10);

/// What every session of this node shares: its key, its chain and the tip it advertises.
#[derive(Debug)]
pub(crate) struct SessionContext {
    key: SecretKey,
    chain: &'static ChainSpec,
    listen_port: u16,
    /// The newest block the node knows, advertised in the eth status. `None` until one is set.
    tip: Mutex<Option<BlockRef>>,
    /// When the "build looks behind" warning was last logged.
    behind_warned: Mutex<Option<Instant>>,
}

impl SessionContext {
    pub(crate) const fn new(key: SecretKey, chain: &'static ChainSpec, listen_port: u16) -> Self {
        Self {
            key,
            chain,
            listen_port,
            tip: Mutex::new(None),
            behind_warned: Mutex::new(None),
        }
    }

    /// Records `block` as the tip to advertise, if it is newer than the current one.
    pub(crate) fn set_tip(&self, block: BlockRef) {
        let mut tip = self.tip.lock().unwrap_or_else(PoisonError::into_inner);
        if tip.is_none_or(|current| block.number > current.number) {
            *tip = Some(block);
        }
    }

    /// Whether a tip has been set. Sessions are only opened once it has: peers end a session
    /// whose status advertises genesis as the head (seen live from op-reth, reth and op-geth).
    pub(crate) fn has_tip(&self) -> bool {
        self.tip().is_some()
    }

    pub(super) fn tip(&self) -> Option<BlockRef> {
        *self.tip.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The node's secp256k1 key, shared by discovery and sessions so peers can dial what they
    /// discover.
    pub(crate) const fn key(&self) -> &SecretKey {
        &self.key
    }

    pub(crate) const fn chain(&self) -> &'static ChainSpec {
        self.chain
    }

    /// The port sessions and discovery listen on, advertised in the hello.
    pub(super) const fn listen_port(&self) -> u16 {
        self.listen_port
    }

    /// The fork filter at our current head: yields our fork id and validates a peer's.
    pub(crate) fn fork_filter(&self) -> ForkFilter {
        let number = self.tip().map_or(0, |tip| tip.number);
        self.chain.fork_filter(number, unix_now())
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
