//! What every session of this node shares: its key, its chain, the head it follows, and the
//! "this build looks behind" warning.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use alloy_eip2124::{ForkFilter, ForkId};
use alloy_primitives::{BlockNumber, Bytes};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::BlockRef;
use secp256k1::SecretKey;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::serve::{Serving, SessionServing};

/// Shortest time between two "this build looks behind" warnings.
const BEHIND_WARN_INTERVAL: Duration = Duration::from_mins(10);

/// What every session of this node shares: its key, its chain and the head it follows.
#[derive(Debug)]
pub(crate) struct SessionContext {
    key: SecretKey,
    chain: &'static ChainSpec,
    listen_port: u16,
    /// The way to the server that answers peers' requests, and the range it holds.
    serving: Serving,
    /// The newest block the node knows, which the binary provides. `None` until it knows one.
    tip: watch::Receiver<Option<BlockRef>>,
    /// When the "build looks behind" warning was last logged.
    behind_warned: Mutex<Option<Instant>>,
}

impl SessionContext {
    pub(crate) fn new(
        key: SecretKey,
        chain: &'static ChainSpec,
        listen_port: u16,
        serving: Serving,
        tip: watch::Receiver<Option<BlockRef>>,
    ) -> Self {
        Self {
            key,
            chain,
            listen_port,
            serving,
            tip,
            behind_warned: Mutex::new(None),
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

    pub(crate) const fn chain(&self) -> &'static ChainSpec {
        self.chain
    }

    /// The port sessions and discovery listen on, advertised in the hello.
    pub(super) const fn listen_port(&self) -> u16 {
        self.listen_port
    }

    /// The serving side of one new session and the channel its answers arrive on. It follows
    /// the tip, which is the end of the range the session advertises.
    pub(super) fn session_serving(&self) -> (SessionServing, mpsc::Receiver<Bytes>) {
        self.serving.session(self.tip.clone())
    }

    /// The fork filter now: yields our fork id and validates a peer's.
    ///
    /// By wall-clock time, with every fork that activates at a block number taken as passed.
    /// The node may know no head yet, and a fork id computed at block 0 names the first block
    /// fork as "next": a peer checking it against its own head would reject us as stale. On an
    /// OP Stack chain the block forks are years old; what changes the fork id while the node
    /// runs is a time fork activating.
    pub(crate) fn fork_filter(&self) -> ForkFilter {
        self.chain.fork_filter(BlockNumber::MAX, unix_now())
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
