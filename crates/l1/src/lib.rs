//! L1 over p2p: which dispute games Ethereum carries for the chain, read from Ethereum's
//! execution peers and verified against L1 block hashes that are trusted.
//!
//! ```text
//! beacon light client ─▶ TrustedL1Block (number, hash, finalized)
//!     ─▶ watcher: headers by hash ─▶ logs bloom ─▶ receipts + transactions (against the
//!        header's roots) ─▶ DisputeGameCreated + create call ─▶ VerifiedGame ─▶ L1Games
//! ```
//!
//! - [`L1Network`] is the component the binary builds and runs; [`L1Config`] its plain-data
//!   configuration, [`TrustedL1Block`] its input and [`L1Error`] what stops it.
//! - `spec` describes Ethereum mainnet as a devp2p network; discovery, sessions and the peer
//!   set are the `el` crate's, used for a second network with its own key and port.
//! - `fetch` reads headers, transactions and receipts from the open sessions and verifies
//!   them; `watch` walks the chain down from trusted hashes and finds the games.
//!
//! The trust is all in the input: an L1 block hash is taken as given (the beacon light
//! client verifies it), and everything read from an execution peer must hash to it or to a
//! root inside a header that does. What a game claims is not checked here: the consumer
//! compares it with our own block. Uses no RPC. See `docs/l1.md`.

mod beacon;
mod fetch;
mod spec;
mod watch;

use std::net::SocketAddr;

use alloy_primitives::{B256, BlockNumber};
use op_indexer_chainspec::ChainSpec;
use op_indexer_el::{ElError, PeerConfig, PeerNetwork};
use op_indexer_primitives::{ExecutionPeer, L1Games};
use tokio::sync::{mpsc, watch as watch_channel};
use tokio_util::sync::CancellationToken;

pub use crate::beacon::{BeaconConfig, BeaconError, LightClient};
use crate::fetch::Fetcher;
use crate::watch::Watcher;

/// An L1 execution block the caller vouches for: the beacon light client verified a beacon
/// header that carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrustedL1Block {
    /// The execution block's number.
    pub number: BlockNumber,
    /// The execution block's hash.
    pub hash: B256,
    /// Whether the block is finalized. Otherwise it is the head as attested, which can still
    /// be replaced.
    pub finalized: bool,
}

/// Configuration of the L1 side.
#[derive(Debug, Clone)]
pub struct L1Config {
    /// The OP Stack chain whose games are watched: its factory, chain id, Bedrock block and
    /// block time come from it. The L1 is Ethereum mainnet: a chain that settles elsewhere
    /// is not supported.
    pub chain: &'static ChainSpec,
    /// Listen address of the L1 node, used for both discovery (UDP) and sessions (TCP). It
    /// must differ from the execution network's.
    pub listen_addr: SocketAddr,
    /// The address the L1 node's record advertises, for a node reachable at a known public
    /// address. `None` lets discovery learn it.
    pub advertised_addr: Option<SocketAddr>,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs. Empty means the chain's:
    /// Ethereum's and the OP Stack's execution nodes share one discovery network.
    pub bootnodes: Vec<String>,
    /// L1 peers that served us in an earlier run: dialed first.
    pub saved_peers: Vec<ExecutionPeer>,
}

/// What stops the L1 side.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum L1Error {
    /// The devp2p side failed: a socket could not be bound, or one of its tasks failed.
    #[error("L1 execution network failed")]
    Network(#[from] ElError),
    /// The watcher panicked.
    #[error("L1 watcher task failed")]
    Watcher(#[source] tokio::task::JoinError),
}

/// The L1 side: sessions with Ethereum execution peers and the watcher that reads the
/// chain's dispute games from them.
#[derive(Debug)]
pub struct L1Network {
    network: PeerNetwork,
    watcher: Watcher,
}

impl L1Network {
    /// Creates the L1 side. Does no I/O.
    ///
    /// `node_key` is the node's secp256k1 secret on Ethereum's network: not the key of the
    /// consensus or the OP execution network, each of which runs its own discovery node.
    /// `trusted` delivers L1 blocks the beacon light client vouches for, in any order and not
    /// necessarily consecutive; nothing is dialed before the first one, because it is the head
    /// the sessions advertise. The games found are published on `games`. A peer worth saving
    /// for the next start is reported on `served`, without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Network`] if `node_key` is not a valid secp256k1 secret.
    pub fn new(
        config: L1Config,
        node_key: B256,
        trusted: mpsc::Receiver<TrustedL1Block>,
        games: watch_channel::Sender<L1Games>,
        served: mpsc::Sender<ExecutionPeer>,
    ) -> Result<Self, L1Error> {
        let bootnodes = if config.bootnodes.is_empty() {
            let chain = config.chain.bootnodes.iter();
            chain.map(|node| (*node).to_owned()).collect()
        } else {
            config.bootnodes
        };
        let peer_config = PeerConfig {
            listen_addr: config.listen_addr,
            advertised_addr: config.advertised_addr,
            saved_peers: config.saved_peers,
        };
        let (head, head_rx) = watch_channel::channel(None);
        let (network, peers) = PeerNetwork::new(
            spec::mainnet(bootnodes),
            peer_config,
            node_key,
            head_rx,
            served,
        )?;
        let watcher = Watcher::new(config.chain, Fetcher::new(peers), trusted, head, games);
        Ok(Self { network, watcher })
    }

    /// Runs the L1 side until `cancel` fires or the source of trusted blocks ends.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error`] if the network cannot bind its sockets or one of its tasks fails,
    /// or the watcher panics.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), L1Error> {
        let Self { network, watcher } = self;
        // Either part ending stops the other.
        let stop = cancel.child_token();
        let watching = tokio::spawn(watcher.run(stop.clone()));
        let network = network.run(stop.clone()).await;
        stop.cancel();
        watching.await.map_err(L1Error::Watcher)?;
        Ok(network?)
    }
}
