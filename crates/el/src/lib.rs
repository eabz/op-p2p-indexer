//! Execution p2p: devp2p sessions with OP Stack execution peers, to fetch what gossip does not
//! carry (the receipts of every block, verified against the block's header) and to serve the
//! blocks this node holds to peers that ask.
//!
//! ```text
//! discv5 (fork id filter) ─▶ peer set (dial, keep, redial) ─▶ session (RLPx, eth/69)
//!     ReceiptsRequest ─▶ fetcher ─▶ GetReceipts ─▶ verify against receipts root ─▶ VerifiedReceipts
//! ```
//!
//! - [`ExecutionNetwork`] is the component the binary builds and runs; [`ElConfig`] is its
//!   plain-data configuration and [`ElError`] what stops it.
//! - `discovery` finds peers of our chain and fork; `session` is one connection; `wire` the
//!   messages; `peers`, `fetch` and `verify` keep sessions, schedule requests and check answers.
//! - `serve` answers peers' requests from a [`BlockProvider`], which the binary implements.
//!
//! A second p2p stack next to `op-indexer-p2p` (libp2p); the two never depend on each other.
//! Nothing from an execution peer is trusted. See `docs/el.md`.

mod config;
mod discovery;
mod error;
mod fetch;
mod horizon;
mod network;
mod pacing;
mod peers;
mod serve;
mod session;
mod sync;
mod verify;
mod warn_limit;
mod wire;

use alloy_primitives::B256;
use op_indexer_primitives::{
    BlockRef, EncodedBlock, ExecutionPeer, FillRequest, ReceiptsRequest, VerifiedReceipts,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

pub use config::ElConfig;
pub use discovery::enode_discovery_addr;
pub use error::ElError;
pub use network::{ETH_RECORD_KEY, NetworkSpec, OPEL_RECORD_KEY, PeerConfig, PeerNetwork};
pub use peers::{Peers, Report};
pub use reth_network_peers::PeerId;
pub use serve::BlockProvider;
pub use session::{BlockRange, RequestError, SessionHandle};
pub use sync::{RangeSync, RoundEnd, SyncPlan};

use crate::fetch::Fetcher;
use crate::serve::Server;

/// The execution network of the OP Stack chain: its peers, the receipts fetcher, range sync
/// and the server of the blocks `P` holds.
#[derive(Debug)]
pub struct ExecutionNetwork<P> {
    config: ElConfig,
    network: PeerNetwork,
    peers: Peers,
    block_server: Server<P>,
    requests: mpsc::Receiver<ReceiptsRequest>,
    verified: mpsc::Sender<VerifiedReceipts>,
    /// A range of blocks to fetch from peers; `None` unless one was asked for.
    sync: Option<RangeSync>,
    /// Spans gossip missed, and where their blocks go; `None` unless asked for.
    fills: Option<(mpsc::Receiver<FillRequest>, mpsc::Sender<Vec<EncodedBlock>>)>,
    /// The network's name, on every line its tasks log.
    label: &'static str,
}

impl<P: BlockProvider> ExecutionNetwork<P> {
    /// Creates the network. Does no I/O.
    ///
    /// `node_key` is the node's secp256k1 secret for the execution network. It must not be the
    /// consensus (libp2p) identity: both run a discv5 node, and one key in two of them would
    /// publish conflicting node records. `head` follows the newest block the node knows
    /// (from gossip, or the newest block it holds when there is no gossip): sessions are opened
    /// only once it has a value, because peers end a session whose status advertises genesis.
    /// Blocks to fetch receipts for arrive on `requests`;
    /// verified receipts leave on `verified`. A peer worth saving for the next start (see
    /// [`ElConfig::saved_peers`]) is reported on `served`, without waiting: once per session
    /// we opened, at its first verified answer. Peers' requests for headers, bodies and
    /// receipts are answered from `provider`, the node's archive.
    ///
    /// # Errors
    ///
    /// Returns [`ElError::InvalidKey`] if `node_key` is not a valid secp256k1 secret.
    pub fn new(
        config: ElConfig,
        node_key: B256,
        head: watch::Receiver<Option<BlockRef>>,
        requests: mpsc::Receiver<ReceiptsRequest>,
        verified: mpsc::Sender<VerifiedReceipts>,
        served: mpsc::Sender<ExecutionPeer>,
        provider: P,
    ) -> Result<Self, ElError> {
        let (block_server, serving) = serve::new(provider, head.clone());
        let spec = NetworkSpec::op_stack(config.chain, config.bootnodes.clone());
        let label = spec.label;
        let peer_config = PeerConfig {
            listen_addr: config.listen_addr,
            advertised_addr: config.advertised_addr,
            saved_peers: config.saved_peers.clone(),
            max_sessions: config.max_sessions,
        };
        let (network, peers) =
            PeerNetwork::with_serving(spec, peer_config, node_key, head, served, serving)?;
        Ok(Self {
            config,
            network,
            peers,
            block_server,
            requests,
            verified,
            sync: None,
            fills: None,
            label,
        })
    }

    /// Fetches what gossip missed: each span asked for on `requests` (see [`FillRequest`]) is
    /// fetched from peers and verified by the hash chain down from its top, and its blocks
    /// are sent on `filled`, a few dozen at a time, ascending.
    #[must_use]
    pub fn with_fills(
        mut self,
        requests: mpsc::Receiver<FillRequest>,
        filled: mpsc::Sender<Vec<EncodedBlock>>,
    ) -> Self {
        self.fills = Some((requests, filled));
        self
    }

    /// The open sessions, as requesters see them: for what peers advertise.
    #[must_use]
    pub fn peers(&self) -> Peers {
        self.peers.clone()
    }

    /// Adds a range of blocks to fetch from peers and verify, next to the receipts of new
    /// blocks. It uses the same sessions, one request at a time on each.
    #[must_use]
    pub fn with_sync(mut self, sync: RangeSync) -> Self {
        self.sync = Some(sync);
        self
    }

    /// Runs the network until `cancel` fires.
    ///
    /// # Errors
    ///
    /// Returns [`ElError`] if discovery or the listener cannot bind their sockets, or a task
    /// of the network fails.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), ElError> {
        let Self {
            config,
            network,
            peers,
            block_server,
            requests,
            verified,
            sync,
            fills,
            label,
        } = self;
        // The peer network names itself; these tasks are named here.
        let span = info_span!("el", network = label);
        // Stopping any part stops the rest.
        let stop = cancel.child_token();
        let mut tasks: JoinSet<Result<(), ElError>> = JoinSet::new();
        {
            let stop = stop.clone();
            tasks.spawn(async move { network.run(stop).await });
        }
        {
            let stop = stop.clone();
            tasks.spawn(async move { block_server.run(stop).await }.instrument(span.clone()));
        }
        if let Some(sync) = sync {
            let (peers, stop) = (peers.clone(), stop.clone());
            let chain = config.chain;
            let run = async move { sync::run(chain, peers, sync, stop).await };
            tasks.spawn(run.instrument(span.clone()));
        }
        if let Some((requests, filled)) = fills {
            let (peers, stop) = (peers.clone(), stop.clone());
            let canyon_time = config.chain.canyon_time();
            let run = sync::run_fills(canyon_time, peers, requests, filled, stop);
            tasks.spawn(run.instrument(span.clone()));
        }
        let fetcher = Fetcher::new(config.chain, peers, requests, verified);
        {
            let stop = stop.clone();
            tasks.spawn(async move { fetcher.run(stop).await }.instrument(span));
        }
        network::join_all(tasks, &stop).await
    }
}
