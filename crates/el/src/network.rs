//! One devp2p `eth` network: which network it is ([`NetworkSpec`]) and the part that finds
//! its peers and keeps sessions with them ([`PeerNetwork`]).
//!
//! The same code serves the OP Stack chain's execution network and Ethereum's: they differ in
//! the network id, the genesis hash, the fork schedule (so the fork id), the bootnodes and
//! the node record key their nodes publish the fork id under. What is asked of the peers and
//! how answers are verified is not here: a requester gets the open sessions through
//! [`Peers`].

use std::net::SocketAddr;
use std::sync::Arc;

use alloy_eip2124::{ForkFilter, ForkFilterKey, Head};
use alloy_primitives::{B256, BlockNumber};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::{BlockRef, ExecutionPeer};
use secp256k1::SecretKey;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::discovery::Discovery;
use crate::peers::{PeerSet, Peers, Report};
use crate::serve::{self, Serving};
use crate::session::{self, SessionContext, SessionHandle, unix_now};
use crate::{ElError, metrics};

/// Discovered peers waiting for the peer set. Discovery repeats what does not fit.
const CANDIDATES_CAPACITY: usize = 256;
/// Inbound sessions waiting for the peer set; more are refused with "too many peers".
const ACCEPTED_CAPACITY: usize = 16;

/// Node record key under which Ethereum execution nodes, and op-geth, publish their fork id.
pub const ETH_RECORD_KEY: &str = "eth";
/// Node record key under which op-reth publishes its fork id.
pub const OPEL_RECORD_KEY: &str = "opel";
/// Node record key that marks an op-p2p-indexer. Its value is [`INDEXER_RECORD_VERSION`].
pub(crate) const INDEXER_RECORD_KEY: &str = "opidx";
/// Version of the op-p2p-indexer entry: how indexers share with each other. Any version marks
/// an indexer.
pub(crate) const INDEXER_RECORD_VERSION: u8 = 1;

/// What identifies one devp2p `eth` network to its peers.
#[derive(Debug, Clone)]
pub struct NetworkSpec {
    /// A short name for the network in metrics and logs: `op` or `l1`.
    pub label: &'static str,
    /// The network id of the eth status (the chain id, for the networks used here).
    pub network_id: u64,
    /// Hash of the chain's block 0.
    pub genesis_hash: B256,
    /// Timestamp (seconds) of the chain's block 0, which the fork filter uses to tell a fork
    /// activation that is a time from one that is a block number.
    pub genesis_time: u64,
    /// Blocks at which a hardfork that changes the fork id activated, ascending.
    pub fork_blocks: Vec<BlockNumber>,
    /// Timestamps (seconds) at which a hardfork that changes the fork id activated, ascending.
    pub fork_times: Vec<u64>,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs.
    pub bootnodes: Vec<String>,
    /// The node record keys this network's nodes publish their fork id under, preferred
    /// first. Our own record carries all of them.
    pub record_keys: &'static [&'static str],
    /// On a network op-p2p-indexers share: blocks below this number are served to, and
    /// fetched from, indexer peers only, and our node record carries the `opidx` entry.
    /// The chain's Bedrock block, so 0 (nothing held back) on a chain without a legacy chain.
    /// `None` on a network without indexers (Ethereum's).
    pub indexers_only_below: Option<BlockNumber>,
}

impl NetworkSpec {
    /// The execution network of an OP Stack chain; `bootnodes` replace the chain's if given.
    #[must_use]
    pub fn op_stack(chain: &ChainSpec, bootnodes: Vec<String>) -> Self {
        let bootnodes = if bootnodes.is_empty() {
            chain.bootnodes().map(str::to_owned).collect()
        } else {
            bootnodes
        };
        Self {
            label: "op",
            network_id: chain.chain_id,
            genesis_hash: chain.genesis_hash,
            genesis_time: chain.genesis_time,
            fork_blocks: chain.fork_blocks.to_vec(),
            fork_times: chain.fork_times().to_vec(),
            bootnodes,
            record_keys: &[OPEL_RECORD_KEY, ETH_RECORD_KEY],
            indexers_only_below: Some(chain.bedrock_block),
        }
    }

    /// The [EIP-2124] fork filter now: yields our fork id and validates a peer's.
    ///
    /// By wall-clock time, with every fork that activates at a block number taken as passed.
    /// The node may know no head yet, and a fork id computed at block 0 names the first block
    /// fork as "next": a peer checking it against its own head would reject us as stale. On
    /// the chains used here the block forks are years old; what changes the fork id while the
    /// node runs is a time fork activating.
    ///
    /// [EIP-2124]: https://eips.ethereum.org/EIPS/eip-2124
    pub(crate) fn fork_filter(&self) -> ForkFilter {
        let head = Head {
            number: BlockNumber::MAX,
            timestamp: unix_now(),
            ..Head::default()
        };
        let blocks = self.fork_blocks.iter().copied().map(ForkFilterKey::Block);
        let times = self.fork_times.iter().copied().map(ForkFilterKey::Time);
        ForkFilter::new(
            head,
            self.genesis_hash,
            self.genesis_time,
            blocks.chain(times),
        )
    }

    /// Whether `time` is a hardfork activation this build knows.
    pub(crate) fn knows_fork_time(&self, time: u64) -> bool {
        self.fork_times.contains(&time)
    }
}

/// Where one network's node listens and what it remembers from earlier runs.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Listen address, used for both discovery (UDP) and sessions (TCP).
    pub listen_addr: SocketAddr,
    /// The address (IP and port, the same for TCP and UDP) the node record advertises, for a
    /// node reachable at a known public address. `None` lets discovery learn it.
    pub advertised_addr: Option<SocketAddr>,
    /// Peers that served us in an earlier run, most recently served first: dialed first.
    pub saved_peers: Vec<ExecutionPeer>,
    /// Sessions kept in each direction: this many dialed, as many accepted. On a network
    /// op-p2p-indexers share, one more is dialed for an indexer peer.
    pub max_sessions: usize,
}

impl PeerConfig {
    /// Default of [`Self::max_sessions`]: a node that asks slowly needs few, and full nodes
    /// ration their slots.
    pub const DEFAULT_MAX_SESSIONS: usize = 4;
}

/// Discovery, the session listener and the peer set of one network.
#[derive(Debug)]
pub struct PeerNetwork {
    config: PeerConfig,
    ctx: Arc<SessionContext>,
    served: mpsc::Sender<ExecutionPeer>,
    reports: mpsc::Receiver<Report>,
    published: watch::Sender<Arc<[SessionHandle]>>,
}

impl PeerNetwork {
    /// Creates the network's node and the handle requesters use. Does no I/O. This node
    /// answers peers' requests with empty answers: it serves nothing.
    ///
    /// `node_key` is the node's secp256k1 secret on this network; two networks need two keys,
    /// because each runs a discv5 node and one key in two of them would publish conflicting
    /// node records. `head` follows the newest block of this network's chain the node knows:
    /// it is what the eth status advertises, and no session is opened while it is `None`,
    /// because peers end a session whose status advertises genesis. A peer worth saving for
    /// the next start is reported on `served`, without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`ElError::InvalidKey`] if `node_key` is not a valid secp256k1 secret.
    pub fn new(
        spec: NetworkSpec,
        config: PeerConfig,
        node_key: B256,
        head: watch::Receiver<Option<BlockRef>>,
        served: mpsc::Sender<ExecutionPeer>,
    ) -> Result<(Self, Peers), ElError> {
        Self::with_serving(spec, config, node_key, head, served, serve::disabled())
    }

    /// As [`Self::new`], with `serving` as the way to whatever answers peers' requests.
    pub(crate) fn with_serving(
        spec: NetworkSpec,
        config: PeerConfig,
        node_key: B256,
        head: watch::Receiver<Option<BlockRef>>,
        served: mpsc::Sender<ExecutionPeer>,
        serving: Serving,
    ) -> Result<(Self, Peers), ElError> {
        let key = SecretKey::from_byte_array(&node_key.0).map_err(|_err| ElError::InvalidKey)?;
        let ctx = Arc::new(SessionContext::new(
            key,
            spec,
            config.listen_addr.port(),
            serving,
            head,
        ));
        // Saved peers are known indexers before discovery finds them again.
        for peer in config.saved_peers.iter().filter(|peer| peer.indexer) {
            ctx.mark_indexer(peer.id);
        }
        let (peers, reports, published) = Peers::new();
        let network = Self {
            config,
            ctx,
            served,
            reports,
            published,
        };
        Ok((network, peers))
    }

    /// Runs discovery, the listener and the peer set until `cancel` fires.
    ///
    /// # Errors
    ///
    /// Returns [`ElError`] if discovery or the listener cannot bind their sockets, or one of
    /// the three fails.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), ElError> {
        let Self {
            config,
            ctx,
            served,
            reports,
            published,
        } = self;
        metrics::describe();
        let discovery =
            Discovery::new(Arc::clone(&ctx), config.listen_addr, config.advertised_addr)?;
        let (candidates_tx, candidates_rx) = mpsc::channel(CANDIDATES_CAPACITY);
        let (accepted_tx, accepted_rx) = mpsc::channel(ACCEPTED_CAPACITY);
        let peer_set = PeerSet::new(
            Arc::clone(&ctx),
            candidates_rx,
            accepted_rx,
            &config,
            served,
            reports,
            published,
        );

        // Every line of this network names it: with L1 on, two of them run.
        let span = info_span!("el", network = ctx.spec().label);
        // Stopping any part stops the rest.
        let stop = cancel.child_token();
        let mut tasks: JoinSet<Result<(), ElError>> = JoinSet::new();
        {
            let stop = stop.clone();
            let run = async move { discovery.run(candidates_tx, stop).await };
            tasks.spawn(run.instrument(span.clone()));
        }
        {
            let (stop, addr) = (stop.clone(), config.listen_addr);
            let run = async move { session::listen(ctx, addr, accepted_tx, stop).await };
            tasks.spawn(run.instrument(span.clone()));
        }
        {
            let stop = stop.clone();
            tasks.spawn(async move { peer_set.run(stop).await }.instrument(span));
        }
        join_all(tasks, &stop).await
    }
}

/// Waits for every task; the first failure stops the others and is returned.
pub(crate) async fn join_all(
    mut tasks: JoinSet<Result<(), ElError>>,
    stop: &CancellationToken,
) -> Result<(), ElError> {
    let mut outcome = Ok(());
    while let Some(joined) = tasks.join_next().await {
        let result = joined.unwrap_or_else(|source| {
            Err(ElError::Task {
                task: "execution network",
                source,
            })
        });
        // Later failures are consequences of the first.
        if outcome.is_ok() {
            outcome = result;
        }
        stop.cancel();
    }
    outcome
}
