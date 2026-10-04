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
mod metrics;
mod pacing;
mod peers;
mod serve;
mod session;
mod sync;
mod verify;
mod wire;

use std::sync::Arc;

use alloy_primitives::B256;
use op_indexer_primitives::{BlockRef, ExecutionPeer, ReceiptsRequest, VerifiedReceipts};
use secp256k1::SecretKey;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub use config::ElConfig;
pub use error::ElError;
pub use serve::BlockProvider;
pub use sync::RangeSync;

use crate::discovery::Discovery;
use crate::fetch::Fetcher;
use crate::peers::PeerSet;
use crate::serve::Server;
use crate::session::SessionContext;
use crate::sync::Syncer;

/// Discovered peers waiting for the peer set. Discovery repeats what does not fit.
const CANDIDATES_CAPACITY: usize = 256;
/// Inbound sessions waiting for the peer set; more are refused with "too many peers".
const ACCEPTED_CAPACITY: usize = 16;

/// The execution network: discovery, sessions, the receipts fetcher and the server of the
/// blocks `P` holds.
#[derive(Debug)]
pub struct ExecutionNetwork<P> {
    config: ElConfig,
    ctx: Arc<SessionContext>,
    block_server: Server<P>,
    requests: mpsc::Receiver<ReceiptsRequest>,
    verified: mpsc::Sender<VerifiedReceipts>,
    served: mpsc::Sender<ExecutionPeer>,
    /// A range of blocks to fetch from peers; `None` unless one was asked for.
    sync: Option<RangeSync>,
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
    /// receipts are answered from `provider`; with `None` the node serves nothing.
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
        provider: Option<P>,
    ) -> Result<Self, ElError> {
        let key = SecretKey::from_byte_array(&node_key.0).map_err(|_err| ElError::InvalidKey)?;
        let (block_server, serving) = serve::new(provider);
        let ctx = Arc::new(SessionContext::new(
            key,
            config.chain,
            config.listen_addr.port(),
            serving,
            head,
        ));
        Ok(Self {
            config,
            ctx,
            block_server,
            requests,
            verified,
            served,
            sync: None,
        })
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
            ctx,
            block_server,
            requests,
            verified,
            served,
            sync,
        } = self;
        metrics::describe();
        let bootnodes = if config.bootnodes.is_empty() {
            config
                .chain
                .bootnodes
                .iter()
                .map(|bootnode| (*bootnode).to_owned())
                .collect()
        } else {
            config.bootnodes.clone()
        };
        let discovery = Discovery::new(
            Arc::clone(&ctx),
            config.listen_addr,
            config.advertised_addr,
            bootnodes,
        )?;

        let (candidates_tx, candidates_rx) = mpsc::channel(CANDIDATES_CAPACITY);
        let (accepted_tx, accepted_rx) = mpsc::channel(ACCEPTED_CAPACITY);

        // Stopping any part stops the rest.
        let stop = cancel.child_token();
        let mut tasks: JoinSet<(&'static str, Result<(), ElError>)> = JoinSet::new();
        {
            let stop = stop.clone();
            tasks.spawn(async move { ("discovery", discovery.run(candidates_tx, stop).await) });
        }
        {
            let (ctx, stop) = (Arc::clone(&ctx), stop.clone());
            let addr = config.listen_addr;
            tasks.spawn(async move {
                (
                    "listener",
                    session::listen(ctx, addr, accepted_tx, stop).await,
                )
            });
        }
        let (peer_set, peers) = PeerSet::new(
            Arc::clone(&ctx),
            candidates_rx,
            accepted_rx,
            &config.saved_peers,
            served,
        );
        {
            let stop = stop.clone();
            tasks.spawn(async move { ("peer set", peer_set.run(stop).await) });
        }
        {
            let stop = stop.clone();
            tasks.spawn(async move { ("server", block_server.run(stop).await) });
        }
        if let Some(sync) = sync {
            let syncer = Syncer::new(config.chain.canyon_time, peers.clone(), sync);
            let stop = stop.clone();
            tasks.spawn(async move { ("range sync", syncer.run(stop).await) });
        }
        let fetcher = Fetcher::new(config.chain, peers, requests, verified);
        {
            let stop = stop.clone();
            tasks.spawn(async move { ("fetcher", fetcher.run(stop).await) });
        }

        let mut outcome = Ok(());
        while let Some(joined) = tasks.join_next().await {
            let result = match joined {
                Ok((_task, result)) => result,
                Err(source) => Err(ElError::Task {
                    task: "execution network",
                    source,
                }),
            };
            // The first failure stops the others; later ones are consequences of it.
            if outcome.is_ok() {
                outcome = result;
            }
            stop.cancel();
        }
        outcome
    }
}
