//! The node's wiring, shared by the `indexer` and `server` binaries.
//!
//! [`run`] starts every component around a committed store the binary opened (the indexer's
//! fjall archive, or the server's R2-backed one): the p2p network next to the pipeline that
//! stores the blocks it emits; when enabled, the execution network, which fetches the receipts
//! the pipeline asks for, serves the committed blocks to peers and, with the range sync on,
//! fetches the blocks between the archive's last one and the chain round after round; and the
//! L1 side, a beacon light client and an L1 execution p2p node that read the chain's dispute
//! games, whose commitments the pipeline promotes. The binary may add tasks of its own
//! ([`Task`]). Shuts down cleanly on Ctrl-C or SIGTERM: the networks first, then the
//! pipeline, which stores what they had already delivered.
//!
//! This crate is the binaries' top-level wiring, so its errors are `eyre` reports, as at a
//! binary's edge.

mod config;
mod persistence;
mod supervision;
mod sync;
pub use op_indexer_runtime::{env_file, shutdown_signal};
mod peers;
mod provider;

use std::future::Future;
use std::pin::Pin;

use std::sync::Arc;

use alloy_primitives::BlockNumber;
use eyre::WrapErr;
use op_indexer_chainspec::ChainSpec;
use op_indexer_el::{BlockProvider as _, ExecutionNetwork, RangeSync};
use op_indexer_l1::{BeaconConfig, L1Config, L1Network, LightClient};
use op_indexer_p2p::{Network, NodeStore, PayloadSource};
use op_indexer_pipeline::{FillReach, Pipeline, ReceiptsChannels};
use op_indexer_primitives::{BlockRef, EncodedBlock, FillRequest, L1Games, L1Heads};
use op_indexer_storage::unsafe_store::MemoryStore;
use op_indexer_storage::{ArchiveStore, StorageError, UnsafeStore};
use op_indexer_stream::{Load, StreamServer};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use crate::config::{Config, Profile};
use crate::config::{ElSettings, L1Settings, NODE_DIR};
use crate::peers::PeerSources;
pub use crate::peers::{NodeServed, PeerCounts};
use crate::persistence::{
    Stores, prepare_storage, save_l1_checkpoints, save_peers, save_sync_checkpoints,
};
use crate::provider::{NodeProvider, RangeEnd};
use crate::supervision::{Tasks, follow, run_components};
use crate::sync::{SyncInputs, forward_l1_heads, plan_sync};

/// Unsafe blocks waiting for the pipeline. Blocks arrive every 2 s on OP Mainnet and every
/// second on Unichain; this absorbs a store that is unreachable for about 8 or 4 minutes
/// before the network starts dropping them.
const BLOCK_CHANNEL_CAPACITY: usize = 256;

/// Requests for receipts waiting for the execution network. Its own queue is bounded too, so
/// this only has to absorb a burst, such as the blocks without receipts found at startup.
const RECEIPT_REQUEST_CAPACITY: usize = 1024;
/// Verified receipts waiting for the pipeline. The execution network waits when it is full,
/// and a block's receipts can be hundreds of kilobytes, so it is kept small.
const VERIFIED_RECEIPTS_CAPACITY: usize = 64;
/// Execution peers waiting to be saved. One is reported per dialed session, so a handful at a
/// time; the execution network drops a report when this is full.
const SERVED_PEERS_CAPACITY: usize = 32;
/// L1 blocks the light client vouches for, waiting for the L1 network: a head every 12 s and
/// a finalized block every few minutes.
const TRUSTED_L1_BLOCKS_CAPACITY: usize = 16;
/// Batches of a range sync waiting for the pipeline. A batch is up to 256 blocks, so this is
/// kept small; the sync waits when it is full.
const SYNC_BATCH_CAPACITY: usize = 2;
/// Missed gossip spans waiting for the execution network: a gap is rare and one at a time is
/// enough; one that does not fit is left to range sync.
const FILL_REQUEST_CAPACITY: usize = 8;
/// Segments of a missed span fetched and waiting for the pipeline (64 blocks each).
const FILLED_CAPACITY: usize = 4;
/// Verified checkpoints of a range sync waiting to be saved; the sync waits when it is full.
const SYNC_CHECKPOINT_CAPACITY: usize = 16;

/// A task of the binary's own, run next to the node's components: it gets a view of the
/// running node and the token that stops the networks, and must end when it fires. It ending
/// earlier stops the node.
pub type Task = Box<
    dyn FnOnce(
            NodeView,
            CancellationToken,
        ) -> Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>
        + Send,
>;

/// What a binary's task can see of the running node.
#[derive(Debug, Clone)]
pub struct NodeView {
    /// The newest block the node knows: the committed store's last block until gossip
    /// delivers a head.
    pub head: watch::Receiver<Option<BlockRef>>,
    /// How busy the stream server is.
    pub load: Load,
    /// The unsafe chain, for [`Self::contiguous_through`].
    unsafe_store: MemoryStore,
    /// Where [`Self::contiguous_through`]'s last search ended.
    range_end: RangeEnd,
    /// Where [`Self::peers`] reads from.
    peers: PeerSources,
}

impl NodeView {
    fn new(head: watch::Receiver<Option<BlockRef>>, load: Load, unsafe_store: MemoryStore) -> Self {
        Self {
            head,
            load,
            unsafe_store,
            range_end: RangeEnd::default(),
            peers: PeerSources::default(),
        }
    }

    /// How many peers each of the node's networks has now. Cheap: read from what the
    /// networks publish as they run.
    #[must_use]
    pub fn peers(&self) -> PeerCounts {
        self.peers.counts()
    }

    /// What the node served each network in the last minute, as each network's last status
    /// line counted it. Cheap, as [`Self::peers`].
    #[must_use]
    pub fn served(&self) -> NodeServed {
        self.peers.served()
    }

    /// The highest block N such that the node holds every block from `archive`'s first
    /// through N, each with its receipts: the range it advertises to execution peers,
    /// `archive`'s blocks up to the first still waiting for its receipts, extended through the
    /// unsafe chain's canonical blocks that link to them and have theirs. `None` while there
    /// is no such block.
    ///
    /// Cheap: each call continues the search from where the last one ended.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if `archive` cannot be read.
    pub async fn contiguous_through<A: Archive>(
        &self,
        archive: &A,
    ) -> Result<Option<BlockNumber>, StorageError> {
        let provider = NodeProvider::sharing_range(
            archive.clone(),
            self.unsafe_store.clone(),
            Arc::clone(&self.range_end),
        );
        Ok(provider.range().await?.map(|(_, end)| end.number))
    }
}

/// What every committed store the node runs on must be.
pub trait Archive: ArchiveStore + std::fmt::Debug {}

impl<A: ArchiveStore + std::fmt::Debug> Archive for A {}

/// Runs the node on `archive`, the committed store, which the binary has opened for
/// `config`'s chain, with the binary's own `tasks` (each named), until a signal arrives or
/// a component stops.
///
/// # Errors
///
/// Returns an error if a store or a network cannot be set up, or a component fails.
pub async fn run<A: Archive>(
    config: Config,
    archive: A,
    tasks: Vec<(&'static str, Task)>,
) -> eyre::Result<()> {
    config.log_capabilities();
    // The archive is opened first by the binary, so a refused archive leaves the node store
    // without a record. Startup-only blocking I/O, before any task runs.
    let store = NodeStore::open(config.data_dir.join(NODE_DIR), config.storage.chain)
        .wrap_err("failed to open node store")?;
    let stores = prepare_storage(&config.storage, archive).await?;
    let keypair = store.identity().wrap_err("failed to load node identity")?;
    let store = Arc::new(store);

    let (blocks_tx, blocks_rx) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
    // The heads the commitment task derives from the L1 side's dispute games. Without the L1
    // side nothing writes them, and the sender is kept so promotion sees a quiet source, not a
    // closed one.
    let (l1_source_tx, l1_source_rx) = watch::channel(L1Heads::default());
    // What promotion acts on: the L1 heads, once the archive is ready to be extended by it.
    let (l1_heads_tx, l1_heads_rx) = watch::channel(L1Heads::default());
    // The pipeline publishes the safe block number once its blocks are committed; the network
    // ignores gaps at or below it.
    let (safe_number_tx, safe_number_rx) = watch::channel(0);
    // The newest block the node knows, which the execution network advertises: the archive's
    // last block until gossip delivers a head. A node that only serves an archive has one too.
    let (head_tx, head_rx) = watch::channel(stores.archive_range.map(|(_, tip)| tip));
    // Taken before anything is published, so its first change is the first gossiped head.
    let gossip_head = head_rx.clone();
    let view_head = head_rx.clone();
    // With the range sync on, it closes the gaps gossip cannot: promotion extends the archive
    // from the unsafe store, which reaches only so far back, so an L1 head is held while the
    // archive is further than that below its safe block, and a sync round closes the gap.
    let sync_archive = config.sync.then(|| stores.archive.clone());
    let gate_archive = sync_archive.clone();

    // Tasks that follow the node until it stops, each returning its name: one ending earlier
    // stops the node.
    let mut followers = Tasks::default();
    let execution = config
        .el
        .map(|el| {
            let sync = sync_archive.map(|archive| SyncInputs {
                archive,
                unsafe_store: stores.unsafe_store.clone(),
                gossip_head,
                l1_heads: l1_source_rx.clone(),
                committed: safe_number_rx.clone(),
                l1: config.l1.is_some(),
            });
            execution_network(el, sync, &store, &stores, head_rx, &mut followers)
        })
        .transpose()?;
    let (execution, receipts, inputs, mut saves) = execution.map_or_else(
        || (None, None, PipelineInputs::default(), Vec::new()),
        |parts| {
            (
                Some(parts.network),
                Some(parts.receipts),
                parts.inputs,
                parts.saves,
            )
        },
    );

    // The blocks older consensus-layer nodes may ask for by number.
    let payloads: Arc<dyn PayloadSource> = Arc::new(NodeProvider::new(
        stores.archive.clone(),
        stores.unsafe_store.clone(),
    ));
    // Reads the stores the pipeline writes, and the unsafe store's events.
    let stream = StreamServer::new(
        config.stream,
        stores.unsafe_store.clone(),
        stores.archive.clone(),
    );
    let view = NodeView::new(view_head, stream.load(), stores.unsafe_store.clone());
    let pipeline = Pipeline::new(
        stores.unsafe_store,
        stores.archive,
        config.network.chain.canyon_time(),
        blocks_rx,
        l1_heads_rx,
        safe_number_tx,
        receipts,
    )
    .with_head(head_tx);
    let pipeline = inputs.extend(pipeline);
    let gate = gate_archive.map(|archive| (archive, safe_number_rx.clone()));
    let forward = forward_l1_heads(l1_source_rx, l1_heads_tx, gate);
    follow(&mut followers, "L1 heads forwarder", forward);
    // The L1 side, whose games the pipeline turns into the heads; without it nothing writes
    // them and the sender is only kept alive, so promotion sees a quiet source, not a closed
    // one.
    let (l1, pipeline, _idle) = match config.l1 {
        Some(settings) => {
            let l1 = l1_side(settings, config.network.chain, &store)?;
            saves.push(("L1 peer saver", l1.served));
            saves.push(("beacon checkpoint saver", l1.checkpoints));
            let isthmus_time = config.network.chain.isthmus_time();
            let pipeline = pipeline.with_l1_games(l1.games, l1_source_tx, isthmus_time);
            (Some((l1.network, l1.light_client)), pipeline, None)
        }
        None => (None, pipeline, Some(l1_source_tx)),
    };
    let network = Network::new(
        config.network,
        keypair,
        store,
        blocks_tx,
        safe_number_rx,
        payloads,
    );
    run_components(
        network,
        execution,
        l1,
        (pipeline, stream),
        (followers, tasks, view),
        saves,
    )
    .await
}

/// The L1 side: the two components and what connects them to the rest.
struct L1Side {
    /// Reads the chain's dispute games from the L1 blocks it is told to trust.
    network: L1Network,
    /// Tells it which L1 blocks to trust: verified beacon headers, from the checkpoint on.
    light_client: LightClient,
    /// The games, for the pipeline to check against our blocks.
    games: watch::Receiver<L1Games>,
    /// Saves the L1 peers that served us.
    served: JoinHandle<()>,
    /// Saves the light client's newest finalized beacon block.
    checkpoints: JoinHandle<()>,
}

/// Builds the L1 side: a beacon light client that follows Ethereum's finality from the
/// configured checkpoint, and an execution p2p node on L1 that reads the chain's dispute
/// games from the blocks the light client vouches for.
fn l1_side(
    settings: L1Settings,
    chain: &'static ChainSpec,
    store: &Arc<NodeStore>,
) -> eyre::Result<L1Side> {
    // A key of its own, like the execution network's.
    let key = store
        .l1_key()
        .wrap_err("failed to load the L1 network key")?;
    let (trusted_tx, trusted_rx) = mpsc::channel(TRUSTED_L1_BLOCKS_CAPACITY);
    let (games_tx, games) = watch::channel(L1Games::default());
    let (served_tx, served_rx) = mpsc::channel(SERVED_PEERS_CAPACITY);
    let config = L1Config {
        chain,
        listen_addr: settings.listen_addr,
        advertised_addr: settings.advertised_addr,
        bootnodes: Vec::new(),
        // L1 peers have few free slots: the ones that served before are dialed first.
        saved_peers: store
            .l1_peers()
            .wrap_err("failed to load the saved L1 peers")?,
    };
    let network = L1Network::new(config, key, trusted_rx, games_tx, served_tx)
        .wrap_err("failed to create the L1 network")?;
    let (finalized_tx, finalized_rx) = watch::channel(None);
    let light_client = LightClient::new(
        BeaconConfig {
            checkpoint: settings.checkpoint,
            saved: store
                .l1_checkpoint()
                .wrap_err("failed to load the saved beacon checkpoint")?,
            listen_addr: settings.beacon_listen_addr,
            // Beacon nodes share the discovery network of the chain's bootnodes.
            bootnodes: chain.consensus_bootnodes().map(str::to_owned).collect(),
        },
        trusted_tx,
        finalized_tx,
    );
    let served = tokio::spawn(save_peers(
        Arc::clone(store),
        served_rx,
        NodeStore::save_l1_peer,
    ));
    let checkpoints = tokio::spawn(save_l1_checkpoints(Arc::clone(store), finalized_rx));
    Ok(L1Side {
        network,
        light_client,
        games,
        served,
        checkpoints,
    })
}

/// The execution network with everything that goes with it.
struct Execution<A: Archive> {
    network: ExecutionNetwork<NodeProvider<A>>,
    /// The pipeline's ends of the receipts channels.
    receipts: ReceiptsChannels,
    /// What it feeds the pipeline besides receipts.
    inputs: PipelineInputs,
    /// Tasks that save what the network reports to the node store.
    saves: Vec<(&'static str, JoinHandle<()>)>,
}

/// What the execution network feeds the pipeline besides receipts.
#[derive(Default)]
struct PipelineInputs {
    /// The batches of the range sync, when a range is configured.
    range: Option<mpsc::Receiver<Vec<EncodedBlock>>>,
    /// Where missed gossip spans are asked for, and the blocks fetched for them.
    fills: Option<(
        mpsc::Sender<FillRequest>,
        mpsc::Receiver<Vec<EncodedBlock>>,
        FillReach,
    )>,
}

impl PipelineInputs {
    /// `pipeline` with these inputs.
    fn extend<U, A>(self, pipeline: Pipeline<U, A>) -> Pipeline<U, A>
    where
        U: UnsafeStore + Clone + Send + Sync + 'static,
        A: ArchiveStore,
    {
        let pipeline = match self.range {
            Some(range) => pipeline.with_range(range),
            None => pipeline,
        };
        match self.fills {
            Some((requests, filled, reach)) => pipeline.with_fills(requests, filled, reach),
            None => pipeline,
        }
    }
}

/// Builds the execution network from its settings and what the node store has saved for it.
/// Peers are served from the archive. `head` is the
/// newest block the node knows. With `sync`, the blocks between the archive and the chain's
/// head are fetched from peers, planned by a task added to `followers`.
fn execution_network<A: Archive>(
    el: ElSettings,
    sync: Option<SyncInputs<A>>,
    store: &Arc<NodeStore>,
    stores: &Stores<A>,
    head: watch::Receiver<Option<BlockRef>>,
    followers: &mut Tasks,
) -> eyre::Result<Execution<A>> {
    // A key of its own: the two networks must not share a node id.
    let key = store
        .execution_key()
        .wrap_err("failed to load the execution network key")?;
    let saved_peers = store
        .execution_peers()
        .wrap_err("failed to load the saved execution peers")?;
    let (requests_tx, requests_rx) = mpsc::channel(RECEIPT_REQUEST_CAPACITY);
    let (verified_tx, verified_rx) = mpsc::channel(VERIFIED_RECEIPTS_CAPACITY);
    let (served_tx, served_rx) = mpsc::channel(SERVED_PEERS_CAPACITY);
    let provider = NodeProvider::new(stores.archive.clone(), stores.unsafe_store.clone());
    let network = ExecutionNetwork::new(
        el.into_config(saved_peers),
        key,
        head,
        requests_rx,
        verified_tx,
        served_tx,
        provider,
    )
    .wrap_err("failed to create the execution network")?;
    let mut saves = vec![(
        "execution peer saver",
        tokio::spawn(save_peers(
            Arc::clone(store),
            served_rx,
            NodeStore::save_execution_peer,
        )),
    )];

    // What the fill leaves to range sync: everything up to the safe head with the L1 side,
    // everything but the last stretch without it.
    let reach = match &sync {
        None => FillReach::Any,
        Some(inputs) if inputs.l1 => FillReach::AboveSafe,
        Some(_) => FillReach::Recent,
    };
    let (network, range) = match sync {
        Some(inputs) => {
            let (plans_tx, plans_rx) = mpsc::channel(1);
            let (blocks_tx, batches) = mpsc::channel(SYNC_BATCH_CAPACITY);
            let (checkpoints_tx, checkpoints_rx) = mpsc::channel(SYNC_CHECKPOINT_CAPACITY);
            let plan = plan_sync(Arc::clone(store), inputs, network.peers(), plans_tx);
            follow(followers, "range sync planner", plan);
            saves.push((
                "sync checkpoint saver",
                tokio::spawn(save_sync_checkpoints(Arc::clone(store), checkpoints_rx)),
            ));
            let network = network.with_sync(RangeSync {
                plans: plans_rx,
                blocks: blocks_tx,
                verified: checkpoints_tx,
            });
            (network, Some(batches))
        }
        None => (network, None),
    };
    // Spans gossip missed: the pipeline asks, the network fetches.
    let (fill_requests_tx, fill_requests_rx) = mpsc::channel(FILL_REQUEST_CAPACITY);
    let (filled_tx, filled_rx) = mpsc::channel(FILLED_CAPACITY);
    let network = network.with_fills(fill_requests_rx, filled_tx);
    Ok(Execution {
        network,
        receipts: ReceiptsChannels {
            requests: requests_tx,
            verified: verified_rx,
        },
        inputs: PipelineInputs {
            range,
            fills: Some((fill_requests_tx, filled_rx, reach)),
        },
        saves,
    })
}
