//! Indexer for the OP Stack peer-to-peer network.
//!
//! Wires the components together: loads config and the node identity, opens the block archive (the
//! committed store), checks that Redis is reachable and its key layout current, and runs the p2p
//! network next to the pipeline that stores the blocks it emits. When the execution network is
//! enabled it runs too: it fetches the receipts the pipeline asks for and serves the archive's
//! blocks to peers, and, when the range sync is on, fetches the blocks between the archive's last
//! one and the chain from peers for the pipeline to store, round after round. When the L1 side is
//! enabled, a beacon light client and an L1 execution p2p node read the chain's dispute games, and
//! the pipeline promotes the blocks they commit to. Shuts down cleanly on Ctrl-C or SIGTERM: the
//! networks first, then the pipeline, which stores what they had already delivered.

mod config;
mod provider;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::BlockNumber;
use eyre::WrapErr;
use op_indexer_chainspec::ChainSpec;
use op_indexer_el::{ExecutionNetwork, RangeSync, RoundEnd, SyncPlan};
use op_indexer_l1::{BeaconConfig, L1Config, L1Network, LightClient};
use op_indexer_p2p::{Network, NodeStore, PayloadSource, StoreError};
use op_indexer_pipeline::{Pipeline, ReceiptsChannels};
use op_indexer_primitives::{BlockRef, EncodedBlock, ExecutionPeer, L1Games, L1Heads, SyncRange};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::unsafe_store::RedisStore;
use op_indexer_storage::{ArchiveStore, StorageConfig, UnsafeStore};
use op_indexer_stream::StreamServer;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::{Config, ElSettings, L1Settings};
use crate::provider::NodeProvider;

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
/// How far below the head the archive may be and still be extended by promotion: the most
/// blocks the unsafe store returns in one read back from a head. A range sync round is planned
/// only for a larger gap.
const CAUGHT_UP_BLOCKS: u64 = 1024;
/// Rest after a round is given up before the next starts, doubled for each round given up in
/// a row.
const ABANDONED_ANCHOR_WAIT: Duration = Duration::from_mins(1);
/// The longest rest after a round is given up.
const ABANDONED_ANCHOR_MAX_WAIT: Duration = Duration::from_mins(30);
/// How long the archive may take to reach the anchor of a round the execution network has
/// fetched completely. Longer, the round's blocks were left out (they did not extend the
/// archive), and the round is given up.
const ROUND_STORE_TIMEOUT: Duration = Duration::from_mins(2);
/// How often a failing read of the archive or the node store is warned about while retried.
const RETRY_WARN_INTERVAL: Duration = Duration::from_mins(1);
/// How far below the gossiped head a round anchors when no safe head is known: the anchor is
/// a block an unsafe reorg will not replace in practice (they are a few blocks deep).
const ANCHOR_DEPTH: u64 = 64;
/// How often the archive is asked whether a round of the range sync has reached its end, and
/// whether it holds the safe block of the L1 heads promotion waits for.
const SYNC_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Verified checkpoints of a range sync waiting to be saved; the sync waits when it is full.
const SYNC_CHECKPOINT_CAPACITY: usize = 16;

/// Directory of the node store (identity and known peers), inside the data directory.
const NODE_DIR: &str = "node";

/// Log timestamp: UTC time of day with milliseconds, e.g. `13:04:12.345`.
const LOG_TIME_FORMAT: &str = "%H:%M:%S%.3f";

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_timer(ChronoUtc::new(LOG_TIME_FORMAT.to_owned()))
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;

    // Startup-only blocking I/O, before any task runs.
    std::fs::create_dir_all(&config.data_dir).wrap_err("failed to create data dir")?;
    // The local stores first, so a data directory of another chain fails before any
    // connection; the archive before the node store, so a refused archive leaves the node
    // store without a record.
    let archive = open_archive(&config.storage)?;
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
    // With the range sync on, it closes the gaps gossip cannot: promotion extends the archive
    // from the unsafe store, which reaches only so far back, so an L1 head is held while the
    // archive is further than that below its safe block, and a sync round closes the gap.
    let sync_archive = config.sync.then(|| stores.archive.clone());
    let gate_archive = sync_archive.clone();

    // Tasks that follow the node until it stops, each returning its name: one ending earlier
    // stops the node.
    let mut followers = JoinSet::new();
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
    let (execution, receipts, range, mut saves) = execution.map_or_else(
        || (None, None, None, Vec::new()),
        |parts| {
            (
                Some(parts.network),
                Some(parts.receipts),
                parts.range,
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
    let pipeline = Pipeline::new(
        stores.unsafe_store,
        stores.archive,
        config.network.chain.canyon_time,
        blocks_rx,
        l1_heads_rx,
        safe_number_tx,
        receipts,
    )
    .with_head(head_tx);
    let pipeline = match range {
        Some(range) => pipeline.with_range(range),
        None => pipeline,
    };
    let gate = gate_archive.map(|archive| (archive, safe_number_rx.clone()));
    let forward = forward_l1_heads(l1_source_rx, l1_heads_tx, gate);
    follow(&mut followers, "L1 heads forwarder", forward);
    // The L1 side, whose games the pipeline turns into the heads; without it nothing writes
    // them and the sender is only kept alive, so promotion sees a quiet source, not a closed
    // one.
    let (l1, pipeline, _idle) = match config.l1 {
        Some(settings) => {
            let l1 = l1_side(settings, config.network.chain, &store)?;
            saves.push(l1.served);
            let isthmus_time = config.network.chain.isthmus_time;
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
    run(network, execution, l1, (pipeline, stream), followers, saves).await
}

/// Spawns `task` into `followers`, named for the error if it ends before the node stops.
fn follow(
    followers: &mut JoinSet<&'static str>,
    name: &'static str,
    task: impl Future<Output = ()> + Send + 'static,
) {
    followers.spawn(async move {
        task.await;
        name
    });
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
    let light_client = LightClient::new(
        BeaconConfig {
            checkpoint: settings.checkpoint,
            listen_addr: settings.beacon_listen_addr,
            // Beacon nodes share the discovery network of the chain's bootnodes.
            bootnodes: chain.bootnodes().map(str::to_owned).collect(),
        },
        trusted_tx,
    );
    let served = tokio::spawn(save_peers(
        Arc::clone(store),
        served_rx,
        NodeStore::save_l1_peer,
    ));
    Ok(L1Side {
        network,
        light_client,
        games,
        served,
    })
}

/// Runs the components until one stops or a signal arrives, then stops the others in order:
/// the networks first, on their own token, so the pipeline can still store what they
/// delivered.
async fn run(
    network: Network,
    execution: Option<ExecutionNetwork<NodeProvider>>,
    l1: Option<(L1Network, LightClient)>,
    (pipeline, stream): (
        Pipeline<RedisStore, FjallArchive>,
        StreamServer<RedisStore, FjallArchive>,
    ),
    mut followers: JoinSet<&'static str>,
    saves: Vec<JoinHandle<()>>,
) -> eyre::Result<()> {
    let cancel = CancellationToken::new();
    let networks_cancel = cancel.child_token();
    let mut network = Some(tokio::spawn(network.run(networks_cancel.clone())));
    let mut execution =
        execution.map(|execution| tokio::spawn(execution.run(networks_cancel.clone())));
    let (l1, light_client) = l1.unzip();
    let mut l1 = l1.map(|l1| tokio::spawn(l1.run(networks_cancel.clone())));
    let mut light_client =
        light_client.map(|light_client| tokio::spawn(light_client.run(networks_cancel.clone())));
    let mut pipeline = Some(tokio::spawn(
        pipeline.run(networks_cancel.clone(), cancel.clone()),
    ));
    // Stopped with the networks: consumers are told the node is shutting down.
    let mut stream = Some(tokio::spawn(stream.run(networks_cancel.clone())));

    // Any component stopping ends the process; the others are stopped and waited for.
    let stopped = tokio::select! {
        signal = shutdown_signal() => signal.map(|signal| info!(signal, "shutting down")),
        result = finished(&mut network) => {
            warn!("network stopped unexpectedly");
            result.wrap_err("network failed")
        }
        result = finished(&mut execution) => {
            warn!("execution network stopped unexpectedly");
            result.wrap_err("execution network failed")
        }
        result = finished(&mut l1) => {
            warn!("L1 network stopped unexpectedly");
            result.wrap_err("L1 network failed")
        }
        result = finished(&mut light_client) => {
            warn!("beacon light client stopped");
            result.wrap_err("beacon light client failed")
        }
        result = finished(&mut pipeline) => {
            warn!("pipeline stopped unexpectedly");
            result.wrap_err("pipeline failed")
        }
        result = finished(&mut stream) => {
            warn!("stream server stopped unexpectedly");
            result.wrap_err("stream server failed")
        }
        Some(ended) = followers.join_next() => match ended {
            Ok(task) => Err(eyre::eyre!("the {task} ended before shutdown")),
            Err(err) => Err(err).wrap_err("a node task panicked"),
        },
    };

    networks_cancel.cancel();
    let network = join(network).await.wrap_err("network failed");
    let execution = join(execution).await.wrap_err("execution network failed");
    let l1 = join(l1).await.wrap_err("L1 network failed");
    let light_client = join(light_client)
        .await
        .wrap_err("beacon light client failed");
    cancel.cancel();
    // The stream ends with the networks; it is waited for with the pipeline, so a slow stream
    // shutdown never holds the pipeline running.
    let (stream, pipeline) = tokio::join!(join(stream), join(pipeline));
    let stream = stream.wrap_err("stream server failed");
    let pipeline = pipeline.wrap_err("pipeline failed");
    // Their inputs are gone with the networks and the pipeline, so they end.
    while let Some(ended) = followers.join_next().await {
        if let Err(err) = ended {
            warn!(%err, "a node task failed");
        }
    }
    for save in saves {
        if let Err(err) = save.await {
            warn!(%err, "a task saving node state failed");
        }
    }
    // The reason the process stopped comes first, then whatever failed while shutting down.
    stopped
        .and(network)
        .and(execution)
        .and(l1)
        .and(light_client)
        .and(stream)
        .and(pipeline)
}

/// Waits for `task` to finish and clears it, so it is not awaited again. Never resolves when
/// there is no task, which lets a disabled component sit in a `select!`. Components run until
/// they are cancelled, so one that ends before shutdown is an error even when it ends cleanly.
async fn finished<E>(task: &mut Option<JoinHandle<Result<(), E>>>) -> eyre::Result<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let Some(handle) = task else {
        return std::future::pending().await;
    };
    let result = handle.await;
    *task = None;
    result.wrap_err("task panicked")??;
    Err(eyre::eyre!("ended before shutdown"))
}

/// Waits for `task` if it has not finished yet.
async fn join<E>(task: Option<JoinHandle<Result<(), E>>>) -> eyre::Result<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match task {
        Some(handle) => Ok(handle.await.wrap_err("task panicked")??),
        None => Ok(()),
    }
}

/// The execution network with everything that goes with it.
struct Execution {
    network: ExecutionNetwork<NodeProvider>,
    /// The pipeline's ends of the receipts channels.
    receipts: ReceiptsChannels,
    /// The batches of the range sync, for the pipeline, when a range is configured.
    range: Option<mpsc::Receiver<Vec<EncodedBlock>>>,
    /// Tasks that save what the network reports to the node store.
    saves: Vec<JoinHandle<()>>,
}

/// Builds the execution network from its settings and what the node store has saved for it.
/// Peers are served from the archive. `head` is the
/// newest block the node knows. With `sync`, the blocks between the archive and the chain's
/// head are fetched from peers, planned by a task added to `followers`.
fn execution_network(
    el: ElSettings,
    sync: Option<SyncInputs>,
    store: &Arc<NodeStore>,
    stores: &Stores,
    head: watch::Receiver<Option<BlockRef>>,
    followers: &mut JoinSet<&'static str>,
) -> eyre::Result<Execution> {
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
    let mut saves = vec![tokio::spawn(save_peers(
        Arc::clone(store),
        served_rx,
        NodeStore::save_execution_peer,
    ))];

    let (network, range) = match sync {
        Some(inputs) => {
            let (plans_tx, plans_rx) = mpsc::channel(1);
            let (blocks_tx, batches) = mpsc::channel(SYNC_BATCH_CAPACITY);
            let (checkpoints_tx, checkpoints_rx) = mpsc::channel(SYNC_CHECKPOINT_CAPACITY);
            let plan = plan_sync(Arc::clone(store), inputs, plans_tx);
            follow(followers, "range sync planner", plan);
            saves.push(tokio::spawn(save_sync_checkpoints(
                Arc::clone(store),
                checkpoints_rx,
            )));
            let network = network.with_sync(RangeSync {
                plans: plans_rx,
                blocks: blocks_tx,
                verified: checkpoints_tx,
            });
            (network, Some(batches))
        }
        None => (network, None),
    };
    Ok(Execution {
        network,
        receipts: ReceiptsChannels {
            requests: requests_tx,
            verified: verified_rx,
        },
        range,
        saves,
    })
}

/// What planning the range sync needs.
struct SyncInputs {
    /// The archive the sync fills: its last block is where each round starts.
    archive: FjallArchive,
    /// Where a block [`ANCHOR_DEPTH`] below the gossiped head is looked up.
    unsafe_store: RedisStore,
    /// The unsafe head gossip delivers.
    gossip_head: watch::Receiver<Option<BlockRef>>,
    /// The L1 heads, before promotion sees them: the safe block is the preferred anchor.
    l1_heads: watch::Receiver<L1Heads>,
    /// The committed safe block's number, which the pipeline publishes.
    committed: watch::Receiver<BlockNumber>,
    /// Whether the L1 side runs: rounds are then anchored on safe heads only.
    l1: bool,
}

/// How a round ended, as the planner sees it.
enum RoundOutcome {
    /// The archive holds the anchor.
    Stored,
    /// The anchor was given up: no peer served it, or its chain does not reach the archive.
    Abandoned,
    /// The node is stopping.
    Stop,
}

/// The rest after a round was given up: when the next one may start, and how long the
/// rest was, doubled if the next is given up too.
struct Rest {
    until: tokio::time::Instant,
    wait: Duration,
}

/// Plans the range sync, round by round, for as long as the node runs, and hands each plan to
/// the execution network: from the block after the archive's last one up to an anchor whose
/// hash is trusted.
///
/// The sync only closes the gaps gossip cannot (see [`next_anchor`] for when a round is
/// planned and what it is anchored on); promotion extends the archive otherwise. The first
/// anchor is the one an unfinished round of an earlier run was working towards, so its
/// verified checkpoints are kept. After a round is given up the next waits
/// [`ABANDONED_ANCHOR_WAIT`], doubled for each round given up in a row. Reads of the archive
/// and the node store that fail are retried; the planner ends only when the node stops.
async fn plan_sync(store: Arc<NodeStore>, mut inputs: SyncInputs, plans: mpsc::Sender<SyncPlan>) {
    let mut resume = {
        let store = Arc::clone(&store);
        let read = retried(&plans, "its saved anchor", move || store.sync_anchor());
        match read.await {
            Some(anchor) => anchor,
            None => return,
        }
    };
    let mut rest: Option<Rest> = None;
    loop {
        let Some(tip) = archive_tip(&inputs.archive, &plans).await else {
            return;
        };
        let from = tip.map_or(0, |tip| tip.number.saturating_add(1));
        // An unfinished round is finished first.
        let resumed = resume.take().filter(|anchor| anchor.number >= from);
        let anchor = match resumed {
            Some(anchor) => anchor,
            None => {
                match next_anchor(&mut inputs, from, rest.as_ref(), &plans).await {
                    Some(Some(anchor)) => anchor,
                    // Something moved, or a wait ended: look again.
                    Some(None) => continue,
                    None => return,
                }
            }
        };
        let checkpoints = {
            let store = Arc::clone(&store);
            let read = retried(&plans, "its checkpoints", move || {
                store.sync_checkpoints(anchor)
            });
            match read.await {
                Some(checkpoints) => checkpoints,
                None => return,
            }
        };
        info!(
            from,
            to = anchor.number,
            anchor = %anchor.hash,
            resumed = resumed.is_some(),
            "range sync planned: fetching these blocks from execution peers"
        );
        let (ended, end) = oneshot::channel();
        let plan = SyncPlan {
            range: SyncRange { from, anchor },
            checkpoints,
            extends: tip,
            ended,
        };
        // The execution network is gone if this fails: the node is shutting down.
        if plans.send(plan).await.is_err() {
            return;
        }
        match round(&inputs.archive, anchor, end, &plans).await {
            RoundOutcome::Stored => rest = None,
            RoundOutcome::Abandoned => {
                let wait = rest.as_ref().map_or(ABANDONED_ANCHOR_WAIT, |earlier| {
                    earlier
                        .wait
                        .saturating_mul(2)
                        .min(ABANDONED_ANCHOR_MAX_WAIT)
                });
                rest = Some(Rest {
                    until: tokio::time::Instant::now() + wait,
                    wait,
                });
            }
            RoundOutcome::Stop => return,
        }
    }
}

/// The anchor of a round from `from`, if one is needed now (see [`plan_sync`]); otherwise
/// waits for the heads to move or the rest after a round given up to end, and returns
/// `Some(None)` so the archive is looked at again. `None` when the node stops.
///
/// With the L1 side a round is needed while the archive is [`CAUGHT_UP_BLOCKS`] or more below
/// the safe head, or below the committed safe block, and is anchored on the safe head only:
/// everything the sync writes is then committed on L1, so no reorg can leave it behind.
/// Without it a round is needed while the archive is that far below the gossiped head, and is
/// anchored on the block [`ANCHOR_DEPTH`] below it: an unsafe reorg deeper than that would
/// leave the archive on a dead branch, which only rebuilding the archive repairs.
async fn next_anchor(
    inputs: &mut SyncInputs,
    from: BlockNumber,
    rest: Option<&Rest>,
    plans: &mpsc::Sender<SyncPlan>,
) -> Option<Option<BlockRef>> {
    let tip = from.checked_sub(1);
    let safe = inputs.l1_heads.borrow_and_update().safe;
    let head = *inputs.gossip_head.borrow_and_update();
    let committed = *inputs.committed.borrow_and_update();
    let far = |number: BlockNumber| number.saturating_sub(from) >= CAUGHT_UP_BLOCKS;
    let resting = rest
        .map(|rest| rest.until)
        .filter(|until| *until > tokio::time::Instant::now());
    let anchor = if resting.is_some() {
        None
    } else if inputs.l1 {
        let behind_committed = tip.is_some_and(|tip| tip < committed);
        safe.filter(|safe| safe.number >= from && (far(safe.number) || behind_committed))
    } else {
        match head.filter(|head| far(head.number)) {
            Some(head) => below_head(&inputs.unsafe_store, head).await,
            None => None,
        }
    };
    if anchor.is_some() {
        return Some(anchor);
    }
    let rest = async {
        match resting {
            Some(until) => tokio::time::sleep_until(until).await,
            None => std::future::pending().await,
        }
    };
    // A closed channel is the node stopping.
    tokio::select! {
        () = plans.closed() => return None,
        changed = inputs.l1_heads.changed() => changed.ok()?,
        changed = inputs.gossip_head.changed() => changed.ok()?,
        changed = inputs.committed.changed() => changed.ok()?,
        () = rest => {}
    }
    Some(None)
}

/// The gossiped block [`ANCHOR_DEPTH`] below `head`, if the unsafe store holds the chain that
/// far down (it does not right after a start: then the next head is tried).
async fn below_head(unsafe_store: &RedisStore, head: BlockRef) -> Option<BlockRef> {
    let stop_at = head.number.checked_sub(ANCHOR_DEPTH.saturating_add(1))?;
    match unsafe_store.ancestry(head, stop_at).await {
        Ok(blocks) => blocks.first().map(|block| BlockRef {
            number: block.block.header.number,
            hash: block.hash,
        }),
        Err(err) => {
            debug!(%err, head = head.number, "no range sync anchor below this head yet");
            None
        }
    }
}

/// Waits until the archive holds `anchor`, or the execution network gives the round up
/// (`end`).
async fn round(
    archive: &FjallArchive,
    anchor: BlockRef,
    mut end: oneshot::Receiver<RoundEnd>,
    plans: &mpsc::Sender<SyncPlan>,
) -> RoundOutcome {
    let mut fetching = true;
    // Set once the round is fetched: the pipeline has that long to store it.
    let mut fetched_at: Option<tokio::time::Instant> = None;
    loop {
        let Some(tip) = archive_tip(archive, plans).await else {
            return RoundOutcome::Stop;
        };
        if tip.is_some_and(|tip| tip.number >= anchor.number) {
            return RoundOutcome::Stored;
        }
        if fetched_at.is_some_and(|at| at.elapsed() >= ROUND_STORE_TIMEOUT) {
            warn!(
                anchor = anchor.number,
                archive_tip = ?tip,
                "range sync round fetched but not stored: its blocks do not extend the archive"
            );
            return RoundOutcome::Abandoned;
        }
        tokio::select! {
            () = plans.closed() => return RoundOutcome::Stop,
            ended = &mut end, if fetching => {
                fetching = false;
                match ended {
                    Ok(RoundEnd::AnchorUnavailable | RoundEnd::NotLinked) => {
                        return RoundOutcome::Abandoned;
                    }
                    // The pipeline is storing the last batches.
                    Ok(RoundEnd::Complete) => fetched_at = Some(tokio::time::Instant::now()),
                    // The network is stopping, which `plans.closed()` shows.
                    Err(_) => {}
                }
            }
            () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
        }
    }
}

/// Hands the L1 heads to promotion. With the range sync on (`gate`: the archive and the
/// committed safe block's number), a head is held while the archive is more than
/// [`CAUGHT_UP_BLOCKS`] below its safe block, or below the committed safe block: promotion
/// could not read the gap from the unsafe store, and with the L1 side a sync round closes it.
/// The hold is looked at again every [`SYNC_POLL_INTERVAL`] against the archive as it grows,
/// so a head can be released while a round is still storing: the range task then leaves out
/// the blocks promotion appended first. Otherwise, or if
/// the archive cannot be read, heads go straight to promotion. The finalized head is held
/// with the safe one: promotion takes them together. Ends when the pipeline or the L1 source
/// is gone.
async fn forward_l1_heads(
    mut heads: watch::Receiver<L1Heads>,
    promotion: watch::Sender<L1Heads>,
    gate: Option<(FjallArchive, watch::Receiver<BlockNumber>)>,
) {
    loop {
        let current = *heads.borrow_and_update();
        let held = match (&gate, current.safe) {
            (Some((archive, committed)), Some(safe)) => match archive.range().await {
                Ok(range) => {
                    let tip = range.map_or(0, |(_, tip)| tip.number);
                    tip.saturating_add(CAUGHT_UP_BLOCKS) < safe.number || tip < *committed.borrow()
                }
                Err(err) => {
                    debug!(%err, "cannot read the archive; the L1 heads go to promotion");
                    false
                }
            },
            _ => false,
        };
        if !held {
            promotion.send_if_modified(|sent| {
                let changed = *sent != current;
                *sent = current;
                changed
            });
        }
        tokio::select! {
            () = promotion.closed() => return,
            changed = heads.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = tokio::time::sleep(SYNC_POLL_INTERVAL), if held => {}
        }
    }
}

/// The archive's last block, `Some(None)` for an empty archive. A failing read is retried
/// every [`SYNC_POLL_INTERVAL`] and warned about once per [`RETRY_WARN_INTERVAL`]. `None`
/// when the node stops (`plans` closes).
async fn archive_tip(
    archive: &FjallArchive,
    plans: &mpsc::Sender<SyncPlan>,
) -> Option<Option<BlockRef>> {
    let mut warned: Option<tokio::time::Instant> = None;
    loop {
        match archive.range().await {
            Ok(range) => return Some(range.map(|(_, tip)| tip)),
            Err(err) => {
                if warned.is_none_or(|at| at.elapsed() >= RETRY_WARN_INTERVAL) {
                    warned = Some(tokio::time::Instant::now());
                    warn!(%err, "range sync: the block archive cannot be read; retrying");
                }
            }
        }
        tokio::select! {
            () = plans.closed() => return None,
            () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
        }
    }
}

/// Runs the node-store read `read` on a blocking thread until it succeeds, retried like
/// [`archive_tip`]. `None` when the node stops.
async fn retried<T, F>(plans: &mpsc::Sender<SyncPlan>, what: &'static str, read: F) -> Option<T>
where
    T: Send + 'static,
    F: Fn() -> Result<T, StoreError> + Clone + Send + 'static,
{
    let mut warned: Option<tokio::time::Instant> = None;
    loop {
        let attempt = tokio::task::spawn_blocking(read.clone()).await;
        let err = match attempt {
            Ok(Ok(value)) => return Some(value),
            Ok(Err(err)) => err.to_string(),
            Err(err) => err.to_string(),
        };
        if warned.is_none_or(|at| at.elapsed() >= RETRY_WARN_INTERVAL) {
            warned = Some(tokio::time::Instant::now());
            warn!(%err, what, "range sync: cannot read the node store; retrying");
        }
        tokio::select! {
            () = plans.closed() => return None,
            () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
        }
    }
}

/// Saves the peers of one execution network that served us, with `save`, so the next run
/// dials them first. Ends when that network drops its sender.
async fn save_peers(
    store: Arc<NodeStore>,
    mut served: mpsc::Receiver<ExecutionPeer>,
    save: fn(&NodeStore, &ExecutionPeer) -> Result<(), StoreError>,
) {
    while let Some(peer) = served.recv().await {
        let store = Arc::clone(&store);
        match tokio::task::spawn_blocking(move || save(&store, &peer)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to save execution peer"),
            Err(err) => warn!(%err, "execution peer save task failed"),
        }
    }
}

/// Saves the checkpoints the range sync verifies, so a restart does not walk the header
/// chain again. Ends when the execution network drops its sender.
async fn save_sync_checkpoints(store: Arc<NodeStore>, mut verified: mpsc::Receiver<Vec<BlockRef>>) {
    while let Some(checkpoints) = verified.recv().await {
        let store = Arc::clone(&store);
        let save = tokio::task::spawn_blocking(move || store.save_sync_checkpoints(&checkpoints));
        match save.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to save range sync checkpoints"),
            Err(err) => warn!(%err, "range sync checkpoint save task failed"),
        }
    }
}

/// The two stores, connected and ready.
struct Stores {
    unsafe_store: RedisStore,
    /// The block archive, the committed store.
    archive: FjallArchive,
    /// The archive's first and last block at startup; `None` when it is empty.
    archive_range: Option<(BlockRef, BlockRef)>,
}

/// Opens the block archive, checking that it holds the configured chain. Startup-only
/// blocking I/O, like the node store.
fn open_archive(config: &StorageConfig) -> eyre::Result<FjallArchive> {
    FjallArchive::open(&config.archive.path, config.chain)
        .wrap_err("failed to open the block archive")
}

/// Reads the range of `archive` (opened by [`open_archive`]), connects to Redis and runs its
/// schema check, and returns the stores once both are ready, so an unreachable or mismatched
/// store stops startup.
async fn prepare_storage(config: &StorageConfig, archive: FjallArchive) -> eyre::Result<Stores> {
    op_indexer_storage::metrics::describe();
    let archive_range = archive
        .range()
        .await
        .wrap_err("failed to read the block archive")?;
    info!(range = ?archive_range, "block archive ready");
    let unsafe_store = RedisStore::connect(&config.redis, config.chain.chain_id)
        .await
        .wrap_err("failed to connect to Redis")?;
    info!(?config, "storage ready");
    Ok(Stores {
        unsafe_store,
        archive,
        archive_range,
    })
}

/// Resolves with the signal's name on Ctrl-C (SIGINT) or, on Unix, SIGTERM, which `docker stop`
/// sends before killing the process.
async fn shutdown_signal() -> eyre::Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).wrap_err("failed to listen for SIGTERM")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.wrap_err("failed to listen for Ctrl-C")?;
                Ok("SIGINT")
            }
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .wrap_err("failed to listen for Ctrl-C")?;
        Ok("SIGINT")
    }
}
