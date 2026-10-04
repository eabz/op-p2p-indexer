//! Indexer for the OP Stack peer-to-peer network.
//!
//! Wires the components together: loads config and the node identity, checks that Redis and
//! ClickHouse are reachable and their schemas current, opens the local block archive when it is
//! enabled, and runs the p2p network next to the pipeline that stores the blocks it emits. When
//! the execution network is enabled it runs too: it fetches the receipts the pipeline asks
//! for and serves the archive's blocks to peers, and, when a range sync is configured,
//! fetches that range from peers for the pipeline to store. Shuts down cleanly on Ctrl-C or SIGTERM: the networks first, then the pipeline, which
//! stores what they had already delivered.

mod config;
mod provider;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::BlockNumber;
use eyre::WrapErr;
use op_indexer_chainspec::ChainSpec;
use op_indexer_el::{ExecutionNetwork, RangeSync, SyncPlan};
use op_indexer_l1::{BeaconConfig, L1Config, L1Network, LightClient};
use op_indexer_p2p::{Network, NodeStore, StoreError};
use op_indexer_pipeline::{Pipeline, ReceiptsChannels};
use op_indexer_primitives::{BlockRef, EncodedBlock, ExecutionPeer, L1Games, L1Heads, SyncRange};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::committed_store::ClickHouseStore;
use op_indexer_storage::unsafe_store::RedisStore;
use op_indexer_storage::{ArchiveRetention, ArchiveStore, StorageConfig};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::{Config, ElSettings, L1Settings};
use crate::provider::ArchiveProvider;

/// Unsafe blocks waiting for the pipeline. Blocks arrive every ~2s; this absorbs a store that
/// is unreachable for several minutes before the network starts dropping them.
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
/// How close to the unsafe head a round of the range sync has to end for the archive to
/// count as caught up. Promotion reads at most 4,096 blocks back from a safe head, and the
/// first one it appends must be at or below the archive's last block: this leaves room for
/// the safe head to be a few thousand blocks further on when it arrives.
const CAUGHT_UP_BLOCKS: u64 = 1024;
/// How often the archive is asked whether a round of the range sync has reached its end.
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
    let stores = prepare_storage(&config.storage).await?;
    let store =
        NodeStore::open(config.data_dir.join(NODE_DIR)).wrap_err("failed to open node store")?;
    let keypair = store.identity().wrap_err("failed to load node identity")?;
    let store = Arc::new(store);

    let (blocks_tx, blocks_rx) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
    // Nothing produces the L1 heads until the L1 crate exists (docs/roadmap.md); the sender is
    // kept so the pipeline sees a quiet source, not a closed one.
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
    // Whether promotion may act: at once, unless a range sync has to bring the archive up to
    // the chain first. Promotion only appends blocks that extend the archive's last one, so
    // if it ran ahead of the sync the archive would stop where the sync ends.
    let sync_archive = stores
        .archive
        .as_ref()
        .filter(|_| config.sync)
        .map(|(archive, _)| archive.clone());
    let (caught_up_tx, caught_up_rx) = watch::channel(sync_archive.is_none());

    let execution = config
        .el
        .map(|el| {
            let sync = sync_archive.map(|archive| SyncInputs {
                archive,
                gossip_head,
                caught_up: caught_up_tx,
            });
            execution_network(el, sync, &store, &stores, head_rx)
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

    let pipeline = Pipeline::new(
        stores.unsafe_store,
        stores.committed,
        stores.archive,
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
    saves.push(tokio::spawn(forward_l1_heads(
        l1_source_rx,
        caught_up_rx,
        l1_heads_tx,
    )));
    // The L1 side, whose games the pipeline turns into the heads; without it nothing writes
    // them and the sender is only kept alive, so promotion sees a quiet source, not a closed
    // one.
    let (l1, pipeline, _idle) = match config.l1 {
        Some(settings) => {
            let l1 = l1_side(settings, config.network.chain, &store)?;
            saves.push(l1.served);
            let pipeline = pipeline.with_l1_games(l1.games, l1_source_tx);
            (Some((l1.network, l1.light_client)), pipeline, None)
        }
        None => (None, pipeline, Some(l1_source_tx)),
    };
    let network = Network::new(config.network, keypair, store, blocks_tx, safe_number_rx);
    run(network, execution, l1, pipeline, saves).await
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
            bootnodes: chain
                .bootnodes
                .iter()
                .map(|bootnode| (*bootnode).to_owned())
                .collect(),
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
    execution: Option<ExecutionNetwork<ArchiveProvider>>,
    l1: Option<(L1Network, LightClient)>,
    pipeline: Pipeline<RedisStore, ClickHouseStore, FjallArchive>,
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
    let mut pipeline = Some(tokio::spawn(pipeline.run(cancel.clone())));

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
    };

    networks_cancel.cancel();
    let network = join(network).await.wrap_err("network failed");
    let execution = join(execution).await.wrap_err("execution network failed");
    let l1 = join(l1).await.wrap_err("L1 network failed");
    let light_client = join(light_client)
        .await
        .wrap_err("beacon light client failed");
    cancel.cancel();
    let pipeline = join(pipeline).await.wrap_err("pipeline failed");
    // Their senders are gone with the execution network, so they end.
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
        .and(pipeline)
}

/// Waits for `task` to finish and clears it, so it is not awaited again. Never resolves when
/// there is no task, which lets a disabled component sit in a `select!`.
async fn finished<E>(task: &mut Option<JoinHandle<Result<(), E>>>) -> eyre::Result<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let Some(handle) = task else {
        return std::future::pending().await;
    };
    let result = handle.await;
    *task = None;
    Ok(result.wrap_err("task panicked")??)
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
    network: ExecutionNetwork<ArchiveProvider>,
    /// The pipeline's ends of the receipts channels.
    receipts: ReceiptsChannels,
    /// The batches of the range sync, for the pipeline, when a range is configured.
    range: Option<mpsc::Receiver<Vec<EncodedBlock>>>,
    /// Tasks that save what the network reports to the node store.
    saves: Vec<JoinHandle<()>>,
}

/// Builds the execution network from its settings and what the node store has saved for it.
/// Peers are served from the archive; without one the node serves nothing. `head` is the
/// newest block the node knows. With `sync`, the blocks between the archive and the chain's
/// head are fetched from peers.
fn execution_network(
    el: ElSettings,
    sync: Option<SyncInputs>,
    store: &Arc<NodeStore>,
    stores: &Stores,
    head: watch::Receiver<Option<BlockRef>>,
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
    let provider = stores
        .archive
        .as_ref()
        .map(|(archive, _)| ArchiveProvider(archive.clone()));
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
            saves.push(tokio::spawn(plan_sync(Arc::clone(store), inputs, plans_tx)));
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
    /// The unsafe head; its first change is the first head gossip delivers in this run.
    gossip_head: watch::Receiver<Option<BlockRef>>,
    /// Set once the archive has caught up with the chain: promotion may then act.
    caught_up: watch::Sender<bool>,
}

/// Plans the range sync, round by round, and hands each plan to the execution network: from
/// the block after the archive's last one up to an anchor, a block whose hash is trusted
/// because the sequencer signed it.
///
/// The first anchor is the one an unfinished sync of an earlier run was working towards, so
/// its verified checkpoints are kept; otherwise it is the unsafe head gossip delivered. A
/// round that ends more than [`CAUGHT_UP_BLOCKS`] behind the head is followed by another to
/// the head of that moment; rounds get shorter each time. `caught_up` is set when a round to
/// a head of this run ends within that distance: from that block on the unsafe store holds
/// the chain, so promotion can extend the archive.
///
/// Ends, and with it the sync, when it has caught up or the node stops.
async fn plan_sync(store: Arc<NodeStore>, inputs: SyncInputs, plans: mpsc::Sender<SyncPlan>) {
    let SyncInputs {
        archive,
        mut gossip_head,
        caught_up,
    } = inputs;
    let mut resume = {
        let store = Arc::clone(&store);
        match tokio::task::spawn_blocking(move || store.sync_anchor()).await {
            Ok(Ok(anchor)) => anchor,
            Ok(Err(err)) => return warn!(%err, "range sync not started: cannot read its anchor"),
            Err(err) => return warn!(%err, "range sync not started: reading its anchor failed"),
        }
    };
    let mut gossip_seen = false;
    loop {
        let Some(from) = archive_next(&archive).await else {
            return;
        };
        // An unfinished sync is finished first; its anchor is not a head of this run.
        let unfinished = resume.take().filter(|anchor| anchor.number >= from);
        let anchor = if let Some(anchor) = unfinished {
            anchor
        } else {
            // A closed channel is the pipeline stopping: the node is shutting down.
            if !gossip_seen && gossip_head.changed().await.is_err() {
                return;
            }
            gossip_seen = true;
            let Some(head) = *gossip_head.borrow_and_update() else {
                continue;
            };
            head
        };
        if anchor.number >= from {
            let checkpoints = {
                let store = Arc::clone(&store);
                tokio::task::spawn_blocking(move || store.sync_checkpoints(anchor)).await
            };
            let checkpoints = match checkpoints {
                Ok(Ok(checkpoints)) => checkpoints,
                Ok(Err(err)) => return warn!(%err, "range sync stopped: cannot read checkpoints"),
                Err(err) => return warn!(%err, "range sync stopped: reading checkpoints failed"),
            };
            info!(
                from,
                to = anchor.number,
                anchor = %anchor.hash,
                resumed = unfinished.is_some(),
                "range sync planned: fetching these blocks from execution peers"
            );
            let plan = SyncPlan {
                range: SyncRange { from, anchor },
                checkpoints,
            };
            // The execution network is gone if this fails: the node is shutting down.
            if plans.send(plan).await.is_err() {
                return;
            }
            // The round is over when the archive holds its last block.
            while archive_next(&archive)
                .await
                .is_some_and(|next| next <= anchor.number)
            {
                tokio::select! {
                    () = plans.closed() => return,
                    () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
                }
            }
        }
        // Only a round to a head of this run leaves the archive at a block the unsafe store
        // holds the chain from.
        if unfinished.is_some() {
            continue;
        }
        let head = gossip_head
            .borrow()
            .map_or(anchor.number, |head| head.number);
        let behind = head.saturating_sub(anchor.number);
        if behind <= CAUGHT_UP_BLOCKS {
            info!(
                archive_tip = anchor.number.max(from.saturating_sub(1)),
                head, "range sync done: the archive has caught up with the chain"
            );
            caught_up.send_replace(true);
            return;
        }
    }
}

/// Hands the L1 heads to promotion, from the moment the archive has caught up with the chain
/// (`caught_up`; at once when no range sync runs). Until then promotion sees no heads and
/// promotes nothing, so the unsafe store keeps the blocks the archive still has to connect
/// to. Ends when the pipeline or the L1 source is gone, or the sync ended without catching
/// up.
async fn forward_l1_heads(
    mut heads: watch::Receiver<L1Heads>,
    mut caught_up: watch::Receiver<bool>,
    promotion: watch::Sender<L1Heads>,
) {
    tokio::select! {
        () = promotion.closed() => return,
        opened = caught_up.wait_for(|caught_up| *caught_up) => {
            if opened.is_err() {
                return;
            }
        }
    }
    loop {
        promotion.send_replace(*heads.borrow_and_update());
        tokio::select! {
            () = promotion.closed() => return,
            changed = heads.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// The block after the archive's last one, 0 for an empty archive; `None`, logged, if the
/// archive cannot be read.
async fn archive_next(archive: &FjallArchive) -> Option<BlockNumber> {
    match archive.range().await {
        Ok(range) => Some(range.map_or(0, |(_, tip)| tip.number.saturating_add(1))),
        Err(err) => {
            warn!(%err, "range sync stopped: the block archive cannot be read");
            None
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

/// The three stores, connected and ready.
struct Stores {
    unsafe_store: RedisStore,
    committed: ClickHouseStore,
    /// The local block archive with how much it keeps; `None` when it is disabled.
    archive: Option<(FjallArchive, ArchiveRetention)>,
    /// The archive's first and last block at startup; `None` when it is empty or disabled.
    archive_range: Option<(BlockRef, BlockRef)>,
}

/// Connects to both stores, runs the Redis schema check and the ClickHouse migrations, opens
/// the local block archive when it is enabled, and returns the stores once all are ready, so
/// an unreachable or mismatched store stops startup.
async fn prepare_storage(config: &StorageConfig) -> eyre::Result<Stores> {
    op_indexer_storage::metrics::describe();
    let unsafe_store = RedisStore::connect(&config.redis, config.chain_id)
        .await
        .wrap_err("failed to connect to Redis")?;
    let committed = ClickHouseStore::new(&config.clickhouse, config.chain_id);
    committed
        .ping()
        .await
        .wrap_err("failed to reach ClickHouse")?;
    committed
        .migrate()
        .await
        .wrap_err("failed to migrate ClickHouse")?;
    info!(?config, "storage ready");

    let archive = match &config.archive {
        Some(archive) => {
            // Startup-only blocking I/O, like the node store.
            let store =
                FjallArchive::open(&archive.path).wrap_err("failed to open the block archive")?;
            let range = store
                .range()
                .await
                .wrap_err("failed to read the block archive")?;
            info!(?range, retention = ?archive.retention, "block archive ready");
            Some((store, archive.retention, range))
        }
        None => None,
    };
    let archive_range = archive.as_ref().and_then(|(_, _, range)| *range);
    Ok(Stores {
        unsafe_store,
        committed,
        archive: archive.map(|(store, retention, _)| (store, retention)),
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
