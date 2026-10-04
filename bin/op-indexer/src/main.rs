//! Indexer for the OP Stack peer-to-peer network.
//!
//! Wires the components together: loads config and the node identity, checks that Redis and
//! ClickHouse are reachable and their schemas current, opens the local block archive when it is
//! enabled, and runs the p2p network next to the pipeline that stores the blocks it emits. When
//! the execution network is enabled it runs too, and fetches the receipts the pipeline asks
//! for. Shuts down cleanly on Ctrl-C or SIGTERM: the networks first, then the pipeline, which
//! stores what they had already delivered.

mod config;

use std::sync::Arc;

use eyre::WrapErr;
use op_indexer_el::ExecutionNetwork;
use op_indexer_p2p::{Network, NodeStore};
use op_indexer_pipeline::{Pipeline, ReceiptsChannels};
use op_indexer_primitives::{ExecutionPeer, L1Heads};
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

use crate::config::Config;

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
    let (_l1_heads_tx, l1_heads_rx) = watch::channel(L1Heads::default());
    // The pipeline publishes the safe block number once its blocks are committed; the network
    // ignores gaps at or below it.
    let (safe_number_tx, safe_number_rx) = watch::channel(0);

    // The execution network and the pipeline's ends of its two channels, when it is enabled.
    let (execution, receipts, peer_saves) = match config.el {
        Some(mut el) => {
            // A key of its own: the two networks must not share a node id.
            let key = store
                .execution_key()
                .wrap_err("failed to load the execution network key")?;
            let (requests_tx, requests_rx) = mpsc::channel(RECEIPT_REQUEST_CAPACITY);
            let (verified_tx, verified_rx) = mpsc::channel(VERIFIED_RECEIPTS_CAPACITY);
            el.saved_peers = store
                .execution_peers()
                .wrap_err("failed to load the saved execution peers")?;
            let (served_tx, served_rx) = mpsc::channel(SERVED_PEERS_CAPACITY);
            let execution = ExecutionNetwork::new(el, key, requests_rx, verified_tx, served_tx)
                .wrap_err("failed to create the execution network")?;
            let peer_saves = tokio::spawn(save_execution_peers(Arc::clone(&store), served_rx));
            let channels = ReceiptsChannels {
                requests: requests_tx,
                verified: verified_rx,
            };
            (Some(execution), Some(channels), Some(peer_saves))
        }
        None => (None, None, None),
    };

    let pipeline = Pipeline::new(
        stores.unsafe_store,
        stores.committed,
        stores.archive,
        blocks_rx,
        l1_heads_rx,
        safe_number_tx,
        receipts,
    );
    let network = Network::new(config.network, keypair, store, blocks_tx, safe_number_rx);

    // The networks stop first, on their own token, so the pipeline can still store what they
    // delivered; cancelling `cancel` stops everything.
    let cancel = CancellationToken::new();
    let networks_cancel = cancel.child_token();
    let mut network = Some(tokio::spawn(network.run(networks_cancel.clone())));
    let mut execution =
        execution.map(|execution| tokio::spawn(execution.run(networks_cancel.clone())));
    let mut pipeline = Some(tokio::spawn(pipeline.run(cancel.clone())));

    // Any component stopping ends the process; the others are stopped and waited for.
    let stopped = tokio::select! {
        signal = shutdown_signal() => {
            info!(signal = signal?, "shutting down");
            Ok(())
        }
        result = finished(&mut network) => {
            warn!("network stopped unexpectedly");
            result.wrap_err("network failed")
        }
        result = finished(&mut execution) => {
            warn!("execution network stopped unexpectedly");
            result.wrap_err("execution network failed")
        }
        result = finished(&mut pipeline) => {
            warn!("pipeline stopped unexpectedly");
            result.wrap_err("pipeline failed")
        }
    };

    networks_cancel.cancel();
    let network = join(network).await.wrap_err("network failed");
    let execution = join(execution).await.wrap_err("execution network failed");
    // The execution network dropped its sender when it returned, so the saves end.
    if let Some(peer_saves) = peer_saves
        && let Err(err) = peer_saves.await
    {
        warn!(%err, "execution peer save task failed");
    }
    cancel.cancel();
    let pipeline = join(pipeline).await.wrap_err("pipeline failed");
    // The reason the process stopped comes first, then whatever failed while shutting down.
    stopped.and(network).and(execution).and(pipeline)
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

/// Saves the execution peers that served us, so the next run dials them first. Ends when the
/// execution network drops its sender.
async fn save_execution_peers(store: Arc<NodeStore>, mut served: mpsc::Receiver<ExecutionPeer>) {
    while let Some(peer) = served.recv().await {
        let store = Arc::clone(&store);
        match tokio::task::spawn_blocking(move || store.save_execution_peer(&peer)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to save execution peer"),
            Err(err) => warn!(%err, "execution peer save task failed"),
        }
    }
}

/// The three stores, connected and ready.
struct Stores {
    unsafe_store: RedisStore,
    committed: ClickHouseStore,
    /// The local block archive with how much it keeps; `None` when it is disabled.
    archive: Option<(FjallArchive, ArchiveRetention)>,
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
            Some((store, archive.retention))
        }
        None => None,
    };
    Ok(Stores {
        unsafe_store,
        committed,
        archive,
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
