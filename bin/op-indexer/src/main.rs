//! Indexer for the OP Stack peer-to-peer network.
//!
//! Wires the components together: loads config and the node identity, checks that Redis and
//! ClickHouse are reachable and their schemas current, opens the local block archive when it is
//! enabled, and runs the p2p network next to the pipeline that stores the blocks it emits.
//! Shuts down cleanly on Ctrl-C or SIGTERM: the network first, then the pipeline, which stores
//! what the network had already delivered.

mod config;

use std::sync::Arc;

use eyre::WrapErr;
use op_indexer_p2p::{Network, NodeStore};
use op_indexer_pipeline::Pipeline;
use op_indexer_primitives::L1Heads;
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::committed_store::ClickHouseStore;
use op_indexer_storage::unsafe_store::RedisStore;
use op_indexer_storage::{ArchiveRetention, ArchiveStore, StorageConfig};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::Config;

/// Unsafe blocks waiting for the pipeline. Blocks arrive every ~2s; this absorbs a store that
/// is unreachable for several minutes before the network starts dropping them.
const BLOCK_CHANNEL_CAPACITY: usize = 256;

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

    let (blocks_tx, blocks_rx) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
    // Nothing produces the L1 heads until the L1 crate exists (docs/roadmap.md); the sender is
    // kept so the pipeline sees a quiet source, not a closed one.
    let (_l1_heads_tx, l1_heads_rx) = watch::channel(L1Heads::default());
    // The pipeline publishes the safe block number once its blocks are committed; the network
    // ignores gaps at or below it.
    let (safe_number_tx, safe_number_rx) = watch::channel(0);

    let pipeline = Pipeline::new(
        stores.unsafe_store,
        stores.committed,
        stores.archive,
        blocks_rx,
        l1_heads_rx,
        safe_number_tx,
    );
    let network = Network::new(
        config.network,
        keypair,
        Arc::new(store),
        blocks_tx,
        safe_number_rx,
    );

    // The network stops first, on its own token, so the pipeline can still store what it
    // delivered; cancelling `cancel` stops both.
    let cancel = CancellationToken::new();
    let network_cancel = cancel.child_token();
    let mut network = tokio::spawn(network.run(network_cancel.clone()));
    let mut pipeline = tokio::spawn(pipeline.run(cancel.clone()));

    // Either component stopping ends the process; the other is stopped and waited for.
    tokio::select! {
        signal = shutdown_signal() => info!(signal = signal?, "shutting down"),
        result = &mut network => {
            warn!("network stopped unexpectedly");
            cancel.cancel();
            let stopped = result.wrap_err("network task panicked")?.wrap_err("network failed");
            pipeline.await.wrap_err("pipeline task panicked")??;
            return stopped;
        }
        result = &mut pipeline => {
            warn!("pipeline stopped unexpectedly");
            cancel.cancel();
            let stopped = result.wrap_err("pipeline task panicked")?.wrap_err("pipeline failed");
            network.await.wrap_err("network task panicked")??;
            return stopped;
        }
    }

    network_cancel.cancel();
    network.await.wrap_err("network task panicked")??;
    cancel.cancel();
    pipeline.await.wrap_err("pipeline task panicked")??;
    Ok(())
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
