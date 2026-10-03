//! Indexer for the OP Stack peer-to-peer network.
//!
//! Wires the components together: loads config and the node identity, checks that Redis and
//! ClickHouse are reachable and their schemas current, opens the local block archive when it is
//! enabled, runs the p2p network, and consumes the unsafe blocks it emits. Shuts down cleanly on Ctrl-C or SIGTERM.

mod config;

use std::sync::Arc;

use eyre::WrapErr;
use op_indexer_p2p::{Network, NodeStore};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::committed_store::ClickHouseStore;
use op_indexer_storage::unsafe_store::RedisStore;
use op_indexer_storage::{ArchiveStore, StorageConfig};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::Config;

/// Unsafe blocks waiting for the consumer. Blocks arrive every ~2s; this absorbs long stalls.
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
    prepare_storage(&config.storage).await?;
    let store =
        NodeStore::open(config.data_dir.join(NODE_DIR)).wrap_err("failed to open node store")?;
    let keypair = store.identity().wrap_err("failed to load node identity")?;

    let (blocks_tx, mut blocks_rx) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    // L2 safe head: nothing feeds it yet, so every gap counts as unsafe.
    // TODO: drive it from the L1 crate once it exists (docs/roadmap.md).
    let (_safe_head_tx, safe_head_rx) = watch::channel(0);
    let network = Network::new(
        config.network,
        keypair,
        Arc::new(store),
        blocks_tx,
        safe_head_rx,
    );
    let mut network = tokio::spawn(network.run(cancel.child_token()));

    // Created once, so a signal arriving while other branches run is not missed.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            signal = &mut shutdown => {
                info!(signal = signal?, "shutting down");
                cancel.cancel();
                network.await.wrap_err("network task panicked")??;
                return Ok(());
            }
            result = &mut network => {
                result.wrap_err("network task panicked")??;
                warn!("network stopped unexpectedly");
                return Ok(());
            }
            // TODO: hand blocks to the pipeline (unsafe store) once it exists.
            Some(_block) = blocks_rx.recv() => {}
        }
    }
}

/// Connects to both stores, runs the Redis schema check and the ClickHouse migrations, opens
/// the local block archive when it is enabled, and returns once all are ready, so an
/// unreachable or mismatched store stops startup.
// TODO: keep the stores and hand them to the pipeline once it exists; nothing writes blocks yet.
async fn prepare_storage(config: &StorageConfig) -> eyre::Result<()> {
    op_indexer_storage::metrics::describe();
    RedisStore::connect(&config.redis, config.chain_id)
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

    if let Some(archive) = &config.archive {
        // Startup-only blocking I/O, like the node store.
        let store =
            FjallArchive::open(&archive.path).wrap_err("failed to open the block archive")?;
        let range = store
            .range()
            .await
            .wrap_err("failed to read the block archive")?;
        info!(?range, retention = ?archive.retention, "block archive ready");
    }
    Ok(())
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
