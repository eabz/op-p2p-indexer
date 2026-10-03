//! Indexer for the OP Stack peer-to-peer network.
//!
//! Wires the components together: loads config and the node identity, runs the p2p network,
//! and consumes the unsafe blocks it emits. Shuts down cleanly on Ctrl-C or SIGTERM.

mod config;

use std::sync::Arc;

use eyre::WrapErr;
use op_indexer_p2p::{Network, NodeStore};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::Config;

/// Unsafe blocks waiting for the consumer. Blocks arrive every ~2s; this absorbs long stalls.
const BLOCK_CHANNEL_CAPACITY: usize = 256;

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
    let store =
        NodeStore::open(config.data_dir.join("node.redb")).wrap_err("failed to open node store")?;
    let keypair = store.identity().wrap_err("failed to load node identity")?;

    let (blocks_tx, mut blocks_rx) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    // L2 safe head: nothing feeds it yet, so every gap counts as unsafe.
    // TODO: drive it from the L1 / reth integration crate once it exists.
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
            // TODO: hand blocks to the pipeline (hot storage) once it exists.
            Some(_block) = blocks_rx.recv() => {}
        }
    }
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
