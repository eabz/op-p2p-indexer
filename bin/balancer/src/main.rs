//! The balancer: the directory of a deployment's servers for one chain (`docs/serving.md`
//! section 6). Servers register with it; clients ask it where to read (Flight
//! `GetFlightInfo` and `ListFlights`, gRPC `Locate`) and then read from the servers.
//!
//! Loads the `.env` file ([`op_indexer_node::env_file`]), sets up tracing, reads the
//! configuration, reads the chain's manifest from R2 (read-only) and runs the
//! [`Balancer`] until Ctrl-C or SIGTERM.

mod config;

use std::path::PathBuf;

use eyre::WrapErr;
use op_indexer_balancer::Balancer;
use op_indexer_chunks::{ChunkStore, Manifest};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::BalancerSettings;

/// Log timestamp: UTC time of day with milliseconds, e.g. `13:04:12.345`.
const LOG_TIME_FORMAT: &str = "%H:%M:%S%.3f";

fn main() -> eyre::Result<()> {
    // Before the runtime starts any thread: loading sets environment variables.
    let env_file = op_indexer_node::env_file::load(std::env::args_os().skip(1))?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("failed to start the tokio runtime")?
        .block_on(run(env_file))
}

async fn run(env_file: Option<PathBuf>) -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_timer(ChronoUtc::new(LOG_TIME_FORMAT.to_owned()))
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    if let Some(path) = env_file {
        info!(path = %path.display(), "loaded env file");
    }

    let settings = BalancerSettings::from_env_and_args()?;
    let store = ChunkStore::r2(&settings.r2, settings.chain, settings.read)
        .wrap_err("failed to set up the R2 chunk store")?;
    let manifest = Manifest::load(&store)
        .await
        .wrap_err("failed to read the R2 manifest")?;
    info!(
        chunks = manifest.entries().len(),
        last_block = manifest.last().map(|entry| entry.last),
        "read the manifest"
    );

    let cancel = CancellationToken::new();
    let run = Balancer::new(settings.balancer, settings.chain, store, manifest).run(cancel.clone());
    tokio::pin!(run);
    // Until a signal arrives, or the balancer stops on its own (it could not listen).
    tokio::select! {
        result = &mut run => return result.wrap_err("the balancer failed"),
        signal = op_indexer_node::shutdown_signal() => info!(signal = signal?, "shutting down"),
    }
    cancel.cancel();
    run.await.wrap_err("the balancer failed")
}
