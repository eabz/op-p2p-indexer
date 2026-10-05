//! The indexer: the full node for a single user, every service in one process, its history in
//! a local fjall archive.
//!
//! Loads the `.env` file ([`op_indexer_node::env_file`]), sets up tracing, reads the
//! configuration, opens the archive and runs the node ([`op_indexer_node::run`]).

use std::path::PathBuf;

use eyre::WrapErr;
use op_indexer_node::Config;
use op_indexer_storage::archive_store::FjallArchive;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

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

    let config = Config::from_env()?;
    // Startup-only blocking I/O, before any task runs: the archive before the node store, so a
    // data directory of another chain is refused before anything else is written.
    std::fs::create_dir_all(config.data_dir()).wrap_err("failed to create data dir")?;
    let storage = config.storage();
    let archive = FjallArchive::open(&storage.archive.path, storage.chain)
        .wrap_err("failed to open the block archive")?;
    op_indexer_node::run(config, archive, Vec::new()).await
}
