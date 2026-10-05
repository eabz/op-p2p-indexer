//! The indexer: the full node for a single user, every service in one process, its history in
//! a local fjall archive.
//!
//! Sets up tracing, reads the configuration, opens the archive and runs the node
//! ([`op_indexer_node::run`]).

use eyre::WrapErr;
use op_indexer_node::Config;
use op_indexer_storage::archive_store::FjallArchive;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

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
    // Startup-only blocking I/O, before any task runs: the archive before the node store, so a
    // data directory of another chain is refused before anything else is written.
    std::fs::create_dir_all(config.data_dir()).wrap_err("failed to create data dir")?;
    let storage = config.storage();
    let archive = FjallArchive::open(&storage.archive.path, storage.chain)
        .wrap_err("failed to open the block archive")?;
    op_indexer_node::run(config, archive, Vec::new()).await
}
