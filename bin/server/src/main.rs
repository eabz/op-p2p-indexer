//! The server: a full node like the indexer (p2p layers, execution and L1 sides, the unsafe
//! chain, stream and Flight, serving peers) that keeps no history. Its committed store is an
//! [`R2Archive`]: sealed chunks read from R2 on demand, and a small local tail of committed
//! blocks not sealed yet (`docs/serving.md` section 5).
//!
//! With `--export` (or `OP_INDEXER_EXPORT=true`) it is also the deployment's exporter, which seals full chunks from its tail
//! into R2 (section 4). Exactly one server per deployment runs it, with the only R2 write key.
//!
//! Loads the `.env` file ([`op_indexer_node::env_file`]), sets up tracing, reads the
//! configuration (the node's, then the server's own), opens the
//! tail and the chunk store, and runs the node ([`op_indexer_node::run`]) with the manifest
//! follower and, when exporting, the exporter next to it.

mod config;

use std::path::PathBuf;

use eyre::WrapErr;
use op_indexer_chunks::ChunkStore;
use op_indexer_node::{Config, Task};
use op_indexer_server::{Exporter, R2Archive, R2Chunks};
use op_indexer_storage::archive_store::FjallArchive;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::ServerConfig;

/// Log timestamp: UTC time of day with milliseconds, e.g. `13:04:12.345`.
const LOG_TIME_FORMAT: &str = "%H:%M:%S%.3f";
/// Directory of the hash-index builder's spill files, inside the data directory.
const INDEX_DIR: &str = "index-build";

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
    let chain = config.chain();
    let server = ServerConfig::from_env_and_args(chain)?;
    // Startup-only blocking I/O, before any task runs: the tail before the node store, so a
    // data directory of another chain is refused before anything else is written.
    std::fs::create_dir_all(config.data_dir()).wrap_err("failed to create data dir")?;
    let storage = config.storage();
    let tail = FjallArchive::open(&storage.archive.path, storage.chain)
        .wrap_err("failed to open the tail")?;
    let store = ChunkStore::r2(&server.r2, chain, server.read)
        .wrap_err("failed to set up the R2 chunk store")?;
    let source = R2Chunks::open(store, config.data_dir().join(INDEX_DIR))
        .await
        .wrap_err("failed to read the R2 manifest")?;
    let archive = R2Archive::open(chain, source, tail)
        .await
        .wrap_err("failed to open the R2 archive")?;

    let mut tasks: Vec<(&'static str, Task)> = Vec::new();
    let follower = archive.clone();
    tasks.push((
        "manifest follower",
        Box::new(move |cancel| Box::pin(async move { Ok(follower.follow(cancel).await?) })),
    ));
    if let Some(exporter_id) = server.export {
        let exporter = Exporter::new(archive.clone(), exporter_id);
        tasks.push((
            "exporter",
            Box::new(move |cancel| Box::pin(async move { Ok(exporter.run(cancel).await?) })),
        ));
    }
    op_indexer_node::run(config, archive, tasks).await
}
