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
use std::time::Instant;

use eyre::WrapErr;
use op_indexer_balancer::register::{HEARTBEAT_INTERVAL, PeerReport, Report, SlotReport};
use op_indexer_chunks::ChunkStore;
use op_indexer_node::{Config, NodeView, PeerCounts, Task};
use op_indexer_server::{ChunkSource, Exporter, R2Archive, R2Chunks};
use op_indexer_storage::ArchiveStore;
use op_indexer_storage::archive_store::FjallArchive;
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::config::{Chunks, ServerConfig};

/// Log timestamp: UTC time of day with milliseconds, e.g. `13:04:12.345`.
const LOG_TIME_FORMAT: &str = "%H:%M:%S%.3f";
/// Directory of the hash-index builder's spill files, inside the data directory.
const INDEX_DIR: &str = "index-build";
/// Directory of the tail, inside the data directory. Not the indexer's `archive`: the tail
/// drops every block the manifest covers, which on an indexer's archive is its history.
const TAIL_DIR: &str = "tail";

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
    // An indexer's data directory: its archive must not be touched, nor its identity shared.
    eyre::ensure!(
        !storage
            .archive
            .path
            .try_exists()
            .wrap_err("failed to look for an indexer's archive")?,
        "{} holds an indexer's block archive: give the server a data directory of its own \
         (OP_INDEXER_DATA_DIR)",
        config.data_dir().display()
    );
    let tail = FjallArchive::open(&config.data_dir().join(TAIL_DIR), storage.chain)
        .wrap_err("failed to open the tail")?;
    let store = match &server.chunks {
        Chunks::R2(r2) => ChunkStore::r2(r2, chain, server.read),
        Chunks::Local { dir, prefix } => ChunkStore::local(dir, prefix, chain, server.read),
    }
    .wrap_err("failed to set up the chunk store")?;
    let source = R2Chunks::open(store, config.data_dir().join(INDEX_DIR))
        .await
        .wrap_err("failed to read the R2 manifest")?;
    let archive = R2Archive::open(chain, source, tail, server.read_budget)
        .await
        .wrap_err("failed to open the R2 archive")?;

    let mut tasks: Vec<(&'static str, Task)> = Vec::new();
    let follower = archive.clone();
    tasks.push((
        "manifest follower",
        Box::new(move |_view, cancel| Box::pin(async move { Ok(follower.follow(cancel).await?) })),
    ));
    if let Some(exporter_id) = server.export {
        let exporter = Exporter::new(archive.clone(), exporter_id);
        tasks.push((
            "exporter",
            Box::new(move |_view, cancel| Box::pin(async move { Ok(exporter.run(cancel).await?) })),
        ));
    }
    if let Some(registration) = server.balancer {
        let (report_tx, report_rx) = watch::channel(Report::default());
        let reported = archive.clone();
        tasks.push((
            "balancer registration",
            Box::new(move |view, cancel| {
                Box::pin(async move {
                    // Registration fails only before it connects (a bad URL or key): then
                    // at once, which stops the node. Reporting ends with `cancel`.
                    tokio::select! {
                        registered = registration.run(report_rx, cancel.clone()) => Ok(registered?),
                        () = report(&reported, &view, &report_tx, &cancel) => Ok(()),
                    }
                })
            }),
        ));
    }
    op_indexer_node::run(config, archive, tasks).await
}

/// Keeps the report the balancer gets current until `cancel` fires: the node's head, the
/// committed store's L1 heads and last sealed block, how far it holds every block, the
/// stream's load and the networks' peers, read once per heartbeat. A store that cannot be
/// read makes the server report itself unhealthy.
async fn report<S: ChunkSource>(
    archive: &R2Archive<S>,
    view: &NodeView,
    sender: &watch::Sender<Report>,
    cancel: &tokio_util::sync::CancellationToken,
) {
    let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The bytes sent as of the last report, and when: the rate is the change since.
    let mut sent = (Instant::now(), view.load.bytes_sent());
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        let heads = archive.heads().await;
        let contiguous = view.contiguous_through(archive).await;
        let healthy = heads.is_ok() && contiguous.is_ok();
        let heads = heads.unwrap_or_default();
        let contiguous = contiguous.unwrap_or_default();
        let last_sealed = archive.last_sealed();
        let now = (Instant::now(), view.load.bytes_sent());
        let bytes_per_second = rate(sent, now);
        sent = now;
        sender.send_replace(Report {
            healthy,
            unsafe_head: view.head.borrow().map(|head| head.number),
            safe_head: heads.safe.map(|safe| safe.number),
            finalized_head: heads.finalized.map(|finalized| finalized.number),
            last_sealed,
            // The tail can trail the manifest a moment, before it drops what was sealed.
            contiguous_through: contiguous.max(last_sealed),
            requests_in_flight: view.load.in_flight(),
            bytes_per_second,
            peers: peer_report(view.peers()),
            slots: slot_report(view),
        });
    }
}

/// Bytes per second between two readings of the bytes sent, each with when it was taken.
fn rate((then, before): (Instant, u64), (now, after): (Instant, u64)) -> u64 {
    let millis = now.duration_since(then).as_millis().max(1);
    let bytes = u128::from(after.saturating_sub(before));
    u64::try_from(bytes.saturating_mul(1000) / millis).unwrap_or(u64::MAX)
}

/// The stream's limits and the places taken, as the heartbeat carries them.
fn slot_report(view: &NodeView) -> SlotReport {
    let (flights, subscriptions) = (view.load.flights(), view.load.subscriptions());
    SlotReport {
        max_flights: Some(count(flights.max)),
        flights_in_use: Some(count(flights.in_use)),
        max_subscriptions: Some(count(subscriptions.max)),
        subscriptions_in_use: Some(count(subscriptions.in_use)),
    }
}

/// The node's peer counts as the heartbeat carries them.
fn peer_report(peers: PeerCounts) -> PeerReport {
    PeerReport {
        consensus_peers: Some(count(peers.consensus)),
        execution_sessions: peers.execution.map(|sessions| count(sessions.total)),
        execution_inbound: peers.execution.map(|sessions| count(sessions.inbound)),
        l1_sessions: peers.l1_execution.map(count),
        beacon_peers: peers.beacon.map(count),
    }
}

/// A count as the heartbeat carries it, saturating.
fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}
