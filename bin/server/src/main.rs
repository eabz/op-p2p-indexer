//! The server: a full node like the indexer (p2p layers, execution and L1 sides, the unsafe
//! chain, stream and Flight, serving peers) that keeps no history. Its committed store is an
//! [`R2Archive`]: sealed chunks read from R2 on demand, and a small local tail of committed
//! blocks not sealed yet (`docs/serving.md` section 5).
//!
//! With `--export` (or `server.export=true`) it is also the deployment's exporter, which seals full chunks from its tail
//! into R2 (section 4). Exactly one server per deployment runs it, with the only R2 write key.
//!
//! Loads the TOML file ([`op_indexer_runtime::config`]), sets up tracing, reads the
//! configuration (the node's, then the server's own), opens the
//! tail and the chunk store, and runs the node ([`op_indexer_node::run`]) with the manifest
//! follower and, when exporting, the exporter next to it.

mod address;
mod config;

use std::path::PathBuf;
use std::time::Instant;

use eyre::WrapErr;
use op_indexer_balancer::register::{
    HEARTBEAT_INTERVAL, PeerReport, Report, ServedReport, SlotReport,
};
use op_indexer_chunks::ChunkStore;
use op_indexer_node::{Config, Defaults, NodeServed, NodeView, PeerCounts, Task, sizing};
use op_indexer_runtime::Startup;
use op_indexer_runtime::machine::Machine;
use op_indexer_server::{ChunkSource, Exporter, R2Archive, R2Chunks};
use op_indexer_storage::ArchiveStore;
use op_indexer_storage::archive_store::FjallArchive;
use tokio::sync::watch;

use crate::config::{Chunks, ServerConfig};

/// Directory of the hash-index builder's spill files, inside the data directory.
const INDEX_DIR: &str = "index-build";
/// Directory of the tail, inside the data directory. Not the indexer's `archive`: the tail
/// drops every block the manifest covers, which on an indexer's archive is its history.
const TAIL_DIR: &str = "tail";

fn main() -> eyre::Result<()> {
    let Startup::Run { config_file } =
        op_indexer_runtime::startup(env!("CARGO_BIN_NAME"), env!("CARGO_PKG_VERSION"))?
    else {
        return Ok(());
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("failed to start the tokio runtime")?
        .block_on(run(config_file))
}

async fn run(config_file: Option<PathBuf>) -> eyre::Result<()> {
    op_indexer_runtime::init_tracing(config_file.as_deref());

    // A server exists to serve: it holds every sealed block and serves peers that sync from
    // it, so it keeps many execution sessions, where an indexer keeps few.
    let config = Config::from_config_with(Defaults {
        el_max_sessions: sizing::server_el_sessions(Machine::get()),
    })?;
    let chain = config.chain();
    let server = ServerConfig::from_config_and_args(chain)?;
    if op_indexer_runtime::config::check_requested() {
        tracing::info!("configuration valid");
        return Ok(());
    }
    tracing::info!(
        read_budget_mib = sizing::mib(server.read_budget),
        "server read budget, sized from the machine or set in TOML"
    );
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
         (server.data_dir)",
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
    if let Some(mut registration) = server.balancer {
        let given = server.address;
        let (report_tx, report_rx) = watch::channel(Report::default());
        let reported = archive.clone();
        let listen = config.stream.listen_addr;
        tasks.push((
            "balancer registration",
            Box::new(move |view, cancel| {
                Box::pin(async move {
                    let Some(address) = address::resolve(given, listen, &view, &cancel).await?
                    else {
                        return Ok(());
                    };
                    registration.address = address;
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
            served: served_report(view.served()),
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

/// What the node served the networks in the last minute, as the heartbeat carries it.
fn served_report(served: NodeServed) -> ServedReport {
    let execution = served.execution;
    ServedReport {
        blocks_forwarded: Some(served.consensus.blocks_forwarded),
        payloads_served: Some(served.consensus.payloads_served),
        execution_requests_served: execution.map(|execution| execution.requests()),
        execution_items_served: execution.map(|execution| execution.items),
        execution_peers_served: execution.map(|execution| execution.peers),
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
