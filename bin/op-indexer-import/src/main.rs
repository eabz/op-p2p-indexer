//! Imports a block range from an external archive into the stores the indexer serves from.
//!
//! ```text
//! archive service ─▶ download ─▶ <state>/raw ─▶ verify ─▶ <state>/verified ─▶ load ─▶ stores
//! ```
//!
//! - `download` ([`mod@download`]) fetches the range in chunks through a [`source::Source`] and
//!   keeps each answer as received. It does nothing else, so a limited request window is spent
//!   on the transfer only.
//! - `verify` ([`mod@verify`]) rebuilds every block's consensus encoding from the downloaded rows
//!   and checks it: header hash, parent links up to a trusted hash, transaction hashes and
//!   root, receipts root, senders. What passes is written as the exact verified bytes
//!   ([`chunk`]).
//! - `load` ([`load`]) appends verified chunks to the local block archive the node serves
//!   from; ClickHouse is written too only when asked.
//!
//! Every step is resumable: a chunk's file exists only when the chunk is complete. The
//! indexer never links this binary and never talks to the archive service. See
//! `docs/import.md`.

mod chunk;
mod cli;
mod deposit;
mod download;
mod load;
mod rows;
mod source;
mod state;
mod transaction;
mod verify;

use clap::Parser;
use eyre::WrapErr;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::cli::{Cli, Command, DownloadArgs, VerifyArgs};
use crate::source::HyperSync;
use crate::state::{Forks, Plan, State};

/// Log timestamp: UTC time of day with milliseconds, as the indexer's.
const LOG_TIME_FORMAT: &str = "%H:%M:%S%.3f";

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_timer(ChronoUtc::new(LOG_TIME_FORMAT.to_owned()))
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    eyre::ensure!(
        cli.range.first_block <= cli.range.last_block,
        "--first-block is above --last-block"
    );
    let plan = Plan {
        first: cli.range.first_block,
        last: cli.range.last_block,
        anchor: cli.range.anchor_hash,
        chunk_blocks: cli.range.chunk_blocks,
        forks: Forks {
            regolith_time: cli.range.regolith_time,
            canyon_time: cli.range.canyon_time,
            isthmus_time: cli.range.isthmus_time,
        },
    };
    // Startup-only blocking I/O, before any task runs.
    let state = State::open(&cli.range.state_dir).wrap_err("failed to create the state dir")?;

    let cancel = CancellationToken::new();
    let signal = tokio::spawn(cancel_on_signal(cancel.clone()));
    let result = match cli.command {
        Command::Download(args) => download(&args, &state, &plan, &cancel).await,
        Command::Verify(args) => verify(&args, &state, &plan, &cancel).await,
        Command::Load(args) => load::run(&args, &state, &plan, &cancel).await,
        Command::Run(args) => {
            let steps = async {
                download(&args.download, &state, &plan, &cancel).await?;
                verify(&args.verify, &state, &plan, &cancel).await?;
                load::run(&args.load, &state, &plan, &cancel).await
            };
            steps.await
        }
    };
    signal.abort();
    result
}

async fn download(
    args: &DownloadArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let source = HyperSync::new(&args.endpoint, &args.api_token)?;
    let requests = usize::try_from(args.requests).wrap_err("--requests is too large")?;
    download::run(source, state, plan, requests, cancel).await
}

async fn verify(
    args: &VerifyArgs,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let threads = args
        .verify_threads
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1)
        .max(1);
    verify::run(state, plan, threads, cancel).await
}

/// Cancels `cancel` on Ctrl-C or, on Unix, SIGTERM, so the running step stops between chunks.
async fn cancel_on_signal(cancel: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        } else {
            let _signal = tokio::signal::ctrl_c().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _signal = tokio::signal::ctrl_c().await;
    }
    info!("stopping; finished chunks are kept");
    cancel.cancel();
}
