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
mod game;
mod load;
mod rows;
mod source;
mod state;
mod transaction;
mod verify;

use std::io::Write;

use clap::Parser;
use eyre::WrapErr;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::cli::{
    Cli, Command, DownloadArgs, OP_MAINNET_LAST_LEGACY_BLOCK, OP_MAINNET_LAST_LEGACY_HASH,
    VerifyArgs,
};
use crate::game::{GameAnchor, GameConfig, L2Chain};
use crate::source::HyperSync;
use crate::state::{Anchor, Forks, Plan, State};

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

    // Startup-only blocking I/O, before any task runs.
    let state = State::open(&cli.range.state_dir).wrap_err("failed to open the state directory")?;
    let (last, anchor) = top(&cli, &state).await?;
    eyre::ensure!(
        cli.range.first_block <= last,
        "--first-block is above the last block of the range, {last}"
    );
    let plan = Plan {
        first: cli.range.first_block,
        last,
        anchor,
        chunk_blocks: cli.range.chunk_blocks,
        forks: Forks {
            regolith: cli.range.regolith_time,
            canyon: cli.range.canyon_time,
            isthmus: cli.range.isthmus_time,
        },
    };

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

/// The last block of the range and what it is checked against. By default the block of the
/// newest dispute game on L1, which is looked up once (by `download` or `run`, which hold
/// the API token) and recorded in the state directory; with range flags a trusted hash, or
/// nothing when that was asked for.
async fn top(cli: &Cli, state: &State) -> eyre::Result<(u64, Anchor)> {
    let range = &cli.range;
    if range.legacy_only {
        let hash = Anchor::Hash(OP_MAINNET_LAST_LEGACY_HASH);
        return Ok((OP_MAINNET_LAST_LEGACY_BLOCK, hash));
    }
    if let Some(last) = range.last_block {
        let anchor = match range.anchor_hash {
            Some(hash) => Anchor::Hash(hash),
            None if last == OP_MAINNET_LAST_LEGACY_BLOCK => {
                Anchor::Hash(OP_MAINNET_LAST_LEGACY_HASH)
            }
            None if range.allow_unanchored_top => Anchor::None,
            None => eyre::bail!(
                "--last-block {last} needs --anchor-hash, the trusted hash of that block \
                 (or --allow-unanchored-top to go without)"
            ),
        };
        return Ok((last, anchor));
    }

    let path = state.anchor_path();
    let game: GameAnchor = if path.exists() {
        let recorded = std::fs::read(path).wrap_err("failed to read anchor.json")?;
        serde_json::from_slice(&recorded).wrap_err("anchor.json is damaged")?
    } else {
        let args = match &cli.command {
            Command::Download(args) => args,
            Command::Run(args) => &args.download,
            Command::Verify(_) | Command::Load(_) => eyre::bail!(
                "the range has no end: no dispute game is recorded in {}. Run `download` \
                 first, or give the range (--legacy-only, or --last-block <n> with \
                 --anchor-hash <hash>)",
                path.display()
            ),
        };
        let l1 = HyperSync::new(&args.l1_endpoint, &args.api_token)?;
        let config = GameConfig {
            factory: args.dispute_game_factory,
            game_type: args.game_type,
            resolved_only: args.resolved_only,
            l2: L2Chain {
                chain_id: args.l2_chain_id,
                genesis_number: args.l2_genesis_block,
                genesis_time: args.l2_genesis_time,
                block_time_secs: args.l2_block_time,
            },
        };
        let game = game::newest_game(&l1, &config).await.wrap_err_with(|| {
            format!(
                "the lookup of the newest dispute game on L1 ({}) failed. To go without it, \
                 give the end of the range yourself: --last-block <n> --allow-unanchored-top \
                 (or --last-block <n> --anchor-hash <hash>, or --legacy-only)",
                args.l1_endpoint
            )
        })?;
        let recorded = serde_json::to_vec_pretty(&game)?;
        state::write_atomic(path, |file| file.write_all(&recorded))
            .wrap_err("failed to write anchor.json")?;
        game
    };
    info!(
        game = %game.game,
        l2_block = game.l2_block,
        l1_block = game.l1_block,
        "the range ends at the block of a dispute game"
    );
    Ok((game.l2_block, Anchor::Game(game)))
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
