//! Imports a block range from an external archive and exports it to object storage, where
//! servers read history from.
//!
//! ```text
//! archive service ─▶ download ─▶ <state>/raw ─▶ verify ─▶ <state>/verified ─▶ export ─▶ R2
//! ```
//!
//! - `download` ([`mod@download`]) decides the range, records it, fetches it in chunks from
//!   the archive service ([`source`]) and keeps each answer as received, so a limited request
//!   window is spent on the transfer only. It then checks the downloaded rows for fields the
//!   service left out and fetches the ones it can from the chain's RPC ([`mod@fill`], [`rpc`]).
//! - `verify` ([`mod@verify`]) rebuilds every block's consensus encoding from the downloaded rows
//!   and checks it: header hash, parent links up to a trusted anchor, transactions root and
//!   receipts root (senders are checked later, by `export`). What passes is written as the exact
//!   verified bytes ([`chunk`]).
//! - `export` ([`mod@export`]) recovers every sender from its signature, checks it against the
//!   one the service reported, and seals the verified blocks into chunks, uploaded to R2 with
//!   their manifest and hash index (`crates/chunks`).
//!
//! Every step is resumable: a chunk's file exists only when the chunk is complete, and the
//! manifest lists only chunks fully uploaded. The indexer never links this binary and never
//! talks to the archive service. See `docs/import.md`.

mod backoff;
mod chunk;
mod cli;
mod download;
mod export;
mod fill;
mod game;
mod progress;
mod rows;
mod rpc;
mod source;
mod state;
mod verify;

use clap::Parser;
use eyre::WrapErr;
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

use crate::cli::{Cli, Command, DownloadArgs, VerifyArgs};
use crate::rpc::Rpc;
use crate::source::HyperSync;
use crate::state::{Anchor, Plan, State};

/// Log timestamp: UTC time of day with milliseconds, as the indexer's.
const LOG_TIME_FORMAT: &str = "%H:%M:%S%.3f";

/// Blocks per chunk unless the first `download` says otherwise.
const DEFAULT_CHUNK_BLOCKS: u64 = 1000;

/// The allocator of this binary: see the workspace manifest for what it buys.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
    let state = State::open(&cli.state_dir).wrap_err("failed to open the state directory")?;

    let cancel = CancellationToken::new();
    let signal = tokio::spawn(cancel_on_signal(cancel.clone()));
    let result = match cli.command {
        Command::Download(args) => download(&args, &state, &cancel).await.map(|_plan| ()),
        Command::Verify(args) => {
            let plan = recorded_plan(&state)?;
            verify(&args.verify, args.from_block, &state, &plan, &cancel).await
        }
        Command::Export(args) => export::run(&args, &state, &recorded_plan(&state)?, &cancel).await,
        Command::Run(args) => {
            let steps = async {
                let plan = download(&args.download, &state, &cancel).await?;
                verify(&args.verify, None, &state, &plan, &cancel).await?;
                export::run(&args.export, &state, &plan, &cancel).await
            };
            steps.await
        }
    };
    signal.abort();
    result
}

/// The plan `download` recorded, which `verify` and `load` work from.
fn recorded_plan(state: &State) -> eyre::Result<Plan> {
    state.read_plan()?.ok_or_else(|| {
        eyre::eyre!(
            "{} has no plan.json: run `download` first, which decides the range",
            state.root().display()
        )
    })
}

/// The plan `download` works from: the recorded one, which the range flags must not
/// contradict, or on the first run the one the flags describe, which is then recorded.
async fn plan(args: &DownloadArgs, state: &State) -> eyre::Result<Plan> {
    if let Some(plan) = state.read_plan()? {
        check_flags(args, &plan)?;
        return Ok(plan);
    }
    eyre::ensure!(
        !state.has_chunks()?,
        "{} holds chunks but no plan.json: it was written by an older build. Delete it and \
         download again",
        state.root().display()
    );
    let chain_id = args.chain.unwrap_or(OP_MAINNET.chain_id);
    let chain = ChainSpec::by_chain_id(chain_id)
        .ok_or_else(|| eyre::eyre!("chain {chain_id} is not known to this build"))?;
    let (last, anchor) = if args.legacy_only {
        let hash = chain.last_legacy_hash.ok_or_else(|| {
            eyre::eyre!(
                "--legacy-only: chain {} began with Bedrock and has no legacy blocks",
                chain.chain_id
            )
        })?;
        (chain.bedrock_block.saturating_sub(1), Anchor::Hash(hash))
    } else if let (Some(last), Some(hash)) = (args.last_block, args.anchor_hash) {
        (last, Anchor::Hash(hash))
    } else {
        let l1 = HyperSync::new(&args.l1_endpoint, &args.api_token)?;
        let game = game::newest_game(&l1, chain).await.wrap_err_with(|| {
            format!(
                "the lookup of the newest dispute game on L1 ({}) failed. To go without it, \
                 give the end of the range yourself: --last-block <n> --anchor-hash <hash>, or \
                 --legacy-only",
                args.l1_endpoint
            )
        })?;
        (game.l2_block, Anchor::Game(game))
    };
    let plan = Plan {
        chain,
        first: args.first_block.unwrap_or_default(),
        last,
        anchor,
        chunk_blocks: args.chunk_blocks.unwrap_or(DEFAULT_CHUNK_BLOCKS),
    };
    eyre::ensure!(
        plan.first <= plan.last,
        "--first-block is above the last block of the range, {last}"
    );
    state
        .write_plan(&plan)
        .wrap_err("failed to write plan.json")?;
    info!(
        chain = chain.chain_id,
        first = plan.first,
        last,
        anchor = %plan.anchor,
        chunk_blocks = plan.chunk_blocks,
        "plan recorded"
    );
    Ok(plan)
}

/// Refuses a range flag that disagrees with the recorded plan: files already written were
/// cut by that plan.
fn check_flags(args: &DownloadArgs, plan: &Plan) -> eyre::Result<()> {
    let legacy = plan.chain.last_legacy_hash.map(|hash| {
        (
            plan.chain.bedrock_block.saturating_sub(1),
            Anchor::Hash(hash),
        )
    });
    let disagreements = [
        (
            "--chain",
            args.chain.is_some_and(|chain| chain != plan.chain.chain_id),
        ),
        (
            "--first-block",
            args.first_block.is_some_and(|first| first != plan.first),
        ),
        (
            "--last-block",
            args.last_block.is_some_and(|last| last != plan.last),
        ),
        (
            "--anchor-hash",
            args.anchor_hash
                .is_some_and(|hash| Anchor::Hash(hash) != plan.anchor),
        ),
        (
            "--legacy-only",
            args.legacy_only && Some((plan.last, plan.anchor)) != legacy,
        ),
        (
            "--chunk-blocks",
            args.chunk_blocks
                .is_some_and(|blocks| blocks != plan.chunk_blocks),
        ),
    ];
    for (flag, disagrees) in disagreements {
        eyre::ensure!(
            !disagrees,
            "{flag} disagrees with the plan recorded in this state directory (chain {}, blocks \
             {} to {}, anchor {}, {} blocks per chunk). Leave the flag out to continue, or use \
             an empty state directory for another range",
            plan.chain.chain_id,
            plan.first,
            plan.last,
            plan.anchor,
            plan.chunk_blocks
        );
    }
    Ok(())
}

async fn download(
    args: &DownloadArgs,
    state: &State,
    cancel: &CancellationToken,
) -> eyre::Result<Plan> {
    // Known before the plan is made, which may already use the service: the recorded plan's
    // chain (a later run need not name it again), else the one asked for.
    let recorded = state.read_plan()?.map(|plan| plan.chain.chain_id);
    let chain_id = recorded.or(args.chain).unwrap_or(OP_MAINNET.chain_id);
    let endpoint = match &args.endpoint {
        Some(endpoint) => endpoint.clone(),
        None => source::default_endpoint(chain_id)
            .ok_or_else(|| {
                eyre::eyre!(
                    "no HyperSync endpoint is known for chain {chain_id}: give it with --endpoint"
                )
            })?
            .to_owned(),
    };
    let plan = plan(args, state).await?;
    let source = HyperSync::new(&endpoint, &args.api_token)?;
    let requests = usize::try_from(args.requests).wrap_err("--requests is too large")?;
    download::ensure_open_files(args.requests)?;
    download::run(&source, state, &plan, requests, cancel).await?;
    // What the service left out of the rows, from the chain's RPC.
    let rpc_endpoint = args
        .rpc_endpoint
        .as_deref()
        .or_else(|| rpc::default_endpoint(plan.chain.chain_id));
    let rpc = rpc_endpoint.map(Rpc::new).transpose()?;
    fill::run(state, &plan, rpc.as_ref(), threads(None), cancel).await?;
    Ok(plan)
}

async fn verify(
    args: &VerifyArgs,
    from_block: Option<u64>,
    state: &State,
    plan: &Plan,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    verify::run(
        state,
        plan,
        threads(args.verify_threads),
        from_block,
        cancel,
    )
    .await
}

/// Threads for the CPU-bound work: `asked`, else one per CPU.
fn threads(asked: Option<usize>) -> usize {
    asked
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1)
        .max(1)
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
