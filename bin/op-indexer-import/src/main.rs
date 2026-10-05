//! Imports a block range from an external archive into object storage, where servers read
//! history from.
//!
//! ```text
//! archive service ─▶ download ─▶ <state>/raw ─▶ verify ─▶ R2 (sealed chunks, manifest, index)
//! ```
//!
//! - `download` ([`mod@download`]) decides the range, records it, fetches it in chunks from
//!   the archive service ([`source`]) and keeps each answer as received, so a limited request
//!   window is spent on the transfer only. It then checks the downloaded rows for fields the
//!   service left out and fetches the ones it can from the chain's RPC ([`mod@fill`], [`rpc`]).
//! - `verify` ([`mod@verify`]) rebuilds every block's consensus encoding from the downloaded rows
//!   and checks it: header hash, parent links up to a trusted anchor, transactions root,
//!   receipts root, and every sender recovered from its signature. It seals the blocks into
//!   chunks and uploads them (`crates/chunks`), deleting each downloaded chunk once the sealed
//!   chunks covering it are uploaded, and lists them in the manifest, with the hash index, once
//!   the last block matches the anchor.
//!
//! - `fetch` ([`mod@fetch`]) is the other way round: it downloads sealed chunks straight from
//!   R2 through URLs a balancer signs, checks them and writes the blocks out.
//!
//! Every step is resumable: a downloaded chunk's file exists only when it is complete, a
//! sealed chunk is recorded in the state directory once uploaded, and the manifest lists only
//! chunks of a range proven up to its anchor. The indexer never links this binary and never
//! talks to the archive service. See `docs/import.md`.

mod backoff;
mod cli;
mod download;
use op_indexer_runtime::env_file;
mod fetch;
mod fill;
mod game;
mod progress;
mod rows;
mod rpc;
mod scan;
mod source;
mod state;
mod verify;

use std::path::PathBuf;

use clap::Parser;
use eyre::WrapErr;
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::cli::{Cli, Command, DownloadArgs, FillFrom, Secret};
use crate::rpc::Rpc;
use crate::source::HyperSync;
use crate::state::{Anchor, Plan, State};

/// Blocks per chunk unless the first `download` says otherwise.
const DEFAULT_CHUNK_BLOCKS: u64 = 1000;

/// The allocator of this binary: see the workspace manifest for what it buys.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> eyre::Result<()> {
    // Before the `.env` file, which may not load: clap's `--version` answers only after it.
    if op_indexer_runtime::version_requested(env!("CARGO_BIN_NAME"), env!("CARGO_PKG_VERSION")) {
        return Ok(());
    }
    // First: loading sets environment variables, which is sound only before the runtime starts
    // any thread, and the command line falls back to them.
    let env_file = env_file::load(std::env::args_os().skip(1))?;
    let cli = Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("failed to start the tokio runtime")?
        .block_on(run(cli, env_file))
}

async fn run(cli: Cli, env_file: Option<PathBuf>) -> eyre::Result<()> {
    op_indexer_runtime::init_tracing(env_file.as_deref());
    // Startup-only blocking I/O, before any task runs; `fetch` needs no state directory.
    let open = || State::open(&cli.state_dir).wrap_err("failed to open the state directory");

    let cancel = CancellationToken::new();
    let signal = tokio::spawn(cancel_on_signal(cancel.clone()));
    let result = match &cli.command {
        Command::Download(args) => download(args, &open()?, &cancel).await.map(|_plan| ()),
        Command::Verify(args) => {
            let state = open()?;
            verify::run(args, &state, &recorded_plan(&state)?, &cancel).await
        }
        Command::Fetch(args) => fetch::run(args).await,
        Command::Run(args) => {
            let state = open()?;
            let steps = async {
                let plan = download(&args.download, &state, &cancel).await?;
                verify::run(&args.verify, &state, &plan, &cancel).await
            };
            steps.await
        }
    };
    signal.abort();
    result
}

/// The plan `download` recorded, which `verify` works from.
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
async fn plan(args: &DownloadArgs, state: &State, api_token: &Secret) -> eyre::Result<Plan> {
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
        let l1 = HyperSync::new(&args.l1_endpoint, api_token)?;
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
            "--chain (OP_INDEXER_CHAIN_ID)",
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
    let api_token = args.api_token()?;
    let plan = plan(args, state, &api_token).await?;
    let source = HyperSync::new(&endpoint, &api_token)?;
    let requests = usize::try_from(args.requests).wrap_err("--requests is too large")?;
    download::ensure_open_files(args.requests)?;
    download::run(
        &source,
        state,
        &plan,
        requests,
        threads(None),
        args.refetch_incomplete
            .then(|| -> eyre::Result<download::Refetch> {
                Ok(download::Refetch {
                    rescan: args.rescan,
                    scan: args.scan,
                    requests: usize::try_from(args.refetch_requests)
                        .wrap_err("--refetch-requests is too large")?,
                })
            })
            .transpose()?,
        cancel,
    )
    .await?;
    // What the service left out of the rows, from the chain's RPC.
    // Rebuilding from L1, an RPC is used only if one is given: the chain's public one is no
    // fallback to count on.
    let rpc_endpoint = match args.fill_from {
        FillFrom::L1 => args.rpc_endpoint.as_deref(),
        FillFrom::Rpc => args
            .rpc_endpoint
            .as_deref()
            .or_else(|| rpc::default_endpoint(plan.chain.chain_id)),
    };
    let batch = usize::try_from(args.rpc_batch).wrap_err("--rpc-batch is too large")?;
    let rpc = rpc_endpoint.map(|url| Rpc::new(url, batch)).transpose()?;
    let rpc_requests =
        usize::try_from(args.rpc_requests).wrap_err("--rpc-requests is too large")?;
    // What the rows lack, rebuilt from L1 unless asked otherwise.
    let l1 = match args.fill_from {
        FillFrom::L1 => Some(HyperSync::new(&args.l1_endpoint, &api_token)?),
        FillFrom::Rpc => None,
    };
    let (rpc, l1) = (rpc.as_ref(), l1.as_ref());
    fill::run(state, &plan, rpc, l1, rpc_requests, threads(None), cancel).await?;
    Ok(plan)
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
    // Preserve the importer policy: Ctrl-C remains usable without a SIGTERM handler,
    // and a signal error still cancels the current step before it starts more work.
    let _signal = op_indexer_runtime::shutdown_signal_with_policy(
        op_indexer_runtime::SignalPolicy::CtrlCFallback,
    )
    .await;
    info!("stopping; finished chunks are kept");
    cancel.cancel();
}
