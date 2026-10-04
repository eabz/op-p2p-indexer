//! The command line: subcommands, flags with their environment fallbacks, and defaults.
//!
//! The range is decided by the flags of [`DownloadArgs`]; chain parameters come from the
//! chainspec. Does not read files or connect anywhere.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use alloy_primitives::B256;
use clap::{Args, Parser, Subcommand};

use crate::load::LoadArgs;

/// Downloads a chain's blocks from an external archive, verifies every block, and loads
/// them into the local block archive the node serves from. No database is needed: ClickHouse
/// is optional, written only with `--clickhouse-url`.
///
/// Run `download`, then `verify`, then `load`, or `run` for all three. Every step keeps its
/// progress in the state directory and can be stopped and started again: nothing completed
/// is redone.
///
/// `download` decides the range on its first run and records it in the state directory;
/// `verify` and `load` read it from there. By default the range is OP Mainnet from block 0 to
/// the last block known to be committed to L1: the block of the newest dispute game. Blocks
/// come from Envio `HyperSync`.
#[derive(Debug, Parser)]
#[command(name = "op-indexer-import", version)]
pub(crate) struct Cli {
    /// Directory for the plan and the downloaded and verified chunks. Use the same one for
    /// every step.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_STATE_DIR",
        default_value = "import-state"
    )]
    pub(crate) state_dir: PathBuf,
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// The step to run.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Fetch the chunks of the range that are not in the state directory yet. Needs the API
    /// token and nothing else; stops with a summary when the service refuses requests or the
    /// disk is nearly full.
    Download(DownloadArgs),
    /// Check every downloaded chunk offline: header hashes and parent links up to the anchor,
    /// transactions roots and receipts roots. Senders are not checked.
    Verify(VerifyCommand),
    /// Append the verified range to the local block archive the node serves from. Needs the
    /// whole range accepted by `verify`, and no database: ClickHouse is written too only with
    /// `--clickhouse-url`. The indexer must not be running.
    Load(LoadArgs),
    /// `download`, `verify`, then `load`, stopping at the first step that cannot finish.
    Run(RunArgs),
}

/// Settings of `download`. The range flags are read on the first run only, when the plan is
/// recorded; on later runs a flag that disagrees with the recorded plan is refused.
#[derive(Debug, Clone, Args)]
pub(crate) struct DownloadArgs {
    /// API token of the archive service. A flag is visible in the process list and the shell
    /// history; the environment variable is not. It is never logged or written to disk.
    #[arg(long, env = "ENVIO_API_TOKEN", hide_env_values = true)]
    pub(crate) api_token: Secret,
    /// Chain id of the chain to import [default: 10, OP Mainnet].
    #[arg(long, env = "OP_INDEXER_IMPORT_CHAIN")]
    pub(crate) chain: Option<u64>,
    /// First block of the range [default: 0].
    #[arg(long, env = "OP_INDEXER_IMPORT_FIRST_BLOCK")]
    pub(crate) first_block: Option<u64>,
    /// Last block of the range, with `--anchor-hash`, in place of the default: the L2 block of
    /// the newest dispute game on L1, which `verify` checks against the game's claim.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_LAST_BLOCK",
        requires = "anchor_hash",
        conflicts_with = "legacy_only"
    )]
    pub(crate) last_block: Option<u64>,
    /// The trusted hash of `--last-block`. Every block is verified by the chain of parent
    /// hashes down from it.
    #[arg(long, env = "OP_INDEXER_IMPORT_ANCHOR_HASH", requires = "last_block")]
    pub(crate) anchor_hash: Option<B256>,
    /// Import only the blocks before Bedrock: the range ends at the chain's last legacy
    /// block, checked against its known hash. Needs no lookup on L1. Refused for a chain that
    /// began with Bedrock (Unichain).
    #[arg(long, env = "OP_INDEXER_IMPORT_LEGACY_ONLY")]
    pub(crate) legacy_only: bool,
    /// Blocks per chunk: one file on disk, and one request when the service answers it in
    /// full [default: 1000].
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_CHUNK_BLOCKS",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) chunk_blocks: Option<u64>,
    /// `HyperSync` endpoint of the chain [default: by chain, `https://optimism.hypersync.xyz`
    /// for OP Mainnet (10) and `https://unichain.hypersync.xyz` for Unichain (130), the host
    /// the service's naming gives, not yet reached from here]. Needed for a chain not in
    /// that list.
    #[arg(long, env = "OP_INDEXER_IMPORT_ENDPOINT")]
    pub(crate) endpoint: Option<String>,
    /// `HyperSync` endpoint of the L1 chain the dispute games are on, for the lookup of the
    /// range's last block. Uses the same API token.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_L1_ENDPOINT",
        default_value = "https://eth.hypersync.xyz"
    )]
    pub(crate) l1_endpoint: String,
    /// Requests in flight at once.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_REQUESTS",
        default_value_t = 64,
        value_parser = clap::value_parser!(u64).range(1..=4096)
    )]
    pub(crate) requests: u64,
}

/// Settings of `verify`.
#[derive(Debug, Clone, Args)]
pub(crate) struct VerifyArgs {
    /// Chunks verified at once (default: one per CPU).
    #[arg(long, env = "OP_INDEXER_IMPORT_VERIFY_THREADS")]
    pub(crate) verify_threads: Option<usize>,
}

/// Settings of the `verify` step on its own.
#[derive(Debug, Clone, Args)]
pub(crate) struct VerifyCommand {
    #[command(flatten)]
    pub(crate) verify: VerifyArgs,
    /// Verify only the chunks from this block on, and do not link or accept the range: a
    /// quick check of one part of the chain. The chunks it verifies are kept; `verify`
    /// without this flag must still run before `load`. Not taken by `run`, whose `load`
    /// needs the whole range accepted.
    #[arg(long, env = "OP_INDEXER_IMPORT_VERIFY_FROM_BLOCK")]
    pub(crate) from_block: Option<u64>,
}

/// Settings of `run`: those of every step.
#[derive(Debug, Clone, Args)]
pub(crate) struct RunArgs {
    #[command(flatten)]
    pub(crate) download: DownloadArgs,
    #[command(flatten)]
    pub(crate) verify: VerifyArgs,
    #[command(flatten)]
    pub(crate) load: LoadArgs,
}

/// A credential given on the command line: the archive service's API token, a database
/// password. `Debug` never shows it.
#[derive(Clone)]
pub(crate) struct Secret(String);

impl Secret {
    /// The value, for the one place that sends it.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl FromStr for Secret {
    type Err = &'static str;

    fn from_str(secret: &str) -> Result<Self, Self::Err> {
        let secret = secret.trim();
        if secret.is_empty() {
            return Err("the value is empty");
        }
        Ok(Self(secret.to_owned()))
    }
}
