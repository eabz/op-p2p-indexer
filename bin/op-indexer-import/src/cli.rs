//! The command line: subcommands, flags with their environment fallbacks, and defaults.
//!
//! The range is decided by the flags of [`DownloadArgs`]; chain parameters come from the
//! chainspec. Does not read files or connect anywhere; reads the environment only through clap,
//! and for the API token's former name.

use std::path::PathBuf;
use std::str::FromStr;
use std::{env, fmt};

use alloy_primitives::B256;
use clap::{Args, Parser, Subcommand};
use eyre::eyre;
use tracing::warn;

/// The former name of `OP_INDEXER_IMPORT_API_TOKEN`, still read.
const DEPRECATED_API_TOKEN_VAR: &str = "ENVIO_API_TOKEN";

/// Downloads a chain's blocks from an external archive, verifies every block, and exports
/// them as sealed chunks to object storage (Cloudflare R2), where servers read history from.
///
/// Run `download`, then `verify`, then `export`, or `run` for all three. Every step keeps its
/// progress (the state directory, and the manifest in the bucket) and can be stopped and
/// started again: nothing completed is redone.
///
/// `download` decides the range on its first run and records it in the state directory;
/// `verify` and `export` read it from there. By default the range is OP Mainnet from block 0 to
/// the last block known to be committed to L1: the block of the newest dispute game. Blocks
/// come from Envio `HyperSync`.
#[derive(Debug, Parser)]
#[command(name = "import", version, arg = crate::env_file::arg())]
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
    /// token; stops with a summary when the service refuses requests or the disk is nearly
    /// full. Then lists every field the downloaded rows lack, and fetches the ones it can from
    /// the chain's RPC endpoint (`--rpc-endpoint`).
    Download(DownloadArgs),
    /// Check every downloaded chunk offline: header hashes and parent links up to the anchor,
    /// transactions roots and receipts roots. Senders are checked by `export`.
    Verify(VerifyCommand),
    /// Convert the verified range into sealed chunks, their manifest and the hash index, and
    /// upload them to object storage (R2). Recovers every transaction's sender from its
    /// signature and checks it against the verified chunk first; stops at the first that
    /// differs. Resumable from the manifest. Needs the whole range accepted by `verify`.
    Export(ExportArgs),
    /// `download`, `verify`, then `export`, stopping at the first step that cannot finish.
    Run(Box<RunArgs>),
}

/// Settings of `export`: where the chunks go. The R2 keys are read from the environment (a
/// flag shows in the process list), never logged or written to disk.
#[derive(Debug, Clone, Args)]
pub(crate) struct ExportArgs {
    /// Write to this local directory instead of R2, in the layout the bucket would hold
    /// (`<dir>/<prefix>/…`; for a test or the bench without credentials).
    #[arg(long)]
    pub(crate) to_dir: Option<PathBuf>,
    /// R2 account id: the endpoint is `https://<account id>.r2.cloudflarestorage.com`.
    #[arg(long, env = "OP_INDEXER_R2_ACCOUNT_ID")]
    pub(crate) r2_account_id: Option<String>,
    /// R2 bucket. Default: `<chain>-snapshot` (`op-snapshot`, `unichain-snapshot`,
    /// `base-snapshot`).
    #[arg(long, env = "OP_INDEXER_R2_BUCKET")]
    pub(crate) r2_bucket: Option<String>,
    /// Folder in the bucket the chunks, manifest and index go under (`<prefix>/chunks/…`,
    /// `<prefix>/manifest/…`, `<prefix>/index/…`).
    #[arg(long, env = "OP_INDEXER_R2_PREFIX", default_value = "archive")]
    pub(crate) r2_prefix: String,
    /// R2 access key id (an API token with write access to the bucket).
    #[arg(long, env = "OP_INDEXER_R2_ACCESS_KEY_ID", hide_env_values = true)]
    pub(crate) r2_access_key_id: Option<Secret>,
    /// R2 secret access key. A flag is visible in the process list; the variable is not.
    #[arg(long, env = "OP_INDEXER_R2_SECRET_ACCESS_KEY", hide_env_values = true)]
    pub(crate) r2_secret_access_key: Option<Secret>,
    /// Another endpoint than the account's (an S3-compatible store).
    #[arg(long, env = "OP_INDEXER_R2_ENDPOINT")]
    pub(crate) r2_endpoint: Option<String>,
    /// Verified chunks prepared at once (read, senders recovered, receipts encoded; default:
    /// one per CPU).
    #[arg(long, env = "OP_INDEXER_IMPORT_EXPORT_THREADS")]
    pub(crate) threads: Option<usize>,
    /// Chunks uploaded at once.
    #[arg(long, env = "OP_INDEXER_IMPORT_EXPORT_UPLOADS", default_value_t = 4)]
    pub(crate) uploads: usize,
}

/// Settings of `download`. The range flags are read on the first run only, when the plan is
/// recorded; on later runs a flag that disagrees with the recorded plan is refused.
#[derive(Debug, Clone, Args)]
pub(crate) struct DownloadArgs {
    /// API token of the archive service, required by `download`. A flag is visible in the
    /// process list and the shell history; the environment variable is not. It is never logged
    /// or written to disk. `ENVIO_API_TOKEN`, its former variable, is still read, with a
    /// warning.
    #[arg(long, env = "OP_INDEXER_IMPORT_API_TOKEN", hide_env_values = true)]
    pub(crate) api_token: Option<Secret>,
    /// Chain id of the chain to import [default: 10, OP Mainnet].
    #[arg(long, env = "OP_INDEXER_CHAIN_ID")]
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
    /// for OP Mainnet (10), `https://unichain.hypersync.xyz` for Unichain (130) and
    /// `https://base.hypersync.xyz` for Base (8453)]. Needed for a chain not in that list.
    #[arg(long, env = "OP_INDEXER_IMPORT_ENDPOINT")]
    pub(crate) endpoint: Option<String>,
    /// JSON-RPC endpoint of the chain, read-only, for what the archive service leaves out of
    /// some rows (Unichain's EIP-7702 authorization lists); what it gives is proven by the
    /// header hash like the rest [default: by chain, `https://mainnet.unichain.org` for
    /// Unichain (130); none for OP Mainnet, whose rows need none so far].
    #[arg(long, env = "OP_INDEXER_IMPORT_RPC_ENDPOINT")]
    pub(crate) rpc_endpoint: Option<String>,
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

impl DownloadArgs {
    /// The API token: `--api-token` or `OP_INDEXER_IMPORT_API_TOKEN`, else the former
    /// `ENVIO_API_TOKEN`, with a deprecation warning.
    ///
    /// # Errors
    ///
    /// Returns an error if none is set, or `ENVIO_API_TOKEN` is blank.
    pub(crate) fn api_token(&self) -> eyre::Result<Secret> {
        if let Some(token) = &self.api_token {
            return Ok(token.clone());
        }
        let token = env::var(DEPRECATED_API_TOKEN_VAR).map_err(|_unset| {
            eyre!("the API token is required: set OP_INDEXER_IMPORT_API_TOKEN, or --api-token")
        })?;
        warn!("{DEPRECATED_API_TOKEN_VAR} is deprecated: rename it OP_INDEXER_IMPORT_API_TOKEN");
        token
            .parse()
            .map_err(|err| eyre!("{DEPRECATED_API_TOKEN_VAR}: {err}"))
    }
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
    /// without this flag must still run before `export`. Not taken by `run`, whose `export`
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
    pub(crate) export: ExportArgs,
}

/// A credential given on the command line: the archive service's API token. `Debug` never
/// shows it.
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
