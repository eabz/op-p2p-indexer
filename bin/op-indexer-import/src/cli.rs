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

/// Downloads a chain's blocks from an external archive, verifies every block, and uploads
/// them as sealed chunks to object storage (Cloudflare R2), where servers read history from.
///
/// Run `download`, then `verify`, or `run` for both. Every step keeps its progress (the state
/// directory, and the manifest in the bucket) and can be stopped and started again: nothing
/// completed is redone.
///
/// `download` decides the range on its first run and records it in the state directory;
/// `verify` reads it from there. By default the range is OP Mainnet from block 0 to
/// the last block known to be committed to L1: the block of the newest dispute game. Blocks
/// come from Envio `HyperSync`.
#[derive(Debug, Parser)]
#[command(name = "import", version, arg = env_file_arg())]
pub(crate) struct Cli {
    /// Directory for the plan, the downloaded chunks and the record of the sealed ones. Use
    /// the same one for every step.
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
    /// Check every downloaded chunk (header hashes, parent links, transactions and receipts
    /// roots, every sender recovered from its signature), seal the blocks into chunks and
    /// upload them to object storage (R2), deleting each downloaded chunk once the sealed
    /// chunks covering it are uploaded. The chunks are listed in the manifest, with the hash
    /// index, only once the last block matches the anchor. Stops at the first block that fails
    /// a check, naming it. Resumable.
    Verify(VerifyArgs),
    /// `download`, then `verify`, stopping at the first step that cannot finish.
    Run(Box<RunArgs>),
    /// Download sealed chunks of a range straight from R2, through URLs a balancer signs
    /// (a `raw` plan), check every block and write them out as RLP, one file per chunk.
    /// Needs no state directory, archive service or R2 key. Files already written are kept.
    Fetch(FetchArgs),
}

/// Settings of `fetch`.
#[derive(Debug, Clone, Args)]
pub(crate) struct FetchArgs {
    /// The balancer's gRPC URL.
    #[arg(long, env = "OP_INDEXER_BALANCER_URL")]
    pub(crate) balancer: String,
    /// User key, sent as `authorization: Bearer <key>` (none if the balancer checks none).
    #[arg(long, env = "OP_INDEXER_API_KEY", hide_env_values = true)]
    pub(crate) api_key: Option<Secret>,
    /// Chain id of the chain [default: 10, OP Mainnet]: its Canyon time for the receipts
    /// roots.
    #[arg(long, env = "OP_INDEXER_CHAIN_ID")]
    pub(crate) chain: Option<u64>,
    /// First block.
    #[arg(long)]
    pub(crate) from: u64,
    /// Last block (inclusive).
    #[arg(long)]
    pub(crate) to: u64,
    /// Directory the files go to: `<first>-<last>.rlp`, each block an RLP list of its header,
    /// body and receipts (each its own RLP, as the eth protocol carries them).
    #[arg(long, default_value = "fetched")]
    pub(crate) out: PathBuf,
    /// Chunks downloaded at once (about 40 MB each).
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u64).range(1..=64))]
    pub(crate) downloads: u64,
}

/// Settings of `verify`: threads, and where the chunks go. The R2 keys are read from the
/// environment (a flag shows in the process list), never logged or written to disk.
#[derive(Debug, Clone, Args)]
pub(crate) struct VerifyArgs {
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
    /// Downloaded chunks verified at once (rebuilt, checked, senders recovered; default: one
    /// per CPU).
    #[arg(long, env = "OP_INDEXER_IMPORT_VERIFY_THREADS")]
    pub(crate) threads: Option<usize>,
    /// Sealed chunks uploaded at once.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_VERIFY_UPLOADS",
        default_value_t = 4,
        value_parser = clap::value_parser!(u64).range(1..=64)
    )]
    pub(crate) uploads: u64,
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
    /// some rows (Unichain's EIP-7702 authorization lists, Base's early header fields); what it
    /// gives is proven by the header hash like the rest [default: by chain,
    /// `https://mainnet.unichain.org` for Unichain (130), `https://mainnet.base.org` for Base
    /// (8453); none for OP Mainnet, whose rows need none so far].
    #[arg(long, env = "OP_INDEXER_IMPORT_RPC_ENDPOINT")]
    pub(crate) rpc_endpoint: Option<String>,
    /// Where the header fields and deposit source hashes the archive service left out come
    /// from: `l1` rebuilds them from L1 (the L1 origins' headers and the portal's deposit logs,
    /// read from `--l1-endpoint`, a span of blocks per query) and the parent block's base fee,
    /// and uses an RPC only when `--rpc-endpoint` is given, for what it cannot rebuild or what
    /// does not hash; `rpc` fetches them all from the RPC (by default the chain's public one).
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_FILL_FROM",
        value_enum,
        default_value_t = FillFrom::L1
    )]
    pub(crate) fill_from: FillFrom,
    /// Calls per request to `--rpc-endpoint`, as one JSON-RPC batch. Public endpoints cap it
    /// (Unichain's at 10); a provider of your own may take more.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_RPC_BATCH",
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(2..=1000)
    )]
    pub(crate) rpc_batch: u64,
    /// Requests to `--rpc-endpoint` in flight at once: kept low for a public endpoint.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_RPC_REQUESTS",
        default_value_t = 4,
        value_parser = clap::value_parser!(u64).range(1..=256)
    )]
    pub(crate) rpc_requests: u64,
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
    /// Also read every downloaded chunk not sealed yet, with its fill, and download again those
    /// whose rows still lack a field or cannot be read: the service's servers do not all answer
    /// alike, and asking again often gives what an answer left out. A new answer replaces the
    /// chunk, and drops its fill, only if it lacks fewer fields.
    ///
    /// The scan takes long on a large range, so the chunks it finds are kept in
    /// `refetch.json` and a later run asks for those, in block order, without scanning again
    /// (`--rescan` scans again). When the service limits requests (HTTP 429) for longer than a
    /// couple of short waits, the run stops and keeps what is left for the next.
    #[arg(long, env = "OP_INDEXER_IMPORT_REFETCH_INCOMPLETE")]
    pub(crate) refetch_incomplete: bool,
    /// With `--refetch-incomplete`: scan the chunks on disk again, even with a list kept.
    #[arg(long, requires = "refetch_incomplete")]
    pub(crate) rescan: bool,
    /// How the scan reads a chunk: `head`, the first block and its transactions of each
    /// answer (an answer that lacks a field lacks it on every row), or `full`, every row.
    #[arg(long, value_enum, default_value_t = ScanMode::Head)]
    pub(crate) scan: ScanMode,
    /// Requests in flight while `--refetch-incomplete` asks for chunks again: fewer than
    /// `--requests`, to stay under the service's rate limit.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_REFETCH_REQUESTS",
        default_value_t = 16,
        value_parser = clap::value_parser!(u64).range(1..=4096)
    )]
    pub(crate) refetch_requests: u64,
}

/// How `download --refetch-incomplete` reads a chunk on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ScanMode {
    /// The first block and its transactions of each answer: no row parsed past them.
    Head,
    /// Every row.
    Full,
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

/// Where `download` takes the header fields and source hashes the archive service left out
/// from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum FillFrom {
    /// Rebuilt from L1 and the parent block; an RPC only if one is given.
    L1,
    /// Fetched from the chain's RPC.
    Rpc,
}

/// Settings of `run`: those of every step.
#[derive(Debug, Clone, Args)]
pub(crate) struct RunArgs {
    #[command(flatten)]
    pub(crate) download: DownloadArgs,
    #[command(flatten)]
    pub(crate) verify: VerifyArgs,
}

/// A credential given on the command line: the archive service's API token, a balancer's user
/// key. `Debug` never shows it.
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

/// Declares the startup file flag for clap; loading happens before argument parsing.
fn env_file_arg() -> clap::Arg {
    clap::Arg::new("env_file")
        .long(op_indexer_runtime::env_file::FLAG.trim_start_matches('-'))
        .env("OP_INDEXER_ENV_FILE")
        .global(true)
        .value_name("PATH")
        .value_parser(clap::value_parser!(PathBuf))
        .help(
            "File of NAME=value lines loaded into the environment first; variables already set \
             win [default: .env, if it exists]",
        )
}
