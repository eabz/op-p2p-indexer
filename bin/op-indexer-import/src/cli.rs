//! The command line: subcommands, flags with their environment fallbacks, and defaults.
//!
//! Everything chain-specific is a flag of [`RangeArgs`] or [`DownloadArgs`]; the defaults are
//! OP Mainnet's blocks before Bedrock. Does not read files or connect anywhere.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use alloy_primitives::{B256, b256};
use clap::{Args, Parser, Subcommand};

use crate::load::LoadArgs;

/// Hash of OP Mainnet block 105,235,062, the last block before Bedrock: the parent hash in
/// the header of the Bedrock block 105,235,063 (hash `0xdbf6a80f…afd3`, the published Bedrock
/// genesis of OP Mainnet, which every execution peer agreed on in `docs/el-viability.md`).
const OP_MAINNET_LAST_LEGACY_HASH: B256 =
    b256!("0x21a168dfa5e727926063a28ba16fd5ee84c814e847c81a699c7a0ea551e4ca50");

/// Downloads a block range from an external archive, verifies every block against a trusted
/// block hash, and loads it into the local block archive the node serves from. No database
/// is needed: ClickHouse is optional, written only with `--clickhouse-url`.
///
/// Run `download`, then `verify`, then `load`, or `run` for all three. Every step keeps its
/// progress in the state directory and can be stopped and started again: nothing completed
/// is redone. The defaults import OP Mainnet's blocks before Bedrock (0 to 105,235,062) from
/// Envio `HyperSync`.
#[derive(Debug, Parser)]
#[command(name = "op-indexer-import", version)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) range: RangeArgs,
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// The step to run.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Fetch the chunks of the range that are not in the state directory yet. Needs the API
    /// token and nothing else; stops with a summary when the service refuses requests.
    Download(DownloadArgs),
    /// Check every downloaded chunk offline: header hashes and parent links up to the anchor,
    /// transaction hashes and roots, receipts roots, senders.
    Verify(VerifyArgs),
    /// Append verified chunks to the local block archive the node serves from, in block
    /// order, up to the first chunk that is not verified yet. Needs no database: ClickHouse
    /// is written too only with `--clickhouse-url`. The indexer must not be running.
    Load(LoadArgs),
    /// `download`, `verify`, then `load`, stopping at the first step that cannot finish.
    Run(RunArgs),
}

/// The blocks to import and where the tool keeps its files. Shared by every step; use the
/// same values for each.
#[derive(Debug, Clone, Args)]
pub(crate) struct RangeArgs {
    /// Directory for downloaded and verified chunks. Needs roughly 100 to 160 GB for the
    /// whole OP Mainnet legacy range.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_STATE_DIR",
        default_value = "import-state"
    )]
    pub(crate) state_dir: PathBuf,
    /// First block of the range.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_FIRST_BLOCK",
        default_value_t = 0
    )]
    pub(crate) first_block: u64,
    /// Last block of the range (default: OP Mainnet's last block before Bedrock).
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_LAST_BLOCK",
        default_value_t = 105_235_062
    )]
    pub(crate) last_block: u64,
    /// Trusted hash of the last block of the range. Every block is verified by the chain of
    /// parent hashes down from it (default: OP Mainnet block 105,235,062).
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_ANCHOR_HASH",
        default_value_t = OP_MAINNET_LAST_LEGACY_HASH
    )]
    pub(crate) anchor_hash: B256,
    /// Blocks per chunk: one file on disk, and one request when the service answers it in
    /// full. Keep it the same across runs, or chunks are downloaded again.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_CHUNK_BLOCKS",
        default_value_t = 1000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) chunk_blocks: u64,
    /// OP Stack chains: Regolith activation, in Unix seconds; from it on the L1-attributes
    /// deposit is not a system transaction and deposit receipts record a nonce (default: OP
    /// Mainnet, where Regolith is active from the Bedrock block). Unused for blocks without
    /// deposits.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_REGOLITH_TIME",
        default_value_t = 0
    )]
    pub(crate) regolith_time: u64,
    /// OP Stack chains: Canyon activation, in Unix seconds; from it on the deposit nonce is
    /// part of the hashed receipt (default: OP Mainnet).
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_CANYON_TIME",
        default_value_t = 1_704_992_401
    )]
    pub(crate) canyon_time: u64,
    /// OP Stack chains: Isthmus activation, in Unix seconds; from it on the header carries the
    /// hash of an empty requests list (default: OP Mainnet).
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_ISTHMUS_TIME",
        default_value_t = 1_746_806_401
    )]
    pub(crate) isthmus_time: u64,
}

/// Settings of `download`.
#[derive(Debug, Clone, Args)]
pub(crate) struct DownloadArgs {
    /// API token of the archive service. A flag is visible in the process list and the shell
    /// history; the environment variable is not. It is never logged or written to disk.
    #[arg(long, env = "ENVIO_API_TOKEN", hide_env_values = true)]
    pub(crate) api_token: ApiToken,
    /// `HyperSync` endpoint of the chain (default: OP Mainnet).
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_ENDPOINT",
        default_value = "https://optimism.hypersync.xyz"
    )]
    pub(crate) endpoint: String,
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

/// The archive service's API token. `Debug` never shows it.
#[derive(Clone)]
pub(crate) struct ApiToken(String);

impl ApiToken {
    /// The token, for the request header only.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiToken(<redacted>)")
    }
}

impl FromStr for ApiToken {
    type Err = &'static str;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let token = token.trim();
        if token.is_empty() {
            return Err("the API token is empty");
        }
        Ok(Self(token.to_owned()))
    }
}
