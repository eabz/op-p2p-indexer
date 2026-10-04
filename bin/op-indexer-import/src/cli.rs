//! The command line: subcommands, flags with their environment fallbacks, and defaults.
//!
//! Everything chain-specific is a flag of [`RangeArgs`] or [`DownloadArgs`]; the defaults are
//! OP Mainnet's blocks before Bedrock. Does not read files or connect anywhere.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use alloy_primitives::{Address, B256, address, b256};
use clap::{Args, Parser, Subcommand};

use crate::load::LoadArgs;

/// OP Mainnet's last block before Bedrock: the default end of the range.
pub(crate) const OP_MAINNET_LAST_LEGACY_BLOCK: u64 = 105_235_062;

/// Hash of OP Mainnet block 105,235,062, the last block before Bedrock: the parent hash in
/// the header of the Bedrock block 105,235,063 (hash `0xdbf6a80f…afd3`, the published Bedrock
/// genesis of OP Mainnet, which every execution peer agreed on in `docs/el-viability.md`).
pub(crate) const OP_MAINNET_LAST_LEGACY_HASH: B256 =
    b256!("0x21a168dfa5e727926063a28ba16fd5ee84c814e847c81a699c7a0ea551e4ca50");

/// OP Mainnet's `DisputeGameFactoryProxy` on Ethereum Mainnet (superchain registry,
/// `superchain/configs/mainnet/op.toml`).
const OP_MAINNET_DISPUTE_GAME_FACTORY: Address =
    address!("0xe5965Ab5962eDc7477C8520243A95517CD252fA9");

/// Downloads a chain's blocks from an external archive, verifies every block, and loads
/// them into the local block archive the node serves from. No database is needed: ClickHouse
/// is optional, written only with `--clickhouse-url`.
///
/// Run `download`, then `verify`, then `load`, or `run` for all three. Every step keeps its
/// progress in the state directory and can be stopped and started again: nothing completed
/// is redone.
///
/// With no range flags the range is OP Mainnet from block 0 to the last block known to be
/// committed to L1: the block of the newest dispute game, which `download` looks up once and
/// records in the state directory. `--legacy-only` imports only the blocks before Bedrock (0
/// to 105235062); `--first-block` and `--last-block` give any other range. Blocks come from
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
    /// Directory for downloaded and verified chunks, and the record of the range's end. Use
    /// the same one for every step.
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
    /// Last block of the range, in place of the default: the L2 block of the newest dispute
    /// game on L1, the last block known to be committed to L1. `download` looks that game up
    /// once and records it in `anchor.json` in the state directory; every later run uses the
    /// recorded one, and `verify` checks the block against the game's claim (delete the file
    /// to move to a newer game).
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_LAST_BLOCK",
        conflicts_with = "legacy_only"
    )]
    pub(crate) last_block: Option<u64>,
    /// With `--last-block`: the trusted hash of that block. Every block is verified by the
    /// chain of parent hashes down from it.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_ANCHOR_HASH",
        requires = "last_block"
    )]
    pub(crate) anchor_hash: Option<B256>,
    /// With `--last-block`, go without `--anchor-hash`: the range is then only checked to be
    /// one chain, not to be the canonical one.
    #[arg(
        long,
        global = true,
        env = "OP_INDEXER_IMPORT_ALLOW_UNANCHORED_TOP",
        requires = "last_block"
    )]
    pub(crate) allow_unanchored_top: bool,
    /// Import only OP Mainnet's blocks before Bedrock: the range ends at block 105235062,
    /// checked against its known hash. Needs no lookup on L1.
    #[arg(long, global = true, env = "OP_INDEXER_IMPORT_LEGACY_ONLY")]
    pub(crate) legacy_only: bool,
    /// Does nothing: ending the range at the newest dispute game is the default. Accepted so
    /// that earlier command lines keep working.
    #[arg(long, global = true, hide = true)]
    pub(crate) latest_game: bool,
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
    /// `HyperSync` endpoint of the L1 chain the dispute games are on (default: Ethereum
    /// Mainnet), for the lookup of the range's last block. Uses the same API token.
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_L1_ENDPOINT",
        default_value = "https://eth.hypersync.xyz"
    )]
    pub(crate) l1_endpoint: String,
    /// The chain's `DisputeGameFactory` on L1 (default: OP Mainnet's).
    #[arg(
        long,
        env = "OP_INDEXER_IMPORT_DISPUTE_GAME_FACTORY",
        default_value_t = OP_MAINNET_DISPUTE_GAME_FACTORY
    )]
    pub(crate) dispute_game_factory: Address,
    /// The game type whose games are used; it must be a fault dispute game.
    #[arg(long, env = "OP_INDEXER_IMPORT_GAME_TYPE", default_value_t = 0)]
    pub(crate) game_type: u32,
    /// Use only a game resolved in the proposer's favour, which is days older than the newest
    /// game.
    #[arg(long, env = "OP_INDEXER_IMPORT_RESOLVED_ONLY")]
    pub(crate) resolved_only: bool,
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
