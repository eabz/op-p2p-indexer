//! Configuration from `OP_INDEXER_*` environment variables.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use eyre::{WrapErr, eyre};
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_el::ElConfig;
use op_indexer_p2p::{Bootnode, NetworkConfig};
use op_indexer_storage::{
    ArchiveConfig, ArchiveRetention, ClickHouseConfig, RedisConfig, StorageConfig,
};

const DEFAULT_CHAIN_ID: u64 = OP_MAINNET.chain_id;
const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9222);
const DEFAULT_MAX_PEERS: u32 = 30;
/// Sessions the execution network keeps in each direction.
const DEFAULT_EL_MAX_SESSIONS: usize = 8;
const DEFAULT_DATA_DIR: &str = "data";
const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:8123";
const DEFAULT_CLICKHOUSE_DATABASE: &str = "op_indexer";
const DEFAULT_CLICKHOUSE_USER: &str = "indexer";
/// 30 days of 2-second blocks (1296000).
const DEFAULT_ARCHIVE_RETENTION_BLOCKS: u64 = 30 * 24 * 60 * 60 / 2;
/// Directory of the local block archive, inside the data directory.
const ARCHIVE_DIR: &str = "archive";
const ARCHIVE_RETENTION_VAR: &str = "OP_INDEXER_ARCHIVE_RETENTION_BLOCKS";
/// Value of [`ARCHIVE_RETENTION_VAR`] that keeps every block.
const ARCHIVE_RETENTION_ALL: &str = "all";

/// Process configuration.
#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) network: NetworkConfig,
    /// The execution network, which fetches receipts; `None` when it is disabled.
    pub(crate) el: Option<ElConfig>,
    /// Unsafe store (Redis), committed store (ClickHouse) and local block archive (fjall).
    pub(crate) storage: StorageConfig,
    /// Directory for local state: the node store (`node/`) and the block archive (`archive/`).
    pub(crate) data_dir: PathBuf,
}

impl Config {
    /// Reads the configuration:
    ///
    /// - `OP_INDEXER_CHAIN_ID`: L2 chain id (default 10, OP Mainnet).
    /// - `OP_INDEXER_LISTEN_ADDR`: p2p listen socket, TCP and UDP (default `0.0.0.0:9222`).
    /// - `OP_INDEXER_BOOTNODES`: comma-separated `enr:` records or `enode://` URLs (default: the
    ///   chain's bootnodes).
    /// - `OP_INDEXER_MAX_PEERS`: maximum connections, inbound and outbound (default 30).
    /// - `OP_INDEXER_DATA_DIR`: node state directory (default `data`).
    /// - `OP_INDEXER_REDIS_URL`: unsafe store (default `redis://127.0.0.1:6379`).
    /// - `OP_INDEXER_CLICKHOUSE_URL`: committed store, HTTP interface (default
    ///   `http://127.0.0.1:8123`).
    /// - `OP_INDEXER_CLICKHOUSE_DATABASE`: ClickHouse database (default `op_indexer`).
    /// - `OP_INDEXER_CLICKHOUSE_USER`: ClickHouse user (default `indexer`).
    /// - `OP_INDEXER_CLICKHOUSE_PASSWORD`: ClickHouse password (default: none). Never logged.
    /// - `OP_INDEXER_ARCHIVE_RETENTION_BLOCKS`: blocks kept in the local archive, the
    ///   `archive` directory inside the data directory: a block count (default 1296000, 30
    ///   days), `all` to keep every block, or `0` to disable the archive.
    /// - `OP_INDEXER_EL_ENABLED`: `true` to join the execution p2p network (devp2p) and fetch
    ///   the receipts gossip does not carry (default `false`: blocks stay without receipts).
    ///   The variables below only apply when it is enabled.
    /// - `OP_INDEXER_EL_LISTEN_ADDR`: execution p2p listen socket, TCP and UDP (default
    ///   `0.0.0.0:30303`).
    /// - `OP_INDEXER_EL_BOOTNODES`: comma-separated `enr:` records or `enode://` URLs
    ///   (default: the chain's execution bootnodes).
    /// - `OP_INDEXER_EL_MAX_SESSIONS`: execution peers kept, as dialed sessions and again as
    ///   accepted ones (default 8).
    /// - `OP_INDEXER_EL_ADVERTISED_ADDR`: public socket (IP and port, the same for TCP and
    ///   UDP) announced in the execution node record, for a node behind NAT or in a container
    ///   (default: unset, the address other peers observe).
    pub(crate) fn from_env() -> eyre::Result<Self> {
        let chain_id = parse_var("OP_INDEXER_CHAIN_ID")?.unwrap_or(DEFAULT_CHAIN_ID);
        let chain = ChainSpec::by_chain_id(chain_id)
            .ok_or_else(|| eyre!("unsupported chain id {chain_id}"))?;
        let bootnodes_override = var("OP_INDEXER_BOOTNODES");
        let bootnodes = match &bootnodes_override {
            Some(list) => list.split(',').map(str::trim).collect(),
            None => chain.bootnodes.to_vec(),
        }
        .into_iter()
        .map(parse_bootnode)
        .collect::<eyre::Result<Vec<Bootnode>>>()?;

        let data_dir = PathBuf::from(var_or("OP_INDEXER_DATA_DIR", DEFAULT_DATA_DIR));
        let archive = archive_retention()?.map(|retention| ArchiveConfig {
            path: data_dir.join(ARCHIVE_DIR),
            retention,
        });

        Ok(Self {
            el: el_config(chain)?,
            network: NetworkConfig {
                chain,
                listen_addr: parse_var("OP_INDEXER_LISTEN_ADDR")?.unwrap_or(DEFAULT_LISTEN_ADDR),
                bootnodes,
                max_peers: parse_var("OP_INDEXER_MAX_PEERS")?.unwrap_or(DEFAULT_MAX_PEERS),
            },
            storage: StorageConfig {
                redis: RedisConfig {
                    url: var_or("OP_INDEXER_REDIS_URL", DEFAULT_REDIS_URL),
                },
                clickhouse: ClickHouseConfig {
                    url: var_or("OP_INDEXER_CLICKHOUSE_URL", DEFAULT_CLICKHOUSE_URL),
                    database: var_or(
                        "OP_INDEXER_CLICKHOUSE_DATABASE",
                        DEFAULT_CLICKHOUSE_DATABASE,
                    ),
                    user: var_or("OP_INDEXER_CLICKHOUSE_USER", DEFAULT_CLICKHOUSE_USER),
                    password: var("OP_INDEXER_CLICKHOUSE_PASSWORD"),
                },
                archive,
                chain_id,
            },
            data_dir,
        })
    }
}

/// Reads the execution network's configuration; `None` unless it is enabled.
fn el_config(chain: &'static ChainSpec) -> eyre::Result<Option<ElConfig>> {
    if !parse_var("OP_INDEXER_EL_ENABLED")?.unwrap_or(false) {
        return Ok(None);
    }
    let max_sessions = parse_var("OP_INDEXER_EL_MAX_SESSIONS")?.unwrap_or(DEFAULT_EL_MAX_SESSIONS);
    Ok(Some(ElConfig {
        chain,
        listen_addr: parse_var("OP_INDEXER_EL_LISTEN_ADDR")?
            .unwrap_or(ElConfig::DEFAULT_LISTEN_ADDR),
        // Parsed by the execution network; an empty list means the chain's bootnodes.
        bootnodes: var("OP_INDEXER_EL_BOOTNODES")
            .map(|list| {
                list.split(',')
                    .map(str::trim)
                    .filter(|node| !node.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        max_outbound_sessions: max_sessions,
        max_inbound_sessions: max_sessions,
        // Filled by the binary from the node store.
        saved_peers: Vec::new(),
        advertised_addr: parse_var("OP_INDEXER_EL_ADVERTISED_ADDR")?,
    }))
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

/// Reads the archive retention: a block count, `all`, or `0` for no archive (`None`).
fn archive_retention() -> eyre::Result<Option<ArchiveRetention>> {
    let Some(value) = var(ARCHIVE_RETENTION_VAR) else {
        return Ok(Some(ArchiveRetention::Blocks(
            DEFAULT_ARCHIVE_RETENTION_BLOCKS,
        )));
    };
    if value.eq_ignore_ascii_case(ARCHIVE_RETENTION_ALL) {
        return Ok(Some(ArchiveRetention::All));
    }
    let blocks: u64 = value.parse().wrap_err_with(|| {
        format!("{ARCHIVE_RETENTION_VAR} is invalid: {value} (expected a block count, `all` or 0)")
    })?;
    Ok((blocks > 0).then_some(ArchiveRetention::Blocks(blocks)))
}

fn var_or(name: &str, default: &str) -> String {
    var(name).unwrap_or_else(|| default.to_owned())
}

fn parse_bootnode(bootnode: &str) -> eyre::Result<Bootnode> {
    bootnode
        .parse()
        .wrap_err_with(|| format!("invalid bootnode {bootnode}"))
}

fn parse_var<T>(name: &str) -> eyre::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    var(name)
        .map(|value| {
            value
                .parse()
                .wrap_err_with(|| format!("{name} is invalid: {value}"))
        })
        .transpose()
}
