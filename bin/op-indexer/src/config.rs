//! Configuration from `OP_INDEXER_*` environment variables.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use eyre::{WrapErr, eyre};
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_p2p::{Bootnode, NetworkConfig};

const DEFAULT_CHAIN_ID: u64 = OP_MAINNET.chain_id;
const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9222);
const DEFAULT_MAX_PEERS: u32 = 30;
const DEFAULT_DATA_DIR: &str = "data";

/// Process configuration.
#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) network: NetworkConfig,
    /// Directory for node state (`node.redb`).
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

        Ok(Self {
            network: NetworkConfig {
                chain,
                listen_addr: parse_var("OP_INDEXER_LISTEN_ADDR")?.unwrap_or(DEFAULT_LISTEN_ADDR),
                bootnodes,
                max_peers: parse_var("OP_INDEXER_MAX_PEERS")?.unwrap_or(DEFAULT_MAX_PEERS),
            },
            data_dir: var("OP_INDEXER_DATA_DIR")
                .unwrap_or_else(|| DEFAULT_DATA_DIR.to_owned())
                .into(),
        })
    }
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
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
