//! Configuration from `OP_INDEXER_*` environment variables.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::B256;
use eyre::{WrapErr, ensure, eyre};
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_el::{ElConfig, PeerConfig};
use op_indexer_p2p::{Bootnode, NetworkConfig};
use op_indexer_primitives::{ChainIdentity, ExecutionPeer};
use op_indexer_storage::{ArchiveConfig, RedisConfig, StorageConfig};
use op_indexer_stream::StreamConfig;

const DEFAULT_CHAIN_ID: u64 = OP_MAINNET.chain_id;
const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9222);
const DEFAULT_MAX_PEERS: u32 = 30;
const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
/// Directory of the local block archive, inside the data directory.
const ARCHIVE_DIR: &str = "archive";
const SYNC_VAR: &str = "OP_INDEXER_EL_SYNC";
const L1_ENABLED_VAR: &str = "OP_INDEXER_L1_ENABLED";
const L1_CHECKPOINT_VAR: &str = "OP_INDEXER_L1_CHECKPOINT";
/// Next to the usual beacon p2p port, 9000.
const DEFAULT_L1_BEACON_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9001);
/// The port next to the execution network's.
const DEFAULT_L1_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 30304);
/// Local only: the stream has no authentication. Clear of the p2p (9222), execution (30303),
/// L1 (30304, 9001) and Redis (6379) ports.
const DEFAULT_STREAM_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50051);
const DEFAULT_STREAM_MAX_SUBSCRIPTIONS: usize = 64;
const DEFAULT_STREAM_MAX_FLIGHTS: usize = 8;

/// What the environment says about the execution network. The rest of its configuration,
/// the peers saved by earlier runs, comes from the node store.
#[derive(Debug)]
pub(crate) struct ElSettings {
    chain: &'static ChainSpec,
    listen_addr: SocketAddr,
    bootnodes: Vec<String>,
    advertised_addr: Option<SocketAddr>,
    max_sessions: usize,
}

impl ElSettings {
    /// The execution network's configuration, with the peers that served earlier runs.
    pub(crate) fn into_config(self, saved_peers: Vec<ExecutionPeer>) -> ElConfig {
        ElConfig {
            chain: self.chain,
            listen_addr: self.listen_addr,
            bootnodes: self.bootnodes,
            saved_peers,
            advertised_addr: self.advertised_addr,
            max_sessions: self.max_sessions,
        }
    }
}

/// What the environment says about the L1 side.
#[derive(Debug, Clone, Copy)]
pub(crate) struct L1Settings {
    /// Listen address of the L1 execution p2p node, UDP and TCP.
    pub(crate) listen_addr: SocketAddr,
    /// Listen address of the beacon light client, UDP and TCP.
    pub(crate) beacon_listen_addr: SocketAddr,
    /// The public socket announced in the L1 node record, when the operator knows it.
    pub(crate) advertised_addr: Option<SocketAddr>,
    /// The finalized beacon block root the light client starts from.
    pub(crate) checkpoint: B256,
}

/// Process configuration.
#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) network: NetworkConfig,
    /// The execution network, which fetches receipts; `None` when it is disabled.
    pub(crate) el: Option<ElSettings>,
    /// Whether to fetch the blocks between the archive's last block and the chain's head
    /// from execution peers.
    pub(crate) sync: bool,
    /// The L1 side, which learns what L1 commits to; `None` when it is disabled.
    pub(crate) l1: Option<L1Settings>,
    /// Unsafe store (Redis) and the block archive (fjall), the committed store.
    pub(crate) storage: StorageConfig,
    /// Directory for local state: the node store (`node/`) and the block archive (`archive/`).
    pub(crate) data_dir: PathBuf,
    /// The gRPC stream of the chain to consumers.
    pub(crate) stream: StreamConfig,
}

impl Config {
    /// Reads the configuration:
    ///
    /// - `OP_INDEXER_CHAIN_ID`: L2 chain id, one of [`ChainSpec::ALL`] (default 10, OP
    ///   Mainnet).
    /// - `OP_INDEXER_LISTEN_ADDR`: p2p listen socket, TCP and UDP (default `0.0.0.0:9222`).
    /// - `OP_INDEXER_BOOTNODES`: comma-separated `enr:` records or `enode://` URLs (default: the
    ///   chain's bootnodes).
    /// - `OP_INDEXER_ADVERTISED_ADDR`: public socket (IP and port, the same for TCP and UDP)
    ///   the consensus-layer node record advertises (default: unset, the address
    ///   peers observe). Set it behind NAT or in a container, with the port forwarded.
    /// - `OP_INDEXER_MAX_PEERS`: maximum connections, inbound and outbound (default 30).
    /// - `OP_INDEXER_DATA_DIR`: node state directory (default `data-<chain>`: `data-op` or
    ///   `data-unichain`, so two chains on one host never share one by default).
    /// - `OP_INDEXER_REDIS_URL`: unsafe store (default `redis://127.0.0.1:6379`).
    /// - `OP_INDEXER_EL_ENABLED`: `true` to join the execution p2p network (devp2p) and fetch
    ///   the receipts gossip does not carry (default `false`: blocks stay without receipts).
    ///   The variables below only apply when it is enabled.
    /// - `OP_INDEXER_EL_LISTEN_ADDR`: execution p2p listen socket, TCP and UDP (default
    ///   `0.0.0.0:30303`).
    /// - `OP_INDEXER_EL_BOOTNODES`: comma-separated `enr:` records or `enode://` URLs
    ///   (default: the chain's execution bootnodes).
    /// - `OP_INDEXER_EL_ADVERTISED_ADDR`: public socket (IP and port, the same for TCP and
    ///   UDP) announced in the execution node record, for a node behind NAT or in a container
    ///   (default: unset, the address other peers observe).
    /// - `OP_INDEXER_EL_MAX_SESSIONS`: execution sessions kept in each direction, dialed and
    ///   accepted; one more is kept for an op-p2p-indexer (default 4). Full nodes ration their
    ///   slots: keep it low.
    /// - `OP_INDEXER_EL_SYNC`: `true` to fetch from execution peers the blocks between the
    ///   archive's last block and the chain that gossip cannot fill, into the archive
    ///   (default `false`), in rounds from the block after the archive's last
    ///   one (block 0 on an empty archive) to a trusted anchor that every fetched block is
    ///   verified against. With the L1 side a round runs while the archive is 1,024 blocks or
    ///   more below the safe head (the unsafe store's read limit) and is anchored on the safe
    ///   head; without it, while the archive is that far below the gossiped head, anchored on
    ///   the sequencer-signed block 64 below it. Without the L1 side the archive then holds
    ///   blocks L1 has not committed (the stream marks them unsafe), and an unsafe reorg deeper
    ///   than 64 blocks leaves it on a dead branch, which only rebuilding the archive repairs.
    ///   Promotion extends the archive otherwise. A restart continues after the archive's last
    ///   block. Needs the execution network. Required with the L1 side.
    /// - `OP_INDEXER_L1_ENABLED`: `true` to follow Ethereum L1 for what it commits to
    ///   (default `false`: no safe or finalized head, nothing is promoted). The node then
    ///   runs a beacon light client, which follows Ethereum's finality from the checkpoint,
    ///   and joins L1's execution p2p network to read the dispute games of the chain from
    ///   the L1 blocks the light client vouches for; it checks each claim against its own
    ///   block and promotes on a match. Needs `OP_INDEXER_L1_CHECKPOINT`, and the range sync
    ///   (`OP_INDEXER_EL_SYNC=true`, so the execution network too): promotion records only
    ///   what the archive holds, and range sync fills the gaps gossip leaves. If no peer serves
    ///   the checkpoint any more the node stops and asks for a newer one.
    /// - `OP_INDEXER_L1_CHECKPOINT`: root of a recent finalized beacon block, from a source
    ///   you trust: the one value the L1 side takes on trust, everything after it is
    ///   verified.
    /// - `OP_INDEXER_L1_LISTEN_ADDR`: L1 execution p2p listen socket, TCP and UDP (default
    ///   `0.0.0.0:30304`; it must differ from `OP_INDEXER_EL_LISTEN_ADDR`).
    /// - `OP_INDEXER_L1_BEACON_LISTEN_ADDR`: listen socket of the beacon light client, TCP
    ///   and UDP (default `0.0.0.0:9001`; it must differ from the other listen addresses).
    /// - `OP_INDEXER_L1_ADVERTISED_ADDR`: public socket (IP and port, the same for TCP and
    ///   UDP) announced in the L1 node record, as `OP_INDEXER_EL_ADVERTISED_ADDR` is for the
    ///   execution network (default: unset, the address other peers observe). Worth setting
    ///   on a server with a public address: L1 peers have few free slots, and a node they
    ///   can dial gets sessions it would not get by dialing.
    /// - `OP_INDEXER_STREAM_LISTEN_ADDR`: gRPC listen socket of the stream (default
    ///   `127.0.0.1:50051`, local only: the stream has no authentication).
    /// - `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS`: stream subscriptions at once (default 64).
    /// - `OP_INDEXER_STREAM_MAX_FLIGHTS`: Arrow Flight `DoGet` streams at once, on the same
    ///   listener (default 8).
    pub(crate) fn from_env() -> eyre::Result<Self> {
        let chain_id = parse_var("OP_INDEXER_CHAIN_ID")?.unwrap_or(DEFAULT_CHAIN_ID);
        let chain = ChainSpec::by_chain_id(chain_id)
            .ok_or_else(|| eyre!("unsupported chain id {chain_id}"))?;
        let bootnodes = match var("OP_INDEXER_BOOTNODES") {
            Some(list) => list
                .split(',')
                .map(|node| parse_bootnode(node.trim()))
                .collect::<eyre::Result<Vec<_>>>(),
            None => chain.bootnodes().map(parse_bootnode).collect(),
        }?;

        let data_dir =
            PathBuf::from(var("OP_INDEXER_DATA_DIR").unwrap_or_else(|| chain.default_data_dir()));
        let archive = ArchiveConfig {
            path: data_dir.join(ARCHIVE_DIR),
        };

        let el = el_settings(chain)?;
        let sync = parse_var(SYNC_VAR)?.unwrap_or(false);
        ensure!(
            !sync || el.is_some(),
            "{SYNC_VAR} needs the execution network: set OP_INDEXER_EL_ENABLED=true"
        );
        let l1 = l1_settings()?;
        // Promotion records only what the archive holds; with the L1 side, range sync is what
        // fills a gap gossip left, so without it the committed chain would stop at the first.
        ensure!(
            l1.is_none() || sync,
            "{L1_ENABLED_VAR} needs {SYNC_VAR}=true: range sync fills the gaps in the archive \
             that promotion cannot, and the committed heads never pass the archive"
        );

        let stream = StreamConfig {
            listen_addr: parse_var("OP_INDEXER_STREAM_LISTEN_ADDR")?
                .unwrap_or(DEFAULT_STREAM_LISTEN_ADDR),
            max_subscriptions: parse_var("OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS")?
                .unwrap_or(DEFAULT_STREAM_MAX_SUBSCRIPTIONS),
            max_flights: parse_var("OP_INDEXER_STREAM_MAX_FLIGHTS")?
                .unwrap_or(DEFAULT_STREAM_MAX_FLIGHTS),
            block_time: Duration::from_secs(chain.block_time_secs),
            receipts: el.is_some(),
        };
        Ok(Self {
            l1,
            stream,
            el,
            sync,
            network: NetworkConfig {
                chain,
                listen_addr: parse_var("OP_INDEXER_LISTEN_ADDR")?.unwrap_or(DEFAULT_LISTEN_ADDR),
                bootnodes,
                advertised_addr: parse_var("OP_INDEXER_ADVERTISED_ADDR")?,
                max_peers: parse_var("OP_INDEXER_MAX_PEERS")?.unwrap_or(DEFAULT_MAX_PEERS),
            },
            storage: StorageConfig {
                redis: RedisConfig {
                    url: var_or("OP_INDEXER_REDIS_URL", DEFAULT_REDIS_URL),
                    canyon_time: chain.canyon_time,
                },
                archive,
                chain: ChainIdentity {
                    chain_id,
                    genesis_hash: chain.genesis_hash,
                },
            },
            data_dir,
        })
    }
}

/// Reads the execution network's settings; `None` unless it is enabled.
fn el_settings(chain: &'static ChainSpec) -> eyre::Result<Option<ElSettings>> {
    if !parse_var("OP_INDEXER_EL_ENABLED")?.unwrap_or(false) {
        return Ok(None);
    }
    Ok(Some(ElSettings {
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
        advertised_addr: parse_var("OP_INDEXER_EL_ADVERTISED_ADDR")?,
        max_sessions: max_sessions()?,
    }))
}

/// `OP_INDEXER_EL_MAX_SESSIONS`, at least 1.
fn max_sessions() -> eyre::Result<usize> {
    let sessions = parse_var("OP_INDEXER_EL_MAX_SESSIONS")?;
    let sessions = sessions.unwrap_or(PeerConfig::DEFAULT_MAX_SESSIONS);
    eyre::ensure!(
        sessions > 0,
        "OP_INDEXER_EL_MAX_SESSIONS must be at least 1"
    );
    Ok(sessions)
}

/// Reads the L1 side's settings; `None` unless it is enabled.
fn l1_settings() -> eyre::Result<Option<L1Settings>> {
    if !parse_var(L1_ENABLED_VAR)?.unwrap_or(false) {
        return Ok(None);
    }
    let checkpoint = parse_var(L1_CHECKPOINT_VAR)?.ok_or_else(|| {
        eyre!("{L1_ENABLED_VAR} needs {L1_CHECKPOINT_VAR}: a recent finalized beacon block root")
    })?;
    Ok(Some(L1Settings {
        listen_addr: parse_var("OP_INDEXER_L1_LISTEN_ADDR")?.unwrap_or(DEFAULT_L1_LISTEN_ADDR),
        beacon_listen_addr: parse_var("OP_INDEXER_L1_BEACON_LISTEN_ADDR")?
            .unwrap_or(DEFAULT_L1_BEACON_LISTEN_ADDR),
        advertised_addr: parse_var("OP_INDEXER_L1_ADVERTISED_ADDR")?,
        checkpoint,
    }))
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
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
