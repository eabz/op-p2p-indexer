//! Configuration from `OP_INDEXER_*` environment variables.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy_primitives::B256;
use eyre::{WrapErr, ensure, eyre};
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_el::{ElConfig, PeerConfig};
use op_indexer_p2p::{Bootnode, NetworkConfig};
use op_indexer_primitives::{ChainIdentity, ExecutionPeer};
use op_indexer_storage::{ArchiveConfig, StorageConfig, UnsafeConfig};
use op_indexer_stream::StreamConfig;

const DEFAULT_CHAIN_ID: u64 = OP_MAINNET.chain_id;
const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9222);
const DEFAULT_MAX_PEERS: u32 = 30;
/// The memory the unsafe chain's blocks may take: about a day of a busy chain's blocks
/// (measured in docs/storage.md section 3).
const DEFAULT_UNSAFE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Directory of the local block archive, inside the data directory.
const ARCHIVE_DIR: &str = "archive";
/// The unsafe chain's journal, inside the data directory.
const UNSAFE_DIR: &str = "unsafe";
/// Directory of the node store (identity and known peers), inside the data directory.
pub(crate) const NODE_DIR: &str = "node";
/// The data directory's default before it was named after the chain.
const OLD_DATA_DIR: &str = "data";
const SYNC_VAR: &str = "OP_INDEXER_EL_SYNC";
const L1_ENABLED_VAR: &str = "OP_INDEXER_L1_ENABLED";
const L1_CHECKPOINT_VAR: &str = "OP_INDEXER_L1_CHECKPOINT";
/// Next to the usual beacon p2p port, 9000.
const DEFAULT_L1_BEACON_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9001);
/// The port next to the execution network's.
const DEFAULT_L1_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 30304);
/// Local by default; API keys are optional. Clear of the p2p (9222), execution (30303),
/// and L1 (30304, 9001) ports.
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

/// Defaults for the node's intended role. Explicit capability variables override these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Profile {
    /// Gossip and verified execution receipts, without historical range sync or L1 tracking.
    Live,
    /// Execution receipts, historical range sync and checkpoint-based L1 tracking.
    Archive,
    /// Archive capabilities using the server binary's object-store-backed history.
    Fleet,
}

impl Profile {
    fn from_env() -> eyre::Result<Option<Self>> {
        match var("OP_INDEXER_PROFILE").as_deref() {
            None => Ok(None),
            Some("live") => Ok(Some(Self::Live)),
            Some("archive") => Ok(Some(Self::Archive)),
            Some("fleet") => Ok(Some(Self::Fleet)),
            Some(value) => Err(eyre!(
                "invalid OP_INDEXER_PROFILE: {value}; expected live, archive or fleet"
            )),
        }
    }

    /// Returns the name used in configuration and startup logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Archive => "archive",
            Self::Fleet => "fleet",
        }
    }

    const fn history(self) -> bool {
        matches!(self, Self::Archive | Self::Fleet)
    }
}

/// Process configuration.
#[derive(Debug)]
pub struct Config {
    profile: Option<Profile>,
    pub(crate) network: NetworkConfig,
    /// The execution network, which fetches receipts; `None` when it is disabled.
    pub(crate) el: Option<ElSettings>,
    /// Whether to fetch the blocks between the archive's last block and the chain's head
    /// from execution peers.
    pub(crate) sync: bool,
    /// The L1 side, which learns what L1 commits to; `None` when it is disabled.
    pub(crate) l1: Option<L1Settings>,
    /// The unsafe chain (in memory, journaled to fjall) and the block archive (fjall), the
    /// committed store.
    pub(crate) storage: StorageConfig,
    /// Directory for local state: the node store (`node/`), the block archive (`archive/`) and
    /// the unsafe chain's journal (`unsafe/`).
    pub(crate) data_dir: PathBuf,
    /// The gRPC stream of the chain to consumers.
    pub stream: StreamConfig,
}

impl Config {
    /// Reads the configuration:
    ///
    /// - `OP_INDEXER_PROFILE`: optional `live` (execution receipts), `archive` (receipts,
    ///   range sync and L1 tracking), or `fleet` (archive defaults, requires the server binary
    ///   and its object storage). Archive and fleet need `OP_INDEXER_L1_CHECKPOINT` while L1
    ///   is enabled. Explicit capability variables override profile defaults; dependency
    ///   validation still applies. Unset preserves the legacy defaults (all three off).
    /// - `OP_INDEXER_CHAIN_ID`: L2 chain id, one of [`ChainSpec::ALL`] (default 10, OP
    ///   Mainnet).
    /// - `OP_INDEXER_P2P_LISTEN_ADDR`: p2p listen socket, TCP and UDP (default `0.0.0.0:9222`).
    /// - `OP_INDEXER_P2P_BOOTNODES`: comma-separated `enr:` records or `enode://` URLs (default: the
    ///   chain's bootnodes).
    /// - `OP_INDEXER_P2P_ADVERTISED_ADDR`: public socket (IP and port, the same for TCP and UDP)
    ///   the consensus-layer node record advertises (default: unset, the address
    ///   peers observe). Set it behind NAT or in a container, with the port forwarded.
    /// - `OP_INDEXER_P2P_MAX_PEERS`: maximum connections, inbound and outbound (default 30).
    /// - `OP_INDEXER_DATA_DIR`: node state directory (default `data-<chain>`: `data-op` or
    ///   `data-unichain`, so two chains on one host never share one by default). Without it,
    ///   the node refuses to start while `data`, an earlier build's default, holds an archive
    ///   or a node store and `data-<chain>` does not exist.
    /// - `OP_INDEXER_UNSAFE_MAX_BYTES`: memory the unsafe chain's blocks may take, in bytes
    ///   (default 2 GiB); past it the lowest heights leave. Its journal is `unsafe/` in the data
    ///   directory, replayed on start.
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
    ///   the checkpoint (nor the saved one) any more the node stops and asks for a newer one.
    /// - `OP_INDEXER_L1_CHECKPOINT`: root of a recent finalized beacon block, from a source
    ///   you trust: the one value the L1 side takes on trust, everything after it is
    ///   verified. Used on the first start, or when it is newer than the saved one: the node
    ///   saves the newest finalized block the light client verified from it, and later starts
    ///   bootstrap from that (it adds no trust), so the configured root may grow old.
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
    ///   `127.0.0.1:50051`, local only; API keys are configured separately).
    /// - `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS`: stream subscriptions at once (default 64).
    /// - `OP_INDEXER_STREAM_MAX_FLIGHTS`: Arrow Flight `DoGet` streams at once, on the same
    ///   listener (default 8).
    /// - `OP_INDEXER_STREAM_API_KEYS`: comma-separated API keys; with any set, a gRPC or Flight
    ///   request is served only with one of them as `authorization: Bearer <key>` (default:
    ///   none, no check). Never logged.
    ///
    /// # Errors
    ///
    /// Returns an error if a variable is invalid or the settings contradict each other.
    pub fn from_env() -> eyre::Result<Self> {
        let profile = Profile::from_env()?;
        let history = profile.is_some_and(Profile::history);
        let chain_id = parse_var("OP_INDEXER_CHAIN_ID")?.unwrap_or(DEFAULT_CHAIN_ID);
        let chain = ChainSpec::by_chain_id(chain_id)
            .ok_or_else(|| eyre!("unsupported chain id {chain_id}"))?;
        let bootnodes = match var("OP_INDEXER_P2P_BOOTNODES") {
            Some(list) => list
                .split(',')
                .map(|node| parse_bootnode(node.trim()))
                .collect::<eyre::Result<Vec<_>>>(),
            None => chain.consensus_bootnodes().map(parse_bootnode).collect(),
        }?;

        let data_dir = match var("OP_INDEXER_DATA_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => default_data_dir(chain)?,
        };
        let archive = ArchiveConfig {
            path: data_dir.join(ARCHIVE_DIR),
        };

        let el = el_settings(chain, profile.is_some())?;
        let sync = parse_var(SYNC_VAR)?.unwrap_or(history);
        ensure!(
            !sync || el.is_some(),
            "{SYNC_VAR} needs the execution network: set OP_INDEXER_EL_ENABLED=true"
        );
        let l1 = l1_settings(history)?;
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
            sync,
            api_keys: var("OP_INDEXER_STREAM_API_KEYS")
                .map(|keys| keys.split(',').map(|key| key.trim().to_owned()).collect())
                .unwrap_or_default(),
        };
        Ok(Self {
            profile,
            l1,
            stream,
            el,
            sync,
            network: NetworkConfig {
                chain,
                listen_addr: parse_var("OP_INDEXER_P2P_LISTEN_ADDR")?
                    .unwrap_or(DEFAULT_LISTEN_ADDR),
                bootnodes,
                advertised_addr: parse_var("OP_INDEXER_P2P_ADVERTISED_ADDR")?,
                max_peers: parse_var("OP_INDEXER_P2P_MAX_PEERS")?.unwrap_or(DEFAULT_MAX_PEERS),
            },
            storage: StorageConfig {
                unsafe_chain: UnsafeConfig {
                    path: data_dir.join(UNSAFE_DIR),
                    canyon_time: chain.canyon_time(),
                    max_bytes: parse_var("OP_INDEXER_UNSAFE_MAX_BYTES")?
                        .unwrap_or(DEFAULT_UNSAFE_MAX_BYTES),
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

impl Config {
    /// Returns the selected profile, or `None` for legacy environment-only defaults.
    #[must_use]
    pub const fn profile(&self) -> Option<Profile> {
        self.profile
    }

    pub(crate) fn log_capabilities(&self) {
        tracing::info!(
            profile = self.profile.map_or("legacy", Profile::as_str),
            chain_id = self.network.chain.chain_id,
            execution_receipts = self.el.is_some(),
            range_sync = self.sync,
            l1_tracking = self.l1.is_some(),
            stream = %self.stream.listen_addr,
            "node capabilities"
        );
    }

    /// The chain the node runs.
    #[must_use]
    pub const fn chain(&self) -> &'static ChainSpec {
        self.network.chain
    }

    /// The stores' configuration: the committed archive's path and the chain it holds.
    #[must_use]
    pub const fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    /// The directory of the node's local state.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

/// The default data directory, `data-<chain>`. Earlier builds defaulted to `data`: a node that
/// ran with it would start on an empty directory with a new identity, so that is refused while
/// `data` holds an archive or a node store and `data-<chain>` does not exist.
fn default_data_dir(chain: &ChainSpec) -> eyre::Result<PathBuf> {
    let dir = PathBuf::from(chain.default_data_dir());
    let exists = dir
        .try_exists()
        .wrap_err_with(|| format!("failed to look for {}", dir.display()))?;
    if exists {
        return Ok(dir);
    }
    let old = Path::new(OLD_DATA_DIR);
    let mut old_used = false;
    for held in [ARCHIVE_DIR, NODE_DIR] {
        old_used |= old
            .join(held)
            .try_exists()
            .wrap_err_with(|| format!("failed to look into {OLD_DATA_DIR}"))?;
    }
    ensure!(
        !old_used,
        "{OLD_DATA_DIR} holds a node's state from an earlier build, whose default data \
         directory it was: move {OLD_DATA_DIR} to {} (or set OP_INDEXER_DATA_DIR={OLD_DATA_DIR})",
        dir.display()
    );
    Ok(dir)
}

/// Reads the execution network's settings; `None` unless it is enabled.
fn el_settings(
    chain: &'static ChainSpec,
    enabled_by_default: bool,
) -> eyre::Result<Option<ElSettings>> {
    if !parse_var("OP_INDEXER_EL_ENABLED")?.unwrap_or(enabled_by_default) {
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
fn l1_settings(enabled_by_default: bool) -> eyre::Result<Option<L1Settings>> {
    if !parse_var(L1_ENABLED_VAR)?.unwrap_or(enabled_by_default) {
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
