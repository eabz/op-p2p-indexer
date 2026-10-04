//! Peer discovery over discv5.
//!
//! OP Stack nodes advertise an `opstack` ENR entry, an RLP string of
//! `uvarint(chain_id) ++ uvarint(version)`, so peers of other chains sharing the discovery
//! network can be filtered out. We advertise our own entry, seed the table from the bootnodes
//! (requesting the ENR of `enode://` ones),
//! run random lookups, and pass dialable peers of our chain to the network.
//! See <https://specs.optimism.io/protocol/rollup-node-p2p.html#discv5>.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use alloy_primitives::ChainId;
use bytes::Bytes;
use discv5::{ConfigBuilder, Discv5, Enr, ListenConfig};
use enr::{CombinedKey, CombinedPublicKey, EnrPublicKey, NodeId};
use libp2p::futures::future::join_all;
use libp2p::identity::{PublicKey, secp256k1};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use unsigned_varint::encode as uvarint;

use crate::Bootnode;
use crate::metrics;

/// ENR key of the OP Stack entry.
const OPSTACK_ENR_KEY: &str = "opstack";
/// Version of the `opstack` ENR entry.
const OPSTACK_ENR_VERSION: u64 = 0;
/// Below this many peers on our block topics, discovery is aggressive: fewer peers than
/// gossipsub's mesh target leave our block meshes under-filled.
const TARGET_PEERS: usize = crate::gossip::MESH_TARGET;
/// While below [`TARGET_PEERS`]: shortest time between lookup rounds. A round itself takes
/// seconds, so rounds run nearly back to back (peers of our chain are a small share of the
/// network). Doubles, up to [`RELAXED_LOOKUP_INTERVAL`], while rounds report no new peer, so
/// seeing the same unreachable peers again does not keep discovery aggressive.
const LOOKUP_INTERVAL: Duration = Duration::from_secs(1);
/// While below [`TARGET_PEERS`]: random lookups run concurrently per round, each toward a
/// different region of the network.
const CONCURRENT_LOOKUPS: usize = 8;
/// At or above [`TARGET_PEERS`]: one lookup this often, to replace peers that drop.
const RELAXED_LOOKUP_INTERVAL: Duration = Duration::from_secs(30);
/// Limit for one lookup.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Limit for requesting an `enode://` bootnode's ENR.
const BOOTNODE_TIMEOUT: Duration = Duration::from_secs(10);
/// Reported peers remembered to tell new candidates from repeats. When full the set is cleared,
/// which at worst makes one round look productive.
const MAX_REPORTED_PEERS: usize = 4096;

/// Errors starting discovery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DiscoveryError {
    /// The identity key could not be converted to a discv5 key.
    #[error("invalid node identity key")]
    Key(#[source] enr::k256::ecdsa::Error),
    /// The local ENR could not be built or signed.
    #[error("failed to build the local ENR")]
    Enr(#[source] enr::Error),
    /// The discv5 service could not be created.
    #[error("failed to create discv5: {0}")]
    Create(&'static str),
    /// The discv5 service could not start (usually: the UDP port is taken).
    #[error("failed to start discv5: {0}")]
    Start(discv5::Error),
}

/// A discv5 node that finds OP Stack peers of one chain.
pub(crate) struct Discovery {
    discv5: Discv5,
    chain_id: ChainId,
    /// Dial addresses already sent to the network, at most [`MAX_REPORTED_PEERS`].
    reported: HashSet<Multiaddr>,
}

impl Discovery {
    /// Creates a discovery node on `listen` (UDP), with the same secp256k1 key as the libp2p
    /// identity. With `advertised`, the record carries that address and port, and is not
    /// rewritten from what peers report.
    pub(crate) fn new(
        keypair: &secp256k1::Keypair,
        listen: SocketAddr,
        advertised: Option<SocketAddr>,
        chain_id: ChainId,
    ) -> Result<Self, DiscoveryError> {
        let key = enr::k256::ecdsa::SigningKey::from_slice(&keypair.secret().to_bytes())
            .map(CombinedKey::from)
            .map_err(DiscoveryError::Key)?;

        // UDP (discovery) and TCP (libp2p) share the port, as in op-node. Without an advertised
        // address our IP is learned from peers (discv5 updates the ENR from PONG votes).
        let mut builder = Enr::builder();
        let public = advertised.unwrap_or(listen);
        if public.is_ipv6() {
            builder.udp6(public.port()).tcp6(public.port());
        } else {
            builder.udp4(public.port()).tcp4(public.port());
        }
        if let Some(advertised) = advertised {
            builder.ip(advertised.ip());
        }
        builder.add_value(OPSTACK_ENR_KEY, &opstack_entry(chain_id));
        // A record's `seq` must grow with every change, across restarts too, or peers keep an
        // older one: start from the Unix time in seconds, as geth does, which is above any
        // earlier run's (discv5 adds one per change while running).
        builder.seq(crate::network::unix_now_secs().max(1));
        let enr = builder.build(&key).map_err(DiscoveryError::Enr)?;

        let mut config = ConfigBuilder::new(ListenConfig::from(listen));
        if advertised.is_some() {
            config.disable_enr_update();
        }
        let config = config.build();
        let discv5 = Discv5::new(enr, key, config).map_err(DiscoveryError::Create)?;
        Ok(Self {
            discv5,
            chain_id,
            reported: HashSet::new(),
        })
    }

    /// Binds the UDP socket and starts the discv5 service. Awaited before [`Self::run`] is
    /// spawned, so a failure (usually: the port is taken) stops the node instead of leaving it
    /// running without discovery.
    pub(crate) async fn start(&mut self) -> Result<(), DiscoveryError> {
        self.discv5.start().await.map_err(DiscoveryError::Start)
    }

    /// Runs discovery until `cancel` fires, sending dialable peers of our chain to `peers`.
    /// Call [`Self::start`] first.
    ///
    /// `subscribed_peers` is the network's count of peers on our block topics; it decides how
    /// aggressively to search.
    pub(crate) async fn run(
        mut self,
        bootnodes: Vec<Bootnode>,
        peers: mpsc::Sender<Multiaddr>,
        subscribed_peers: watch::Receiver<usize>,
        cancel: CancellationToken,
    ) {
        let total = bootnodes.len();
        let mut added = 0;
        let resolved = join_all(bootnodes.into_iter().map(|bootnode| self.resolve(bootnode))).await;
        for enr in resolved.into_iter().flatten() {
            let peer = enr.node_id();
            debug!(%peer, chain_id = ?opstack_chain_id(&enr), "resolved bootnode");
            match self.discv5.add_enr(enr) {
                Ok(()) => added += 1,
                Err(err) => warn!(%peer, err, "failed to add bootnode"),
            }
        }
        info!(added, bootnodes = total, "discovery started");

        let mut ticks = interval(LOOKUP_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_lookup: Option<Instant> = None;
        let mut aggressive_interval = LOOKUP_INTERVAL;
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticks.tick() => {
                    let short_of_peers = *subscribed_peers.borrow() < TARGET_PEERS;
                    let interval = if short_of_peers { aggressive_interval } else { RELAXED_LOOKUP_INTERVAL };
                    if last_lookup.is_some_and(|at| at.elapsed() < interval) {
                        continue;
                    }
                    let concurrency = if short_of_peers { CONCURRENT_LOOKUPS } else { 1 };
                    let new_peers = self.lookup(&peers, concurrency).await;
                    last_lookup = Some(Instant::now());
                    aggressive_interval = if new_peers == 0 {
                        (aggressive_interval * 2).min(RELAXED_LOOKUP_INTERVAL)
                    } else {
                        LOOKUP_INTERVAL
                    };
                }
            }
        }
        self.discv5.shutdown();
    }

    /// Returns a bootnode's ENR, requesting it over discv5 for `enode://` bootnodes. A failed
    /// request is logged and yields `None`.
    async fn resolve(&self, bootnode: Bootnode) -> Option<Enr> {
        let addr = match bootnode {
            Bootnode::Enr(enr) => return Some(enr),
            Bootnode::Enode(addr) => addr,
        };
        match timeout(BOOTNODE_TIMEOUT, self.discv5.request_enr(addr.clone())).await {
            Ok(Ok(enr)) => Some(enr),
            Ok(Err(err)) => {
                warn!(%addr, %err, "failed to resolve bootnode");
                None
            }
            Err(_elapsed) => {
                warn!(%addr, "bootnode ENR request timed out");
                None
            }
        }
    }

    /// Runs `concurrency` random lookups at once and reports dialable peers of our chain.
    /// Returns how many of them were not reported before.
    async fn lookup(&mut self, peers: &mpsc::Sender<Multiaddr>, concurrency: usize) -> usize {
        let lookups = (0..concurrency)
            .map(|_| timeout(LOOKUP_TIMEOUT, self.discv5.find_node(NodeId::random())));
        let mut found = Vec::new();
        for result in join_all(lookups).await {
            match result {
                Ok(Ok(enrs)) => found.extend(enrs),
                Ok(Err(err)) => debug!(%err, "discovery lookup failed"),
                Err(_elapsed) => debug!("discovery lookup timed out"),
            }
        }
        // Random lookups return few peers of our chain; the routing table accumulates them, so
        // report it too. The network skips peers it is already connected to or dialing.
        let found_ours = found
            .iter()
            .filter(|enr| opstack_chain_id(enr) == Some(self.chain_id))
            .count();
        let prefer_ipv6 = !self.discv5.ip_mode().is_ipv4();
        let candidates: HashSet<Multiaddr> = found
            .iter()
            .chain(&self.discv5.table_entries_enr())
            .filter(|enr| opstack_chain_id(enr) == Some(self.chain_id))
            .filter_map(|enr| dial_addr(enr, prefer_ipv6))
            .collect();
        debug!(
            found = found.len(),
            found_ours,
            candidates = candidates.len(),
            table = self.discv5.connected_peers(),
            "discovery lookup finished"
        );
        if self.reported.len() >= MAX_REPORTED_PEERS {
            self.reported.clear();
        }
        let candidate_count = candidates.len();
        let mut new_peers = 0;
        for addr in candidates {
            if peers.try_send(addr.clone()).is_err() {
                break; // Network is busy or shutting down; the next lookup reports again.
            }
            new_peers += usize::from(self.reported.insert(addr));
        }
        metrics::discovery_round(concurrency, candidate_count, new_peers);
        new_peers
    }
}

/// Encodes our `opstack` ENR value: `uvarint(chain_id) ++ uvarint(version)`.
fn opstack_entry(chain_id: ChainId) -> Bytes {
    let mut chain_buf = uvarint::u64_buffer();
    let mut version_buf = uvarint::u64_buffer();
    Bytes::from(
        [
            uvarint::u64(chain_id, &mut chain_buf),
            uvarint::u64(OPSTACK_ENR_VERSION, &mut version_buf),
        ]
        .concat(),
    )
}

/// Returns the chain id from a peer's `opstack` ENR entry, if present and well formed.
fn opstack_chain_id(enr: &Enr) -> Option<ChainId> {
    let value: Bytes = enr.get_decodable(OPSTACK_ENR_KEY)?.ok()?;
    let (chain_id, _version) = unsigned_varint::decode::u64(&value).ok()?;
    Some(chain_id)
}

/// Returns `/ip4|ip6/<ip>/tcp/<port>/p2p/<peer id>` for an ENR with a secp256k1 key and a TCP
/// socket. One address per peer: the family we listen on when the ENR has it (that family is
/// known to work on this host), otherwise the other one.
fn dial_addr(enr: &Enr, prefer_ipv6: bool) -> Option<Multiaddr> {
    let v4 = enr.tcp4_socket().map(SocketAddr::V4);
    let v6 = enr.tcp6_socket().map(SocketAddr::V6);
    let socket = if prefer_ipv6 { v6.or(v4) } else { v4.or(v6) }?;
    let CombinedPublicKey::Secp256k1(key) = enr.public_key() else {
        return None;
    };
    let public = secp256k1::PublicKey::try_from_bytes(&key.encode()).ok()?;
    let addr = Multiaddr::from(socket.ip()).with(Protocol::Tcp(socket.port()));
    addr.with_p2p(PeerId::from_public_key(&PublicKey::from(public)))
        .ok()
}
