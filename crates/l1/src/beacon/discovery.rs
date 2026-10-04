//! Discovery of beacon nodes over discv5.
//!
//! Beacon nodes of every network, and execution nodes too, share one discv5 network. A beacon
//! node's record carries an `eth2` entry whose first four bytes are its fork digest
//! ([ENR structure]); nodes with ours are passed on as candidates. When most beacon nodes
//! carry another digest, this build's fork schedule is behind, and that is logged.
//!
//! Does not dial anyone.
//!
//! [ENR structure]: https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#eth2-field

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use discv5::{ConfigBuilder, Discv5, Enr, ListenConfig};
use enr::{CombinedKey, CombinedPublicKey, NodeId};
use libp2p::identity::{PublicKey, secp256k1};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::BeaconError;
use super::spec::ForkDigest;

/// The record entry of a beacon node.
const ETH2_KEY: &str = "eth2";
/// Time between lookup rounds: ordinary traffic of the DHT.
const LOOKUP_INTERVAL: Duration = Duration::from_secs(6);
/// Random lookups per round.
const LOOKUPS_PER_ROUND: usize = 3;
/// Limit for one lookup.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);
/// Limit for asking an `enode://` bootnode for its record.
const BOOTNODE_TIMEOUT: Duration = Duration::from_secs(10);
/// Records remembered as seen; the set is emptied when it is full.
const MAX_SEEN: usize = 50_000;
/// Beacon records of other digests seen before the build is said to be behind.
const BEHIND_AFTER: u64 = 50;

/// A beacon node of our network, to dial.
#[derive(Debug, Clone)]
pub(super) struct Candidate {
    pub(super) peer: PeerId,
    pub(super) addr: Multiaddr,
}

/// A discv5 node that looks for beacon nodes.
pub(super) struct Discovery {
    discv5: Discv5,
    digest: ForkDigest,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl Discovery {
    /// Binds the discovery socket at `listen` and seeds the routing table with `bootnodes`
    /// (`enr:` records, or `enode://` URLs whose record is asked for).
    ///
    /// # Errors
    ///
    /// Returns [`BeaconError::Discovery`] if the discv5 node cannot be created or started.
    pub(super) async fn start(
        listen: SocketAddr,
        bootnodes: &[String],
        digest: ForkDigest,
    ) -> Result<Self, BeaconError> {
        let key = CombinedKey::generate_secp256k1();
        let mut record = Enr::builder();
        record.udp4(listen.port());
        let record = record
            .build(&key)
            .map_err(|err| BeaconError::Discovery(err.to_string()))?;
        let config = ConfigBuilder::new(ListenConfig::from_ip(listen.ip(), listen.port())).build();
        let mut discv5 = Discv5::new(record, key, config)
            .map_err(|err| BeaconError::Discovery(err.to_owned()))?;
        discv5
            .start()
            .await
            .map_err(|err| BeaconError::Discovery(err.to_string()))?;
        for bootnode in bootnodes {
            let record = if bootnode.starts_with("enr:") {
                bootnode.parse::<Enr>().ok()
            } else if let Some(addr) = enode_discovery_addr(bootnode) {
                let asked = timeout(BOOTNODE_TIMEOUT, discv5.request_enr(addr)).await;
                asked.ok().and_then(Result::ok)
            } else {
                None
            };
            if let Some(record) = record {
                // Refused for a record of our own key or a duplicate: nothing lost.
                let _added = discv5.add_enr(record);
            } else {
                debug!(bootnode, "beacon bootnode not usable");
            }
        }
        Ok(Self { discv5, digest })
    }

    /// Looks for beacon nodes of our digest until `cancel` fires, sending each once on
    /// `found`. A candidate is dropped when `found` is full: more turn up.
    pub(super) async fn run(self, found: mpsc::Sender<Candidate>, cancel: CancellationToken) {
        let mut seen: HashSet<NodeId> = HashSet::new();
        let mut digests: HashMap<ForkDigest, u64> = HashMap::new();
        let mut warned = false;
        let mut round = interval(LOOKUP_INTERVAL);
        round.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = round.tick() => {}
            }
            let mut records = Vec::new();
            for _ in 0..LOOKUPS_PER_ROUND {
                let lookup = self.discv5.find_node(NodeId::random());
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    result = timeout(LOOKUP_TIMEOUT, lookup) => {
                        if let Ok(Ok(found)) = result {
                            records.extend(found);
                        }
                    }
                }
            }
            records.extend(self.discv5.table_entries_enr());
            if seen.len() > MAX_SEEN {
                seen.clear();
            }
            for record in records {
                if !seen.insert(record.node_id()) {
                    continue;
                }
                let Some(digest) = record_digest(&record) else {
                    continue;
                };
                *digests.entry(digest).or_default() += 1;
                if digest != self.digest {
                    continue;
                }
                if let Some(candidate) = candidate(&record) {
                    // Full: the client has candidates enough for now.
                    let _queued = found.try_send(candidate);
                }
            }
            let ours = digests.get(&self.digest).copied().unwrap_or_default();
            let most = digests.iter().max_by_key(|(_, count)| **count);
            if let Some((digest, count)) = most
                && !warned
                && *count >= BEHIND_AFTER
                && *count > ours.saturating_mul(4)
            {
                warned = true;
                warn!(
                    most_common = %alloy_primitives::hex::encode(digest),
                    ours = %alloy_primitives::hex::encode(self.digest),
                    "most beacon nodes are on another fork digest: this build's beacon fork \
                     schedule is behind, and the light client will find few peers"
                );
            }
        }
    }
}

/// The fork digest in a record's `eth2` entry: the first four bytes of an SSZ `ENRForkID`.
fn record_digest(record: &Enr) -> Option<ForkDigest> {
    // The entry is an RLP byte string.
    let mut raw = record.get_raw_rlp(ETH2_KEY)?;
    let entry = alloy_rlp::Header::decode_bytes(&mut raw, false).ok()?;
    entry.first_chunk::<4>().copied()
}

/// The libp2p identity and TCP address of a record's node.
fn candidate(record: &Enr) -> Option<Candidate> {
    let CombinedPublicKey::Secp256k1(key) = record.public_key() else {
        return None;
    };
    let key = secp256k1::PublicKey::try_from_bytes(&key.to_sec1_bytes()).ok()?;
    let addr = record.tcp4_socket()?;
    Some(Candidate {
        peer: PeerId::from_public_key(&PublicKey::from(key)),
        addr: Multiaddr::from(*addr.ip()).with(Protocol::Tcp(addr.port())),
    })
}

/// Turns an `enode://<key>@<ip>:<port>[?discport=<udp port>]` URL into the address discv5
/// asks for the node's record at.
fn enode_discovery_addr(enode: &str) -> Option<Multiaddr> {
    let (key, endpoint) = enode.strip_prefix("enode://")?.split_once('@')?;
    let (endpoint, query) = endpoint.split_once('?').unwrap_or((endpoint, ""));
    let (ip, port) = endpoint.rsplit_once(':')?;
    let udp = query.strip_prefix("discport=").unwrap_or(port);
    let mut sec1 = vec![4];
    sec1.extend(alloy_primitives::hex::decode(key).ok()?);
    let key = secp256k1::PublicKey::try_from_bytes(&sec1).ok()?;
    let peer = PeerId::from_public_key(&PublicKey::from(key));
    format!("/ip4/{ip}/udp/{udp}/p2p/{peer}").parse().ok()
}
