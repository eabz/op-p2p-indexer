//! Discovery of execution peers over discv5.
//!
//! OP Stack execution nodes sit in the same discv5 network as Ethereum's and other chains'
//! nodes. A node record says which chain and fork its node is on through an [EIP-2124] fork id,
//! under the `eth` key (op-geth) or the `opel` key (op-reth). We advertise both entries,
//! walk the network with random lookups, and pass on the nodes whose fork hash equals ours.
//!
//! Peers of our chain and fork are a tiny share of that network (about 25 nodes), so finding
//! them fast matters. Discovery starts in a fast phase that keeps [`FAST_LOOKUPS`] random
//! lookups in flight; once [`TARGET_KNOWN_PEERS`] are known or [`FAST_PHASE`] has passed it
//! falls back to a few lookups every [`LOOKUP_INTERVAL`], which keeps replacements coming.
//! Lookups are ordinary DHT traffic; a new lookup starts at most every [`LOOKUP_SPACING`].
//!
//! Does not dial anyone: candidates go to the peer set.
//!
//! [EIP-2124]: https://eips.ethereum.org/EIPS/eip-2124

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use alloy_eip2124::ForkId;
use alloy_rlp::Decodable;
use discv5::{ConfigBuilder, Discv5, Enr, ListenConfig, QueryError};
use enr::{CombinedKey, CombinedPublicKey, EnrPublicKey, NodeId};
use futures_util::future::join_all;
use futures_util::stream::{FuturesUnordered, StreamExt};
use reth_network_peers::{NodeRecord, PeerId};
use tokio::sync::mpsc;
use tokio::time::error::Elapsed;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::session::SessionContext;
use crate::{ElError, metrics};

/// Time between lookup rounds after the fast phase. The viability test found about one new
/// peer every two minutes at this pace.
const LOOKUP_INTERVAL: Duration = Duration::from_secs(10);
/// Random lookups per round after the fast phase, each toward a different region of the
/// network.
const CONCURRENT_LOOKUPS: usize = 5;
/// Random lookups kept in flight during the fast phase.
const FAST_LOOKUPS: usize = 16;
/// The fast phase ends once this many peers of our chain and fork are known. More than exist
/// today, so in practice [`FAST_PHASE`] ends it.
const TARGET_KNOWN_PEERS: usize = 32;
/// Longest the fast phase lasts. The whole network has been walked several times by then;
/// peers that appear later are found by the slow rounds.
const FAST_PHASE: Duration = Duration::from_mins(10);
/// How often the routing table is searched for peers of our chain. It changes slowly, so this
/// is not done after every lookup.
const TABLE_SCAN_INTERVAL: Duration = Duration::from_secs(10);
/// Shortest time between two lookup starts in the fast phase, so lookups that fail at once
/// (no network, an empty routing table) do not become a busy loop.
const LOOKUP_SPACING: Duration = Duration::from_millis(250);
/// Limit for one lookup.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(20);
/// Limit for requesting an `enode://` bootnode's node record.
const BOOTNODE_TIMEOUT: Duration = Duration::from_secs(10);
/// Known peers are sent to the peer set again this often, so it can redial them.
const REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// Peers of our chain remembered for re-reporting. When full, the set starts over.
const MAX_KNOWN_PEERS: usize = 4096;
/// How often discovery says that it has found no peer of our fork while records of other forks
/// keep arriving: the sign of a build that does not know a fork the chain has activated.
const NO_PEERS_WARN_INTERVAL: Duration = Duration::from_mins(10);

/// An execution peer of our chain and fork that can be dialed.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    /// The peer's public key, which the encrypted handshake checks.
    pub(crate) peer_id: PeerId,
    /// The peer's TCP address.
    pub(crate) addr: SocketAddr,
}

/// A discv5 node that finds execution peers of our chain and fork.
pub(crate) struct Discovery {
    discv5: Discv5,
    ctx: Arc<SessionContext>,
    /// Peers of our chain found so far, at most [`MAX_KNOWN_PEERS`].
    known: HashMap<NodeId, Candidate>,
    /// What the node record advertised when it was last logged.
    advertised: Option<Advertised>,
    /// The fork id in the node record.
    fork_id: ForkId,
    /// Node records with an `opel` entry of another fork seen since the last warning: OP Stack
    /// nodes of another chain, or of ours past a fork this build does not know.
    other_forks: u64,
}

/// The address a node record advertises: what other nodes dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Advertised {
    ip: Option<IpAddr>,
    udp: Option<u16>,
    tcp: Option<u16>,
}

impl Discovery {
    /// Creates a discovery node on `listen` (UDP) with the session key, advertising the same
    /// port for sessions (TCP) and our fork id. With `advertised`, the node record carries that
    /// address and port and discv5 leaves it alone; without, the IP is learned from peers.
    ///
    /// # Errors
    ///
    /// Returns [`ElError`] if the key, the node record or the discv5 service is invalid.
    pub(crate) fn new(
        ctx: Arc<SessionContext>,
        listen: SocketAddr,
        advertised: Option<SocketAddr>,
    ) -> Result<Self, ElError> {
        let mut secret = ctx.key().secret_bytes();
        let key =
            CombinedKey::secp256k1_from_bytes(&mut secret).map_err(|_err| ElError::InvalidKey)?;

        let fork_id = ctx.fork_filter().current();
        let fork_entry = vec![fork_id];
        let mut builder = Enr::builder();
        match advertised {
            // A configured address goes into the record as it is.
            Some(SocketAddr::V4(addr)) => {
                builder.ip4(*addr.ip()).udp4(addr.port()).tcp4(addr.port());
            }
            Some(SocketAddr::V6(addr)) => {
                builder.ip6(*addr.ip()).udp6(addr.port()).tcp6(addr.port());
            }
            // Otherwise only the ports: discv5 adds the IP once enough nodes report it.
            None if listen.is_ipv6() => {
                builder.udp6(listen.port()).tcp6(listen.port());
            }
            None => {
                builder.udp4(listen.port()).tcp4(listen.port());
            }
        }
        for key in ctx.spec().record_keys {
            builder.add_value(*key, &fork_entry);
        }
        let enr = builder.build(&key).map_err(ElError::Enr)?;

        let mut config = ConfigBuilder::new(ListenConfig::from(listen));
        if advertised.is_some() {
            // discv5 must neither replace a configured address with what peers report nor
            // withdraw it when nobody dials in.
            config.disable_enr_update();
        }
        let config = config.build();
        let discv5 = Discv5::new(enr, key, config).map_err(ElError::DiscoveryCreate)?;
        Ok(Self {
            discv5,
            ctx,
            known: HashMap::new(),
            advertised: None,
            fork_id,
            other_forks: 0,
        })
    }

    /// Runs discovery until `cancel` fires, sending dialable peers of our chain and fork to
    /// `candidates`: each when first seen, and all known ones again every [`REPORT_INTERVAL`].
    ///
    /// # Errors
    ///
    /// Returns [`ElError::DiscoveryStart`] if the UDP socket cannot be bound.
    pub(crate) async fn run(
        mut self,
        candidates: mpsc::Sender<Candidate>,
        cancel: CancellationToken,
    ) -> Result<(), ElError> {
        self.discv5.start().await.map_err(ElError::DiscoveryStart)?;
        let added = self.add_bootnodes().await;
        info!(
            added,
            bootnodes = self.ctx.spec().bootnodes.len(),
            "execution discovery started"
        );
        self.log_advertised();

        let mut rounds = interval(LOOKUP_INTERVAL);
        rounds.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut spacing = interval(LOOKUP_SPACING);
        spacing.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut reports = interval(REPORT_INTERVAL);
        reports.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut table_scans = interval(TABLE_SCAN_INTERVAL);
        table_scans.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut no_peers = interval(NO_PEERS_WARN_INTERVAL);
        no_peers.set_missed_tick_behavior(MissedTickBehavior::Delay);
        no_peers.reset();
        // Lookups in flight. Dropping them on shutdown is fine: discv5 owns the queries.
        let mut lookups = FuturesUnordered::new();
        let started = Instant::now();
        let mut fast = true;
        loop {
            if fast && (self.known.len() >= TARGET_KNOWN_PEERS || started.elapsed() >= FAST_PHASE) {
                fast = false;
                info!(
                    known = self.known.len(),
                    after = ?started.elapsed(),
                    "execution discovery leaves its fast phase"
                );
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = reports.tick() => {
                    for candidate in self.known.values() {
                        // A full channel means the peer set is busy; the next report repeats.
                        if candidates.try_send(candidate.clone()).is_err() {
                            break;
                        }
                    }
                }
                Some(result) = lookups.next() => self.found(result, &candidates),
                // The routing table keeps peers that random lookups themselves rarely return.
                _ = no_peers.tick() => self.warn_no_peers(),
                _ = table_scans.tick() => {
                    self.refresh_fork_id();
                    self.log_advertised();
                    self.consider(&self.discv5.table_entries_enr(), &candidates);
                }
                // Fast phase: top the lookups in flight up, one per tick.
                _ = spacing.tick(), if fast && lookups.len() < FAST_LOOKUPS => {
                    lookups.push(self.start_lookup());
                }
                // Afterwards: a round of lookups every interval.
                _ = rounds.tick(), if !fast => {
                    for _ in 0..CONCURRENT_LOOKUPS.saturating_sub(lookups.len()) {
                        lookups.push(self.start_lookup());
                    }
                }
            }
        }
        self.discv5.shutdown();
        Ok(())
    }

    /// Seeds the routing table: `enr:` records directly, `enode://` bootnodes after asking
    /// them for their record. Returns how many were added.
    async fn add_bootnodes(&self) -> usize {
        let bootnodes = &self.ctx.spec().bootnodes;
        let resolved = join_all(bootnodes.iter().map(|bootnode| self.resolve(bootnode))).await;
        let mut added = 0;
        for enr in resolved.into_iter().flatten() {
            match self.discv5.add_enr(enr) {
                Ok(()) => added += 1,
                Err(err) => warn!(err, "failed to add execution bootnode"),
            }
        }
        added
    }

    async fn resolve(&self, bootnode: &str) -> Option<Enr> {
        if bootnode.starts_with("enr:") {
            return bootnode
                .parse()
                .inspect_err(|err: &String| warn!(err, "invalid execution bootnode record"))
                .ok();
        }
        let Some(addr) = enode_discovery_addr(bootnode) else {
            warn!(bootnode, "invalid execution bootnode");
            return None;
        };
        match timeout(BOOTNODE_TIMEOUT, self.discv5.request_enr(addr.clone())).await {
            Ok(Ok(enr)) => Some(enr),
            Ok(Err(err)) => {
                debug!(addr, %err, "failed to resolve execution bootnode");
                None
            }
            Err(_elapsed) => {
                debug!(addr, "execution bootnode record request timed out");
                None
            }
        }
    }

    /// Logs the address the node record advertises, when it differs from what was last
    /// logged. discv5 changes it by itself: it sets the IP and UDP port once enough nodes
    /// report the same address, follows a change of that address, and withdraws both for six
    /// hours if nobody dials in within five minutes (it then takes the node to be unreachable).
    fn log_advertised(&mut self) {
        let enr = self.discv5.local_enr();
        let current = Advertised {
            ip: enr
                .ip4()
                .map(IpAddr::V4)
                .or_else(|| enr.ip6().map(IpAddr::V6)),
            udp: enr.udp4().or_else(|| enr.udp6()),
            tcp: enr.tcp4().or_else(|| enr.tcp6()),
        };
        if self.advertised != Some(current) {
            self.advertised = Some(current);
            info!(
                ip = ?current.ip,
                udp = ?current.udp,
                tcp = ?current.tcp,
                "execution node record advertises"
            );
        }
    }

    /// Puts our current fork id into the node record if it changed: a time fork activated
    /// while the node runs. A record left at the old fork id would have peers past the fork
    /// take us for a node that missed it.
    fn refresh_fork_id(&mut self) {
        let current = self.ctx.fork_filter().current();
        if current == self.fork_id {
            return;
        }
        let entry = vec![current];
        for key in self.ctx.spec().record_keys.iter().copied() {
            if let Err(err) = self.discv5.enr_insert(key, &entry) {
                warn!(
                    key,
                    ?err,
                    "failed to update the fork id in the execution node record"
                );
                return;
            }
        }
        info!(fork_id = ?current, "a hardfork activated; execution node record updated");
        self.fork_id = current;
        // Peers of the fork before are not ours any more.
        self.known.clear();
    }

    /// Says that no peer of our fork is known while OP Stack records of other forks arrive.
    /// Peers past a fork this build does not know are skipped like any other chain's, so
    /// without this a node on a stale build finds nobody and logs nothing.
    fn warn_no_peers(&mut self) {
        let other_forks = std::mem::take(&mut self.other_forks);
        if self.known.is_empty() && other_forks > 0 {
            warn!(
                our_fork_hash = ?self.fork_id.hash,
                other_forks,
                "execution discovery knows no peer on this build's fork, only nodes on other \
                 forks; if the chain activated a hardfork this build does not know, \
                 update the fork activations"
            );
        }
    }

    /// Starts one lookup toward a random region of the network, bounded by [`LOOKUP_TIMEOUT`].
    fn start_lookup(
        &self,
    ) -> impl Future<Output = Result<Result<Vec<Enr>, QueryError>, Elapsed>> + use<> {
        timeout(LOOKUP_TIMEOUT, self.discv5.find_node(NodeId::random()))
    }

    /// Handles a finished lookup: reports the new peers of our chain and fork it returned.
    fn found(
        &mut self,
        result: Result<Result<Vec<Enr>, QueryError>, Elapsed>,
        candidates: &mpsc::Sender<Candidate>,
    ) {
        match result {
            Ok(Ok(enrs)) => self.consider(&enrs, candidates),
            Ok(Err(err)) => debug!(%err, "execution discovery lookup failed"),
            Err(_elapsed) => debug!("execution discovery lookup timed out"),
        }
    }

    /// Reports the peers of our chain and fork among `found` that were not known yet.
    fn consider(&mut self, found: &[Enr], candidates: &mpsc::Sender<Candidate>) {
        let ours = self.fork_id;
        let keys = self.ctx.spec().record_keys;
        let preferred = keys.first().copied();
        let mut new_peers = 0;
        for enr in found {
            let Some(fork_id) = fork_id(enr, keys) else {
                continue;
            };
            if fork_id.hash != ours.hash {
                if preferred.is_some_and(|key| enr.get_raw_rlp(key).is_some()) {
                    self.other_forks = self.other_forks.saturating_add(1);
                }
                continue;
            }
            // A node on our fork announcing a next fork we do not know: this build is behind.
            if fork_id.next != 0 && !self.ctx.spec().knows_fork_time(fork_id.next) {
                self.ctx.warn_build_behind(fork_id, "node record");
            }
            let Some(candidate) = candidate(enr) else {
                continue;
            };
            if self.known.len() >= MAX_KNOWN_PEERS {
                self.known.clear();
            }
            if self
                .known
                .insert(enr.node_id(), candidate.clone())
                .is_none()
            {
                new_peers += 1;
                metrics::candidate_discovered(self.ctx.spec().label);
                // A full channel means the peer set is busy; the next report sends it.
                let _sent = candidates.try_send(candidate);
            }
        }
        // Lookups finish several times a second in the fast phase: log only those that found
        // a peer.
        if new_peers > 0 {
            debug!(
                found = found.len(),
                new_peers,
                known = self.known.len(),
                table = self.discv5.connected_peers(),
                "execution discovery found new peers"
            );
        }
    }
}

/// Returns the fork id a node record carries under the first of `keys` it has. Each entry
/// holds a list with one fork id.
fn fork_id(enr: &Enr, keys: &[&str]) -> Option<ForkId> {
    keys.iter().find_map(|key| {
        let mut raw = enr.get_raw_rlp(*key)?;
        Vec::<ForkId>::decode(&mut raw).ok()?.into_iter().next()
    })
}

/// Builds the dialable candidate of a node record with a secp256k1 key, a public IPv4
/// address and a TCP port. A record is written by its node, so its address is untrusted: one
/// that points into this host or a private network would make us dial it.
fn candidate(enr: &Enr) -> Option<Candidate> {
    let CombinedPublicKey::Secp256k1(public) = enr.public_key() else {
        return None;
    };
    let addr = enr.tcp4_socket()?;
    if addr.port() == 0 || !is_public(*addr.ip()) {
        return None;
    }
    Some(Candidate {
        peer_id: PeerId::from_slice(public.encode_uncompressed().as_ref()),
        addr: SocketAddr::V4(addr),
    })
}

/// Whether `ip` is an address on the public internet: not loopback, private, link-local,
/// unspecified, broadcast, multicast or reserved for documentation.
const fn is_public(ip: Ipv4Addr) -> bool {
    !(ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation())
}

/// Turns an `enode://<key>@<ip>:<port>[?discport=<udp port>]` URL into the discv5 contact
/// address `/ip4/<ip>/udp/<port>/p2p/<peer id>`.
pub fn enode_discovery_addr(enode: &str) -> Option<String> {
    let record: NodeRecord = enode.parse().ok()?;
    // discv5 names a node by the libp2p peer id of its uncompressed public key.
    let key = [[0x04].as_slice(), record.id.as_slice()].concat();
    let public = libp2p_identity::secp256k1::PublicKey::try_from_bytes(&key).ok()?;
    let peer = libp2p_identity::PeerId::from_public_key(&libp2p_identity::PublicKey::from(public));
    let family = if record.address.is_ipv6() {
        "ip6"
    } else {
        "ip4"
    };
    Some(format!(
        "/{family}/{}/udp/{}/p2p/{peer}",
        record.address, record.udp_port
    ))
}
