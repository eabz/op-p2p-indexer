//! Configuration of the execution network, as plain data.
//!
//! Does not read the environment; the binary fills [`ElConfig`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::ExecutionPeer;

/// Base wait before the peer set dials a peer again after a failed dial or an ended session.
const REDIAL_INTERVAL: Duration = Duration::from_mins(5);

/// Configuration of the execution network.
#[derive(Debug, Clone)]
pub struct ElConfig {
    /// The chain: genesis hash, fork activations and bootnodes come from it.
    pub chain: &'static ChainSpec,
    /// Listen address, used for both discovery (UDP) and sessions (TCP).
    pub listen_addr: SocketAddr,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs. Empty means the chain's.
    pub bootnodes: Vec<String>,
    /// Sessions the node tries to keep by dialing peers.
    pub max_outbound_sessions: usize,
    /// Sessions accepted from peers that dial us.
    pub max_inbound_sessions: usize,
    /// Peers that served us in an earlier run, most recently served first. They are dialed
    /// first, without waiting for discovery to find them again.
    pub saved_peers: Vec<ExecutionPeer>,
    /// The address (IP and port, the same for TCP and UDP) the node record advertises, for a
    /// node reachable at a known public address. `None` lets discovery learn the address from
    /// what other nodes report.
    pub advertised_addr: Option<SocketAddr>,
}

impl ElConfig {
    /// Default listen address: every interface, the usual execution p2p port.
    pub const DEFAULT_LISTEN_ADDR: SocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 30303);

    /// The part of the configuration the peer set uses.
    pub(crate) const fn peer_set(&self) -> PeerSetConfig {
        PeerSetConfig {
            target_sessions: self.max_outbound_sessions,
            max_inbound: self.max_inbound_sessions,
            redial_interval: REDIAL_INTERVAL,
        }
    }
}

/// What the peer set needs to know.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerSetConfig {
    /// Outbound sessions to keep.
    pub(crate) target_sessions: usize,
    /// Most inbound sessions kept at once.
    pub(crate) max_inbound: usize,
    /// Base wait before dialing a peer again after a TCP failure, a timeout, a failed hello or
    /// status, or a session that ended for an ordinary reason. Full peers are retried sooner.
    pub(crate) redial_interval: Duration,
}
