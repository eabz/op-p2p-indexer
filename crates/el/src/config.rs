//! Configuration of the execution network, as plain data.
//!
//! Does not read the environment; the binary fills [`ElConfig`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::ExecutionPeer;

/// Configuration of the execution network.
#[derive(Debug, Clone)]
pub struct ElConfig {
    /// The chain: genesis hash, fork activations and bootnodes come from it.
    pub chain: &'static ChainSpec,
    /// Listen address, used for both discovery (UDP) and sessions (TCP).
    pub listen_addr: SocketAddr,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs. Empty means the chain's.
    pub bootnodes: Vec<String>,
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
}
