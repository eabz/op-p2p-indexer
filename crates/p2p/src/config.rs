//! Network configuration.

use std::net::SocketAddr;

use op_indexer_chainspec::ChainSpec;

use crate::Bootnode;

/// Configuration for [`crate::Network`].
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Chain to follow: selects the gossip topics, filters discovered peers, and provides the
    /// sequencer key that signs unsafe blocks.
    pub chain: &'static ChainSpec,
    /// Listen address, used for libp2p (TCP) and discv5 (UDP), e.g. `0.0.0.0:9222`.
    pub listen_addr: SocketAddr,
    /// Discovery bootnodes.
    pub bootnodes: Vec<Bootnode>,
    /// The public address (IP and port, the same for TCP and UDP) the node record advertises,
    /// for a node behind NAT or in a container whose public address is known. `None` lets
    /// discovery learn it from peers ([NAT]).
    ///
    /// [NAT]: https://specs.optimism.io/protocol/rollup-node-p2p.html#nat
    pub advertised_addr: Option<SocketAddr>,
    /// Maximum established connections, inbound and outbound combined. Inbound connections may
    /// take at most half, so outbound dials always have room.
    pub max_peers: u32,
}
