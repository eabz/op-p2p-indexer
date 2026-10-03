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
    /// Maximum established connections, inbound and outbound combined. Inbound connections may
    /// take at most half, so outbound dials always have room.
    pub max_peers: u32,
}
