//! How many peers the node's networks have now, and what it served them in the last minute,
//! for the binary's tasks (the server's heartbeat to its balancer).
//!
//! Read from what each network already publishes as it runs (the consensus swarm's count of
//! gossip peers and its last status line's tally, the execution networks' open sessions and
//! their last serving line, the beacon swarm's count of peers): nothing is polled, and a read
//! is a few loads.

use std::net::IpAddr;

use op_indexer_el::{BlockProvider, ExecutionNetwork, ExecutionServed, Peers, SessionCounts};
use op_indexer_l1::{L1Network, LightClient};
use op_indexer_p2p::{GossipServed, Network};
use tokio::sync::watch;

/// The peers of each network, as of one read ([`NodeView::peers`](crate::NodeView::peers)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerCounts {
    /// Consensus peers connected and subscribed to the block topics: what gossip arrives from.
    pub consensus: usize,
    /// Sessions with the chain's execution peers; `None` without the execution network.
    pub execution: Option<SessionCounts>,
    /// Sessions with L1 execution peers; `None` without the L1 side.
    pub l1_execution: Option<usize>,
    /// Beacon peers of the light client; `None` without the L1 side.
    pub beacon: Option<usize>,
}

/// What the node served each network in the last minute, as of one read
/// ([`NodeView::served`](crate::NodeView::served)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeServed {
    /// The consensus network: blocks relayed and served by number.
    pub consensus: GossipServed,
    /// The chain's execution network; `None` without it.
    pub execution: Option<ExecutionServed>,
}

/// Where the counts are read from: one handle per network that runs.
#[derive(Debug, Clone, Default)]
pub(crate) struct PeerSources {
    consensus: Option<watch::Receiver<usize>>,
    execution: Option<Peers>,
    l1: Option<Peers>,
    beacon: Option<watch::Receiver<usize>>,
    consensus_served: Option<watch::Receiver<GossipServed>>,
    execution_served: Option<watch::Receiver<ExecutionServed>>,
    public_ip: Option<watch::Receiver<Option<IpAddr>>>,
}

impl PeerSources {
    /// The sources of the networks given, before they start.
    pub(crate) fn new<P: BlockProvider>(
        consensus: &Network,
        execution: Option<&ExecutionNetwork<P>>,
        l1: Option<&(L1Network, LightClient)>,
    ) -> Self {
        Self {
            consensus: Some(consensus.peer_count()),
            execution: execution.map(ExecutionNetwork::peers),
            l1: l1.map(|(network, _)| network.peers()),
            beacon: l1.map(|(_, light_client)| light_client.peer_count()),
            consensus_served: Some(consensus.served()),
            execution_served: execution.map(ExecutionNetwork::serving),
            public_ip: execution.map(ExecutionNetwork::public_ip),
        }
    }

    /// The public IP the execution network learns; `None` without it.
    pub(crate) fn public_ip(&self) -> Option<watch::Receiver<Option<IpAddr>>> {
        self.public_ip.clone()
    }

    /// The counts now.
    pub(crate) fn counts(&self) -> PeerCounts {
        PeerCounts {
            consensus: self.consensus.as_ref().map_or(0, |count| *count.borrow()),
            execution: self.execution.as_ref().map(Peers::session_counts),
            l1_execution: self.l1.as_ref().map(|l1| l1.session_counts().total),
            beacon: self.beacon.as_ref().map(|beacon| *beacon.borrow()),
        }
    }

    /// What was served in the last minute.
    pub(crate) fn served(&self) -> NodeServed {
        NodeServed {
            consensus: self
                .consensus_served
                .as_ref()
                .map(|served| *served.borrow())
                .unwrap_or_default(),
            execution: self
                .execution_served
                .as_ref()
                .map(|served| *served.borrow()),
        }
    }
}
