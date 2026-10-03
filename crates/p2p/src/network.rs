//! The network component: drives the libp2p swarm (gossipsub plus connection limits) and runs
//! discovery.
//!
//! [`Network::run`] owns the swarm. Child tasks in its `JoinSet`s do work that may block or wait:
//! discovery, a bounded number of blocking validations, and known-peer saves to the node store.
//! Gossipsub holds each message until we report the validation result, and only forwards accepted
//! ones.

mod state;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::BlockNumber;
use libp2p::connection_limits::{self, ConnectionLimits};
use libp2p::futures::StreamExt;
use libp2p::identity::{Keypair, secp256k1};
use libp2p::multiaddr::Protocol;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, noise, tcp, yamux};
use op_indexer_primitives::UnsafeBlock;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use self::state::State;
use crate::block::BlockValidator;
use crate::discovery::{Discovery, DiscoveryError};
use crate::gossip::{self, GossipError};
use crate::metrics;
use crate::{NetworkConfig, NodeStore};

/// Peers found by discovery and waiting to be dialed. Each lookup round reports the routing
/// table's peers of our chain; extras are dropped and reported again by the next round.
const DISCOVERED_PEERS_CAPACITY: usize = 256;
/// Close connections with no open streams after this long.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
/// Connections being established at once, per direction. Bounds dial bursts from discovery.
const MAX_PENDING_CONNECTIONS: u32 = 16;
/// How often connected peers are checked for eviction.
const EVICTION_INTERVAL: Duration = Duration::from_secs(10);

/// Errors that stop the network.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetworkError {
    /// The TCP/Noise/yamux transport could not be built.
    #[error("failed to build the libp2p transport")]
    Transport(#[source] noise::Error),
    /// Gossipsub could not be set up.
    #[error(transparent)]
    Gossip(#[from] GossipError),
    /// Discovery could not be created or started.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The discovery task ended while the node was running.
    #[error("discovery stopped unexpectedly")]
    DiscoveryStopped,
    /// The discovery task panicked.
    #[error("discovery task failed")]
    DiscoveryFailed(#[source] tokio::task::JoinError),
    /// The listen address could not be bound.
    #[error("failed to listen on {0}")]
    Listen(Multiaddr, #[source] libp2p::TransportError<std::io::Error>),
}

/// The p2p node: discovers OP Stack peers, joins block gossip, and emits validated unsafe blocks.
#[derive(Debug)]
pub struct Network {
    config: NetworkConfig,
    keypair: secp256k1::Keypair,
    store: Arc<NodeStore>,
    blocks: mpsc::Sender<UnsafeBlock>,
    safe_head: watch::Receiver<BlockNumber>,
}

/// The swarm's protocols: connection limits enforced for every connection, then gossipsub.
#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct Behaviour {
    limits: connection_limits::Behaviour,
    gossipsub: gossip::Behaviour,
}

impl Network {
    /// Creates a node with identity `keypair` that sends validated blocks to `blocks` and
    /// remembers peers that deliver them in `store`.
    ///
    /// `safe_head` is the highest L2 block known to be committed to L1; 0 until an L1 source is
    /// wired. Missed blocks at or below it are not reported as a gap.
    pub const fn new(
        config: NetworkConfig,
        keypair: secp256k1::Keypair,
        store: Arc<NodeStore>,
        blocks: mpsc::Sender<UnsafeBlock>,
        safe_head: watch::Receiver<BlockNumber>,
    ) -> Self {
        Self {
            config,
            keypair,
            store,
            blocks,
            safe_head,
        }
    }

    /// Runs the node until `cancel` fires or the block consumer drops its receiver.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkError`] if the transport, gossipsub, or discovery cannot be set up, the
    /// listen address cannot be bound, or discovery stops while the node runs.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), NetworkError> {
        let Self {
            config,
            keypair,
            store,
            blocks,
            safe_head,
        } = self;
        let chain = config.chain;
        metrics::describe();

        let (gossipsub, topics) = gossip::behaviour(chain.chain_id)?;
        let limits = connection_limits::Behaviour::new(
            ConnectionLimits::default()
                .with_max_established(Some(config.max_peers))
                // Inbound may take at most half, so discovery dials always have room.
                .with_max_established_incoming(Some(config.max_peers / 2))
                .with_max_established_per_peer(Some(1))
                .with_max_pending_incoming(Some(MAX_PENDING_CONNECTIONS))
                .with_max_pending_outgoing(Some(MAX_PENDING_CONNECTIONS)),
        );
        let mut discovery = Discovery::new(&keypair, config.listen_addr, chain.chain_id)?;
        discovery.start().await?;

        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(Keypair::from(keypair))
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(NetworkError::Transport)?
            .with_behaviour(|_| Behaviour { limits, gossipsub })
            .unwrap_or_else(|never| match never {})
            .with_swarm_config(|swarm| swarm.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
            .build();
        let listen =
            Multiaddr::from(config.listen_addr.ip()).with(Protocol::Tcp(config.listen_addr.port()));
        swarm
            .listen_on(listen.clone())
            .map_err(|err| NetworkError::Listen(listen, err))?;
        info!(peer_id = %swarm.local_peer_id(), chain_id = chain.chain_id, "p2p node starting");

        let (discovered_tx, mut discovered_rx) = mpsc::channel(DISCOVERED_PEERS_CAPACITY);
        let (peer_count_tx, peer_count_rx) = watch::channel(0);
        let mut discovery_task = JoinSet::new();
        discovery_task.spawn(discovery.run(
            config.bootnodes,
            discovered_tx,
            peer_count_rx,
            cancel.child_token(),
        ));

        let validator = BlockValidator::new(chain.chain_id, chain.unsafe_block_signer);
        let mut state = State::new(topics, validator, blocks, store, peer_count_tx, safe_head);

        state.dial_known_peers(&mut swarm).await;

        let mut evictions = interval(EVICTION_INTERVAL);
        evictions.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let result = loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break Ok(()),
                // Before swarm events, so a busy swarm cannot delay accept/reject reports.
                Some(result) = state.validations.join_next() => match result {
                    Ok(validated) => state.on_validated(&mut swarm, validated),
                    Err(err) => warn!(%err, "validation task failed"),
                },
                event = swarm.select_next_some() => state.on_swarm_event(&mut swarm, event),
                Some(addr) = discovered_rx.recv() => state.dial(&mut swarm, addr),
                Some(result) = state.persists.join_next() => match result {
                    Ok(Ok(())) => metrics::known_peer_saved(),
                    Ok(Err(err)) => warn!(%err, "failed to save known peer"),
                    Err(err) => warn!(%err, "known peer save task failed"),
                },
                // Discovery only ends on cancellation, which is handled above; anything else is fatal.
                Some(result) = discovery_task.join_next() => break Err(match result {
                    Ok(()) => NetworkError::DiscoveryStopped,
                    Err(err) => NetworkError::DiscoveryFailed(err),
                }),
                _ = evictions.tick() => {
                    state.evict_idle_peers(&mut swarm);
                    state.dial_queued_known_peers(&mut swarm);
                }
            }
            // A closed block channel means the consumer is gone: shut down.
            if state.consumer_closed() {
                info!("block consumer closed");
                break Ok(());
            }
        };

        info!("p2p node stopping");
        discovery_task.shutdown().await;
        state.validations.shutdown().await;
        // Let pending peer saves finish; they are short writes.
        while state.persists.join_next().await.is_some() {}
        result
    }
}
