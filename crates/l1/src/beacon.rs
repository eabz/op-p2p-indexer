//! A beacon light client: the L1 execution blocks Ethereum's consensus vouches for, learned
//! from beacon peers over libp2p and verified.
//!
//! ```text
//! checkpoint root (configuration) ─▶ bootstrap ─▶ sync committee
//!   ─▶ finality and optimistic updates, each signed by the committee
//!   ─▶ TrustedL1Block { number, hash, finalized }
//! ```
//!
//! - [`LightClient`] is the component the binary builds and runs; [`BeaconConfig`] its
//!   plain-data configuration and [`BeaconError`] what stops it.
//! - `spec` holds the beacon network's parameters; `types` the SSZ containers; `verify` the
//!   proofs and signatures that make a header trusted; `rpc` the request/response wire
//!   format; `discovery` finds beacon nodes; `network` is the swarm and its peers; `client` decides
//!   what to ask for.
//!
//! The one trusted input is the checkpoint: the root of a finalized beacon block, given in
//! configuration. Everything after it is verified. Nothing is persisted: a restart
//! bootstraps again, which takes well under a minute, so the checkpoint has to stay recent
//! enough for peers to hold its bootstrap. Uses no RPC. See `docs/l1.md`.

mod client;
mod discovery;
mod network;
mod rpc;
mod spec;
mod types;
mod verify;

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::B256;
use libp2p::{Multiaddr, noise};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::TrustedL1Block;
use crate::beacon::client::Client;
use crate::beacon::rpc::StatusData;
use crate::beacon::spec::{MAINNET, SLOTS_PER_EPOCH};

/// Configuration of the beacon light client.
#[derive(Debug, Clone)]
pub struct BeaconConfig {
    /// Root of a finalized beacon block the operator trusts: the light client starts from
    /// it. It must be recent: peers serve the bootstrap of a checkpoint for a limited time.
    pub checkpoint: B256,
    /// Listen address, used for both discovery (UDP) and libp2p (TCP). It must differ from
    /// the other networks' addresses.
    pub listen_addr: SocketAddr,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs. Beacon nodes share their
    /// discovery network with execution nodes, so the chain's bootnodes work.
    pub bootnodes: Vec<String>,
}

/// What stops the beacon light client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BeaconError {
    /// The discovery node could not be created or started (usually: the UDP port is taken).
    #[error("failed to start beacon discovery: {0}")]
    Discovery(String),
    /// The encrypted transport could not be set up.
    #[error("failed to set up the beacon transport")]
    Transport(#[source] noise::Error),
    /// Gossipsub could not be set up.
    #[error("failed to set up beacon gossip: {0}")]
    Gossip(String),
    /// The TCP listen address could not be bound.
    #[error("failed to listen for beacon peers on {0}")]
    Listen(Multiaddr, #[source] libp2p::TransportError<std::io::Error>),
    /// Peers do not hold the bootstrap of the configured checkpoint: it is too old, or not
    /// the root of a finalized block. The operator has to configure a newer one.
    #[error(
        "{peers} beacon peers do not hold the light-client bootstrap of checkpoint \
         {checkpoint}: configure the root of a more recent finalized beacon block"
    )]
    CheckpointUnavailable {
        /// The configured checkpoint.
        checkpoint: B256,
        /// Peers that were asked and did not have it.
        peers: usize,
    },
    /// The verification task panicked.
    #[error("light-client verification task failed")]
    Verification(#[source] tokio::task::JoinError),
}

/// The beacon light client of Ethereum mainnet.
#[derive(Debug)]
pub struct LightClient {
    config: BeaconConfig,
    trusted: mpsc::Sender<TrustedL1Block>,
}

impl LightClient {
    /// Creates the light client. Does no I/O.
    ///
    /// Each L1 execution block it verifies is sent on `trusted`: the checkpoint's own block
    /// first, then every newer finalized block (`finalized: true`) and every newer head as
    /// the sync committee attested it (`finalized: false`).
    #[must_use]
    pub const fn new(config: BeaconConfig, trusted: mpsc::Sender<TrustedL1Block>) -> Self {
        Self { config, trusted }
    }

    /// Runs the light client until `cancel` fires or the receiver of trusted blocks is
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns [`BeaconError`] if a socket cannot be bound, or peers do not hold the
    /// bootstrap of the configured checkpoint.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), BeaconError> {
        let Self { config, trusted } = self;
        let spec = &MAINNET;
        let now = SystemTime::now().duration_since(UNIX_EPOCH);
        let epoch = spec.slot_at(now.map_or(0, |since| since.as_secs())) / SLOTS_PER_EPOCH;
        let digest = spec.fork_digest(epoch);

        info!(
            checkpoint = %config.checkpoint,
            fork_digest = %alloy_primitives::hex::encode(digest),
            listen = %config.listen_addr,
            "beacon light client starting"
        );

        // Until the bootstrap: a node that has synced nothing.
        let (status, status_rx) = watch::channel(StatusData {
            finalized_root: B256::ZERO,
            finalized_epoch: 0,
            head_root: spec.genesis_block_root,
            head_slot: 0,
        });
        let (network, handle, gossip) =
            network::new(config.listen_addr, config.bootnodes, digest, status_rx)?;
        let client = Client::new(
            spec,
            config.checkpoint,
            digest,
            handle,
            gossip,
            status,
            trusted,
        );
        // Either part ending stops the other.
        let stop = cancel.child_token();
        let (networking, result) = tokio::join!(
            async {
                let result = Box::pin(network.run(stop.clone())).await;
                stop.cancel();
                result
            },
            async {
                let result = client.run(stop.clone()).await;
                stop.cancel();
                result
            },
        );
        networking?;
        result
    }
}
