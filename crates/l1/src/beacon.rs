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
//! configuration. Everything after it is verified. The newest finalized block verified is
//! published ([`BeaconCheckpoint`]) for the node to save, and a restart bootstraps from the
//! saved one (well under a minute), so the configured checkpoint only has to be recent on the
//! first start. A saved block adds no trust: it was verified from a configured checkpoint.
//! Uses no RPC. See `docs/l1.md`.

mod client;
mod discovery;
mod network;
mod rpc;
mod spec;
mod types;
mod verify;

use std::net::SocketAddr;

use alloy_primitives::B256;
use libp2p::{Multiaddr, noise};
use op_indexer_primitives::BeaconCheckpoint;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::TrustedL1Block;
use crate::beacon::client::{Bootstrap, Client};
use crate::beacon::rpc::StatusData;
use crate::beacon::spec::{MAINNET, SLOTS_PER_EPOCH};

/// Configuration of the beacon light client.
#[derive(Debug, Clone)]
pub struct BeaconConfig {
    /// Root of a finalized beacon block the operator trusts: the light client starts from
    /// it on the first start, or when it is newer than [`Self::saved`]. It must be recent
    /// then: peers serve the bootstrap of a checkpoint for a limited time.
    pub checkpoint: B256,
    /// The newest finalized block a previous run verified, if the node saved one: started
    /// from when it descends from [`Self::checkpoint`], else used as the fallback
    /// (`docs/l1.md` §8, "Restart").
    pub saved: Option<BeaconCheckpoint>,
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
    /// Peers hold the bootstrap of neither the configured checkpoint nor the saved one: both
    /// are too old, or not the root of a finalized block. The operator has to configure a
    /// newer one.
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
    finalized: watch::Sender<Option<BeaconCheckpoint>>,
    peer_count: watch::Sender<usize>,
}

impl LightClient {
    /// Creates the light client. Does no I/O.
    ///
    /// Each L1 execution block it verifies is sent on `trusted`: the checkpoint's own block
    /// first, then every newer finalized block (`finalized: true`) and every newer head as
    /// the sync committee attested it (`finalized: false`). The beacon block of each of
    /// those finalized blocks is published on `finalized`, for the node to save and pass
    /// back as [`BeaconConfig::saved`] after a restart: about once an epoch.
    #[must_use]
    pub fn new(
        config: BeaconConfig,
        trusted: mpsc::Sender<TrustedL1Block>,
        finalized: watch::Sender<Option<BeaconCheckpoint>>,
    ) -> Self {
        Self {
            config,
            trusted,
            finalized,
            peer_count: watch::Sender::new(0),
        }
    }

    /// The number of beacon peers connected, kept current while the light client runs.
    #[must_use]
    pub fn peer_count(&self) -> watch::Receiver<usize> {
        self.peer_count.subscribe()
    }

    /// Runs the light client until `cancel` fires or the receiver of trusted blocks is
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns [`BeaconError`] if a socket cannot be bound, or peers hold the bootstrap of
    /// neither the saved nor the configured checkpoint.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), BeaconError> {
        let Self {
            config,
            trusted,
            finalized,
            peer_count,
        } = self;
        let spec = &MAINNET;
        let epoch = spec.now_slot() / SLOTS_PER_EPOCH;
        let digest = spec.fork_digest(epoch);

        let bootstraps = bootstraps(config.checkpoint, config.saved);
        info!(
            checkpoint = %bootstraps.0.root,
            saved = bootstraps.0.root != config.checkpoint,
            saved_slot = config.saved.map(|saved| saved.slot),
            fallback = ?bootstraps.1.map(|fallback| fallback.root),
            fork_digest = %alloy_primitives::hex::encode(digest),
            listen = %config.listen_addr,
            "beacon light client starting"
        );

        // Always a node that has synced nothing: a light client holds no beacon blocks to serve
        // and SHOULD report genesis in its `Status` (light client networking, "Light clients":
        // https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/p2p-interface.md#light-clients).
        let status = StatusData {
            finalized_root: B256::ZERO,
            finalized_epoch: 0,
            head_root: spec.genesis_block_root,
            head_slot: 0,
        };
        let (network, handle, gossip) = network::new(
            config.listen_addr,
            config.bootnodes,
            digest,
            status,
            peer_count,
        )?;
        let client = Client::new(spec, bootstraps, handle, gossip, trusted, finalized);
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

/// The root to bootstrap from, and the one to try if peers do not hold it, as
/// [`BeaconConfig::saved`] says.
fn bootstraps(configured: B256, saved: Option<BeaconCheckpoint>) -> (Bootstrap, Option<Bootstrap>) {
    let from_configured = Bootstrap {
        root: configured,
        origin: configured,
    };
    match saved.filter(|saved| saved.root != configured) {
        None => (from_configured, None),
        // Verified from the configured checkpoint, so newer than it.
        Some(saved) if saved.origin == configured => (saved.into(), Some(from_configured)),
        // Verified from another: the operator configured a new checkpoint since.
        Some(saved) => (from_configured, Some(saved.into())),
    }
}
