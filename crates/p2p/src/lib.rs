//! OP Stack p2p: discv5 discovery, gossipsub block gossip, and unsafe-block validation.
//!
//! Protocols come from maintained crates (`discv5`, `libp2p` with gossipsub and connection
//! limits, op-alloy payload types); this crate adds what is OP-specific:
//!
//! ```text
//! discv5 (opstack ENR filter) ─▶ dial (backoff, limits) ─▶ gossipsub (snappy, OP message id,
//!     StrictNoSign, peer scoring) ─▶ block validation (bounded, blocking) ─▶ accept/reject
//!     ─▶ UnsafeBlock channel
//! ```
//!
//! [`NodeStore`] persists node state (the identity key and known good peers) in an embedded
//! fjall database, a directory of its own.
//! Knows nothing about block storage; consumers receive [`UnsafeBlock`]s over a channel.
//!
//! [`UnsafeBlock`]: op_indexer_primitives::UnsafeBlock

mod block;
mod bootnode;
mod config;
mod discovery;
mod gossip;
mod metrics;
mod network;
mod peers;
mod store;
mod sync;

pub use bootnode::{Bootnode, BootnodeError};
pub use config::NetworkConfig;
pub use discovery::DiscoveryError;
pub use gossip::GossipError;
pub use network::{Network, NetworkError};
pub use store::{NodeStore, StoreError};
pub use sync::{BlockFuture, PayloadSource};
