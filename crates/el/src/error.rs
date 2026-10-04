//! The error that stops the execution network.

use std::io;
use std::net::SocketAddr;

/// Why the execution network could not start or had to stop.
///
/// Failures with one peer are not here: they end that session, see `session`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ElError {
    /// The node key is not a valid secp256k1 secret.
    #[error("invalid execution node key")]
    InvalidKey,
    /// The local node record could not be built or signed.
    #[error("failed to build the local node record")]
    Enr(#[source] enr::Error),
    /// The discv5 service could not be created.
    #[error("failed to create discv5: {0}")]
    DiscoveryCreate(&'static str),
    /// The discv5 service could not start (usually: the UDP port is taken).
    #[error("failed to start discv5: {0}")]
    DiscoveryStart(discv5::Error),
    /// The session listener could not bind its TCP port.
    #[error("failed to listen for execution peers on {addr}")]
    Listen {
        /// The address that could not be bound.
        addr: SocketAddr,
        /// The bind error.
        #[source]
        source: io::Error,
    },
    /// One of the network's tasks ended by panicking or being aborted.
    #[error("execution network task {task} failed")]
    Task {
        /// The task.
        task: &'static str,
        /// The join error.
        #[source]
        source: tokio::task::JoinError,
    },
    /// The range sync reached blocks of the chain this build cannot read. No peer can serve
    /// them differently, so the sync cannot continue.
    #[error("range sync cannot continue: {0}")]
    Sync(String),
    /// A channel a component needs was closed while the network was still running.
    #[error("execution network channel {channel} closed")]
    ChannelClosed {
        /// The channel.
        channel: &'static str,
    },
}
