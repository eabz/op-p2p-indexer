//! Discovery bootnodes: `enr:` records, or `enode://` URLs.
//!
//! op-node publishes most OP Stack bootnodes as enodes,
//! `enode://<uncompressed secp256k1 key>@<ip>:<port>[?discport=<udp port>]`. An enode carries no
//! signed record, so discovery contacts it at its UDP address and asks for its ENR.

use std::net::SocketAddr;
use std::str::FromStr;

use alloy_primitives::hex;
use discv5::Enr;
use libp2p::identity::{PublicKey, secp256k1};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};

/// A discovery bootnode.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Bootnode {
    /// A signed node record, added to the routing table directly.
    Enr(Enr),
    /// An enode's discovery address, `/ip4|ip6/<ip>/udp/<port>/p2p/<peer id>`, whose ENR is
    /// requested at startup.
    Enode(Multiaddr),
}

/// Why a bootnode string could not be parsed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BootnodeError {
    /// The `enr:` record is invalid.
    #[error("invalid enr record: {0}")]
    Enr(String),
    /// The `enode://` URL is malformed, has a DNS host, or has an invalid key.
    #[error("invalid enode url (expected enode://<key>@<ip>:<port>[?discport=<port>])")]
    Enode,
    /// The string is neither an `enr:` record nor an `enode://` URL.
    #[error("bootnode must start with enr: or enode://")]
    UnknownFormat,
}

impl FromStr for Bootnode {
    type Err = BootnodeError;

    fn from_str(bootnode: &str) -> Result<Self, Self::Err> {
        if bootnode.starts_with("enr:") {
            return bootnode.parse().map(Self::Enr).map_err(BootnodeError::Enr);
        }
        let enode = bootnode
            .strip_prefix("enode://")
            .ok_or(BootnodeError::UnknownFormat)?;
        enode_discovery_addr(enode)
            .map(Self::Enode)
            .ok_or(BootnodeError::Enode)
    }
}

/// Parses `<key>@<ip>:<port>[?discport=<udp port>]` into a discv5 contact address.
fn enode_discovery_addr(enode: &str) -> Option<Multiaddr> {
    let (key_hex, endpoint) = enode.split_once('@')?;
    let (endpoint, query) = endpoint.split_once('?').unwrap_or((endpoint, ""));
    // `<ipv4>:<port>` or `[<ipv6>]:<port>`.
    let socket: SocketAddr = endpoint.parse().ok()?;
    let udp_port = match query.strip_prefix("discport=") {
        Some(port) => port.parse().ok()?,
        None => socket.port(),
    };

    let key = hex::decode(key_hex).ok()?;
    let public = secp256k1::PublicKey::try_from_bytes(&[[0x04].as_slice(), &key].concat()).ok()?;
    Multiaddr::from(socket.ip())
        .with(Protocol::Udp(udp_port))
        .with_p2p(PeerId::from_public_key(&PublicKey::from(public)))
        .ok()
}
