//! The swarm's protocols: connection limits, identify, gossipsub on the two light-client
//! topics, and one request/response behaviour per protocol of the beacon network this node
//! speaks.
//!
//! Holds no state about peers and decides nothing: see `network`.

use std::io;
use std::time::Duration;

use alloy_primitives::hex;
use libp2p::connection_limits::{self, ConnectionLimits};
use libp2p::gossipsub::{
    self, DataTransform, IdentTopic, MessageAuthenticity, MessageId, RawMessage, TopicHash,
    ValidationMode, WhitelistSubscriptionFilter,
};
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{StreamProtocol, identify, identity};
use sha2::{Digest, Sha256};

use super::REQUEST_TIMEOUT;
use crate::beacon::BeaconError;
use crate::beacon::rpc::{self, Chunk, Codec};
use crate::beacon::spec::ForkDigest;

/// Connections at most: the peers, the dials in progress, and a few that have not said yet
/// what they serve.
const MAX_CONNECTIONS: u32 = 24;
/// Connections peers opened whose handshake is in progress.
const MAX_PENDING_INBOUND: u32 = 8;
/// Largest gossip frame read from a peer. Updates are a few kilobytes; the rest of a frame is
/// gossipsub's own control messages.
const MAX_GOSSIP_FRAME_BYTES: usize = 1024 * 1024;
/// Gossipsub's heartbeat on the beacon network ([gossipsub parameters]).
///
/// [gossipsub parameters]: https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#the-gossip-domain-gossipsub
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(700);
/// `MESSAGE_DOMAIN_VALID_SNAPPY`: the first bytes hashed into a message id.
const MESSAGE_DOMAIN_VALID_SNAPPY: [u8; 4] = [1, 0, 0, 0];
/// Bytes of a message id.
const MESSAGE_ID_LEN: usize = 20;

/// Gossipsub with snappy-compressed data, tracking only the light-client topics.
pub(super) type Gossipsub = gossipsub::Behaviour<Snappy, WhitelistSubscriptionFilter>;

/// The request/response protocols a request goes out on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Asked {
    Bootstrap,
    Updates,
    Finality,
    Optimistic,
}

impl Asked {
    pub(super) const ALL: [Self; 4] = [
        Self::Bootstrap,
        Self::Updates,
        Self::Finality,
        Self::Optimistic,
    ];

    /// The protocol name the request goes out on.
    pub(super) const fn protocol(self) -> &'static str {
        match self {
            Self::Bootstrap => rpc::BOOTSTRAP,
            Self::Updates => rpc::UPDATES_BY_RANGE,
            Self::Finality => rpc::FINALITY_UPDATE,
            Self::Optimistic => rpc::OPTIMISTIC_UPDATE,
        }
    }
}

/// The swarm's protocols. Each request/response protocol is a behaviour of its own: a
/// behaviour sends a request on the first protocol the peer supports, so they cannot share
/// one.
#[derive(NetworkBehaviour)]
pub(super) struct Behaviour {
    pub(super) limits: connection_limits::Behaviour,
    pub(super) identify: identify::Behaviour,
    pub(super) gossipsub: Gossipsub,
    pub(super) status: request_response::Behaviour<Codec>,
    pub(super) ping: request_response::Behaviour<Codec>,
    pub(super) metadata_v2: request_response::Behaviour<Codec>,
    pub(super) metadata_v3: request_response::Behaviour<Codec>,
    pub(super) goodbye: request_response::Behaviour<Codec>,
    pub(super) bootstrap: request_response::Behaviour<Codec>,
    pub(super) updates: request_response::Behaviour<Codec>,
    pub(super) finality: request_response::Behaviour<Codec>,
    pub(super) optimistic: request_response::Behaviour<Codec>,
}

impl Behaviour {
    pub(super) fn new(key: &identity::Keypair, gossipsub: Gossipsub) -> Self {
        let protocol = |name: &'static str, support| {
            let config = request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT);
            let protocols = [(StreamProtocol::new(name), support)];
            request_response::Behaviour::with_codec(Codec::of(name), protocols, config)
        };
        let identify = identify::Config::new("eth2/1.0.0".to_owned(), key.public())
            .with_agent_version(concat!("op-indexer/", env!("CARGO_PKG_VERSION")).to_owned());
        let limits = ConnectionLimits::default()
            .with_max_established(Some(MAX_CONNECTIONS))
            .with_max_pending_incoming(Some(MAX_PENDING_INBOUND))
            .with_max_established_per_peer(Some(1));
        Self {
            limits: connection_limits::Behaviour::new(limits),
            identify: identify::Behaviour::new(identify),
            gossipsub,
            status: protocol(rpc::STATUS, ProtocolSupport::Full),
            ping: protocol(rpc::PING, ProtocolSupport::Inbound),
            metadata_v2: protocol(rpc::METADATA_V2, ProtocolSupport::Inbound),
            metadata_v3: protocol(rpc::METADATA_V3, ProtocolSupport::Inbound),
            goodbye: protocol(rpc::GOODBYE, ProtocolSupport::Inbound),
            bootstrap: protocol(rpc::BOOTSTRAP, ProtocolSupport::Outbound),
            updates: protocol(rpc::UPDATES_BY_RANGE, ProtocolSupport::Outbound),
            finality: protocol(rpc::FINALITY_UPDATE, ProtocolSupport::Outbound),
            optimistic: protocol(rpc::OPTIMISTIC_UPDATE, ProtocolSupport::Outbound),
        }
    }

    pub(super) fn requests(&mut self, asked: Asked) -> &mut request_response::Behaviour<Codec> {
        match asked {
            Asked::Bootstrap => &mut self.bootstrap,
            Asked::Updates => &mut self.updates,
            Asked::Finality => &mut self.finality,
            Asked::Optimistic => &mut self.optimistic,
        }
    }
}

pub(super) type RpcEvent = request_response::Event<Vec<u8>, Vec<Chunk>>;

/// Snappy block decompression of gossip data, checking the size before allocating
/// ([gossip encoding]).
///
/// [gossip encoding]: https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#encodings
#[derive(Debug, Clone, Copy)]
pub(super) struct Snappy;

impl DataTransform for Snappy {
    fn inbound_transform(&self, raw: RawMessage) -> Result<gossipsub::Message, io::Error> {
        let len = snap::raw::decompress_len(&raw.data)?;
        if len > rpc::MAX_LIGHT_CLIENT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gossip message too large",
            ));
        }
        Ok(gossipsub::Message {
            source: raw.source,
            data: snap::raw::Decoder::new().decompress_vec(&raw.data)?,
            sequence_number: raw.sequence_number,
            topic: raw.topic,
        })
    }

    fn outbound_transform(&self, _: &TopicHash, data: Vec<u8>) -> Result<Vec<u8>, io::Error> {
        Ok(snap::raw::Encoder::new().compress_vec(&data)?)
    }
}

/// `SHA256(domain ‖ len(topic) ‖ topic ‖ decompressed data)[:20]` ([message id], Altair).
///
/// [message id]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/p2p-interface.md#topics-and-messages
fn message_id(message: &gossipsub::Message) -> MessageId {
    let topic = message.topic.as_str().as_bytes();
    let topic_len = u64::try_from(topic.len()).unwrap_or(u64::MAX);
    let mut id = Sha256::new()
        .chain_update(MESSAGE_DOMAIN_VALID_SNAPPY)
        .chain_update(topic_len.to_le_bytes())
        .chain_update(topic)
        .chain_update(&message.data)
        .finalize()
        .to_vec();
    id.truncate(MESSAGE_ID_LEN);
    MessageId::from(id)
}

/// Builds gossipsub, subscribed to the two light-client topics of `digest`, and returns it
/// with their hashes: the finality topic's, then the optimistic topic's.
pub(super) fn gossip(digest: ForkDigest) -> Result<(Gossipsub, TopicHash, TopicHash), BeaconError> {
    let config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(HEARTBEAT_INTERVAL)
        .max_transmit_size(MAX_GOSSIP_FRAME_BYTES)
        // StrictNoSign: a message carrying an author, sequence number, or signature is rejected.
        .validation_mode(ValidationMode::Anonymous)
        // Nothing is forwarded until a validation result is reported.
        .validate_messages()
        .message_id_fn(message_id)
        .build()
        .map_err(|err| BeaconError::Gossip(err.to_string()))?;
    let topic = |name: &str| {
        let digest = hex::encode(digest);
        IdentTopic::new(format!("/eth2/{digest}/{name}/ssz_snappy"))
    };
    let topics = [
        topic("light_client_finality_update"),
        topic("light_client_optimistic_update"),
    ];
    let [finality, optimistic] = topics.each_ref().map(IdentTopic::hash);
    // Peers subscribe to hundreds of topics; only ours are tracked.
    let ours = WhitelistSubscriptionFilter([finality.clone(), optimistic.clone()].into());
    let authenticity = MessageAuthenticity::Anonymous;
    let mut behaviour =
        Gossipsub::new_with_subscription_filter_and_transform(authenticity, config, ours, Snappy)
            .map_err(|err| BeaconError::Gossip(err.to_owned()))?;
    for topic in &topics {
        behaviour
            .subscribe(topic)
            .map_err(|err| BeaconError::Gossip(err.to_string()))?;
    }
    Ok((behaviour, finality, optimistic))
}
