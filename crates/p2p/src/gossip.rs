//! Gossipsub configured per the OP Stack p2p spec.
//!
//! `libp2p-gossipsub` handles the protocol: meshes, IHAVE/IWANT, deduplication, forwarding, and
//! peer scoring. We supply the OP-specific parts: snappy as a data transform (so messages are
//! decompressed once, on receipt), the message id, `StrictNoSign` (anonymous messages), size
//! limits, mesh and scoring parameters, and manual validation, so only messages
//! [`crate::block`] accepts are forwarded.
//!
//! Gossipsub calls the data transform and the message id function inline, so snappy
//! decompression and the message-id hash run on the swarm task. That is why [`Snappy`] checks
//! the decompressed size before allocating.
//! See <https://specs.optimism.io/protocol/rollup-node-p2p.html#gossipsub-configuration>.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use alloy_primitives::ChainId;
use libp2p::gossipsub::{
    self, DataTransform, IdentTopic, Message, MessageAuthenticity, MessageId, PeerScoreParams,
    PeerScoreThresholds, RawMessage, TopicHash, TopicScoreParams, ValidationMode,
};
use op_indexer_primitives::PayloadVersion;
use sha2::{Digest, Sha256};

use crate::block;
use crate::metrics;

/// Gossipsub behaviour with snappy decompression applied to every received message.
pub(crate) type Behaviour = gossipsub::Behaviour<Snappy>;

/// Maximum decompressed size of a gossip message, per the OP p2p spec.
const MAX_GOSSIP_SIZE: usize = 10 * 1024 * 1024;
/// Gossipsub mesh target (D); discovery searches aggressively until this many peers are subscribed.
pub(crate) const MESH_TARGET: usize = 8;
const MESH_LOW: usize = 6;
const MESH_HIGH: usize = 12;
const GOSSIP_LAZY: usize = 6;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const FANOUT_TTL: Duration = Duration::from_secs(24);
/// Heartbeats of message history kept for IWANT replies.
const HISTORY_LENGTH: usize = 12;
/// Heartbeats of history advertised in IHAVE gossip.
const HISTORY_GOSSIP: usize = 3;
/// 130 heartbeats of 0.5s.
const SEEN_TTL: Duration = Duration::from_secs(65);
/// Below this score a peer's RPCs are ignored (and the network disconnects it).
///
/// About three rejected blocks put a peer below it (see [`peer_score_params`]). Blocks dated
/// more than a few seconds in the future are rejected, so a local clock running behind by more
/// than that makes every honest peer look invalid and gets them all disconnected: the host
/// clock must be kept in sync (NTP).
pub(crate) const GRAYLIST_THRESHOLD: f64 = -40.0;
/// Message id domain for valid snappy payloads; invalid ones are dropped by [`Snappy`].
const DOMAIN_VALID_SNAPPY: [u8; 4] = [1, 0, 0, 0];
/// Bytes of the SHA-256 digest kept as the message id.
const MESSAGE_ID_LEN: usize = 20;
/// Slots in op-node's scoring "epoch".
const SLOTS_PER_EPOCH: u32 = 6;
/// Time over which the invalid-delivery penalty halves: about 2.3 minutes, 69 slots of 2 s.
const INVALID_DELIVERY_HALF_LIFE: Duration = Duration::from_secs(138);

/// Errors setting up gossipsub.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GossipError {
    /// The gossipsub configuration was rejected.
    #[error("invalid gossipsub configuration")]
    Config(#[source] gossipsub::ConfigBuilderError),
    /// The gossipsub behaviour could not be created.
    #[error("failed to create gossipsub: {0}")]
    Behaviour(&'static str),
    /// The peer scoring parameters were rejected.
    // `String` is the error type of libp2p-gossipsub's `with_peer_score`.
    #[error("invalid gossipsub peer scoring parameters: {0}")]
    Score(String),
    /// A block topic could not be subscribed to, so its blocks would never arrive.
    #[error("failed to subscribe to topic {topic}")]
    Subscribe {
        /// The topic.
        topic: TopicHash,
        /// Why gossipsub refused it.
        #[source]
        source: gossipsub::SubscriptionError,
    },
}

/// Builds the gossipsub behaviour, subscribed to every block topic of `chain_id`, and returns it
/// with the payload version each topic carries. Every received message must then be validated.
/// `block_time` is the chain's, the slot that peer scores decay by.
///
/// # Errors
///
/// Returns [`GossipError`] if gossipsub rejects the configuration or the scoring parameters, or
/// a block topic cannot be subscribed to.
pub(crate) fn behaviour(
    chain_id: ChainId,
    block_time: Duration,
) -> Result<(Behaviour, HashMap<TopicHash, PayloadVersion>), GossipError> {
    let config = gossipsub::ConfigBuilder::default()
        .mesh_n(MESH_TARGET)
        .mesh_n_low(MESH_LOW)
        .mesh_n_high(MESH_HIGH)
        .gossip_lazy(GOSSIP_LAZY)
        .heartbeat_interval(HEARTBEAT_INTERVAL)
        .fanout_ttl(FANOUT_TTL)
        .history_length(HISTORY_LENGTH)
        .history_gossip(HISTORY_GOSSIP)
        .duplicate_cache_time(SEEN_TTL)
        .max_transmit_size(MAX_GOSSIP_SIZE)
        // StrictNoSign: a message carrying an author, sequence number, or signature is rejected.
        .validation_mode(ValidationMode::Anonymous)
        // Hold messages until we report a validation result; only accepted ones are forwarded.
        .validate_messages()
        .message_id_fn(message_id)
        .build()
        .map_err(GossipError::Config)?;
    let mut behaviour =
        Behaviour::new_with_transform(MessageAuthenticity::Anonymous, config, Snappy)
            .map_err(GossipError::Behaviour)?;

    let mut topics = HashMap::new();
    for version in block::VERSIONS {
        let topic = IdentTopic::new(block::topic(chain_id, version));
        behaviour
            .subscribe(&topic)
            .map_err(|source| GossipError::Subscribe {
                topic: topic.hash(),
                source,
            })?;
        topics.insert(topic.hash(), version);
    }

    behaviour
        .with_peer_score(
            peer_score_params(topics.keys(), block_time),
            peer_score_thresholds(),
        )
        .map_err(GossipError::Score)?;
    Ok((behaviour, topics))
}

/// Peer scoring: op-node's "light" parameters, plus a mild invalid-message penalty on our block
/// topics.
///
/// The peer-level values are `LightPeerScoreParams` from [`op-node/p2p/peer_params.go`]. Two
/// terms differ from op-node:
///
/// - Per-topic scoring. op-node uses none, so REJECTs cost a peer nothing there. We score only
///   invalid deliveries, squared and weighted by -5: one costs 5, two cost 20, and three cost 45,
///   which is below [`GRAYLIST_THRESHOLD`], so the peer is disconnected. That tolerates a
///   one-off.
/// - The slow-peer penalty, which only rust-libp2p has; it keeps the library default.
///
/// The slot is the chain's block time, as in op-node (which falls back to 2 s when it is
/// unset).
///
/// [`op-node/p2p/peer_params.go`]: https://github.com/ethereum-optimism/optimism/blob/c8e4ba855d79ca56463909ef5a2c5830a1189401/op-node/p2p/peer_params.go
fn peer_score_params<'a>(
    topics: impl Iterator<Item = &'a TopicHash>,
    block_time: Duration,
) -> PeerScoreParams {
    let slot = block_time;
    let epoch = slot.saturating_mul(SLOTS_PER_EPOCH);
    let topic_params = TopicScoreParams {
        topic_weight: 1.0,
        time_in_mesh_weight: 0.0,
        first_message_deliveries_weight: 0.0,
        mesh_message_deliveries_weight: 0.0,
        mesh_failure_penalty_weight: 0.0,
        invalid_message_deliveries_weight: -5.0,
        // 0.99 per slot at 2 s blocks.
        invalid_message_deliveries_decay: gossipsub::score_parameter_decay_with_base(
            INVALID_DELIVERY_HALF_LIFE,
            slot,
            0.5,
        ),
        ..TopicScoreParams::default()
    };
    PeerScoreParams {
        topics: topics
            .map(|topic| (topic.clone(), topic_params.clone()))
            .collect(),
        decay_interval: slot,
        decay_to_zero: 0.01,
        app_specific_weight: 1.0,
        ip_colocation_factor_weight: -35.0,
        ip_colocation_factor_threshold: 10.0,
        behaviour_penalty_weight: -16.0,
        behaviour_penalty_threshold: 6.0,
        behaviour_penalty_decay: gossipsub::score_parameter_decay_with_base(epoch * 10, slot, 0.01),
        retain_score: epoch * 100,
        ..PeerScoreParams::default()
    }
}

/// Score thresholds, op-node's `NewPeerScoreThresholds`: stop gossiping below -10, stop
/// publishing below -40, and ignore all RPCs (graylist) below -40.
fn peer_score_thresholds() -> PeerScoreThresholds {
    PeerScoreThresholds {
        gossip_threshold: -10.0,
        publish_threshold: -40.0,
        graylist_threshold: GRAYLIST_THRESHOLD,
        accept_px_threshold: 20.0,
        opportunistic_graft_threshold: 0.05,
    }
}

/// `SHA256(domain ++ decompressed data)[:20]`. The topic is not hashed, unlike L1 gossip.
fn message_id(message: &Message) -> MessageId {
    let mut id = Sha256::new()
        .chain_update(DOMAIN_VALID_SNAPPY)
        .chain_update(&message.data)
        .finalize()
        .to_vec();
    id.truncate(MESSAGE_ID_LEN);
    MessageId::from(id)
}

/// Snappy block (de)compression of message data, checking the size before allocating.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Snappy;

impl DataTransform for Snappy {
    fn inbound_transform(&self, raw: RawMessage) -> Result<Message, io::Error> {
        let data = decompress(&raw.data).inspect_err(|_| metrics::gossip_decompress_failed())?;
        Ok(Message {
            source: raw.source,
            data,
            sequence_number: raw.sequence_number,
            topic: raw.topic,
        })
    }

    fn outbound_transform(&self, _topic: &TopicHash, data: Vec<u8>) -> Result<Vec<u8>, io::Error> {
        Ok(snap::raw::Encoder::new().compress_vec(&data)?)
    }
}

fn decompress(data: &[u8]) -> Result<Vec<u8>, io::Error> {
    let len = snap::raw::decompress_len(data)?;
    if len > MAX_GOSSIP_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "gossip message too large",
        ));
    }
    Ok(snap::raw::Decoder::new().decompress_vec(data)?)
}
