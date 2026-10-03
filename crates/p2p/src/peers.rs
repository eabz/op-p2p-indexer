//! Eviction of connected peers that hold a connection slot without joining block gossip.
//!
//! Only subscription is judged. Delivery is not: gossipsub credits a block to the first peer
//! that forwards it and peers outside our mesh only announce, so an honest peer can go a long
//! time without a delivery to its name. Under-delivering mesh peers are left to peer scoring.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use libp2p::PeerId;

/// How long a connected peer has to subscribe to one of our block topics. Gossipsub sends
/// subscriptions in its first RPC, so real peers subscribe within seconds of connecting; the
/// margin covers slow links and peers that are still starting up.
const SUBSCRIBE_GRACE: Duration = Duration::from_secs(30);

/// When each connected peer connected. One entry per connected peer, so the connection limit
/// bounds it.
#[derive(Debug, Default)]
pub(crate) struct ConnectedPeers {
    since: HashMap<PeerId, Instant>,
}

impl ConnectedPeers {
    /// Records that `peer` connected at `now`.
    pub(crate) fn connected(&mut self, peer: PeerId, now: Instant) {
        self.since.entry(peer).or_insert(now);
    }

    /// Forgets `peer` once its connection closed.
    pub(crate) fn disconnected(&mut self, peer: &PeerId) {
        self.since.remove(peer);
    }

    /// Returns the peers to evict at `now`: connected for longer than [`SUBSCRIBE_GRACE`] and
    /// not `is_subscribed` to any of our block topics, whether they never subscribed or left.
    pub(crate) fn idle(
        &self,
        now: Instant,
        is_subscribed: impl Fn(&PeerId) -> bool,
    ) -> Vec<PeerId> {
        self.since
            .iter()
            .filter(|&(peer, &since)| {
                now.duration_since(since) > SUBSCRIBE_GRACE && !is_subscribed(peer)
            })
            .map(|(&peer, _)| peer)
            .collect()
    }
}
