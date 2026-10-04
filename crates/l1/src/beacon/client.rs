//! The light client's state machine: decides what to ask the beacon network for, verifies
//! what comes back, and passes on the L1 blocks it vouches for.
//!
//! One thing is done at a time, in this order of need:
//!
//! 1. no store yet: ask for the bootstrap of the configured checkpoint;
//! 2. the next sync committee is not known, or the clock is more than one period ahead of
//!    the store: ask for `LightClientUpdatesByRange` from the store's period (one update per
//!    period, each proving the committee that signs the next);
//! 3. a finality or optimistic update arrived over gossip: take it;
//! 4. when no update verified for a while: ask for the optimistic update every slot, and
//!    once an epoch for the finality update.
//!
//! Each answer and gossip message is verified off the runtime (`verify`) against a copy of
//! the store, which replaces the store when it verifies. A peer whose data fails
//! verification is reported to the network, which drops it.
//!
//! Does not touch the swarm, pick peers or frame messages: see `network`.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use alloy_primitives::{B256, Bytes};
use libp2p::PeerId;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::BeaconError;
use super::network::{Gossip, NetworkHandle, Request, RequestError, Response, Topic, Verdict};
use super::rpc::StatusData;
use super::spec::{BeaconSpec, ForkDigest, SLOTS_PER_EPOCH, SLOTS_PER_PERIOD};
use super::verify::{Accepted, Store, VerifyError};
use crate::TrustedL1Block;

/// How often the loop looks for something to ask.
const TICK: Duration = Duration::from_secs(1);
/// Time between two polls for the newest update: a slot.
const POLL_INTERVAL: Duration = Duration::from_secs(12);
/// How long polling stays quiet after an update that verified: gossip delivers one every
/// slot, and polling only fills in when it stops.
const QUIET_AFTER_UPDATE: Duration = Duration::from_secs(30);
/// Time between two attempts to learn the next sync committee, which peers can only prove
/// once a block of the period is finalized.
const COMMITTEE_RETRY: Duration = Duration::from_secs(600);
/// The same while the store cannot follow without it: the clock is past the last period
/// whose committee is known.
const CATCH_UP_RETRY: Duration = Duration::from_secs(20);
/// Peers that must say they do not hold the checkpoint's bootstrap before the checkpoint is
/// given up as too old.
const BOOTSTRAP_REFUSALS: usize = 12;

/// What a piece of light-client data is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Bootstrap,
    Updates,
    Finality,
    Optimistic,
}

/// The answer to the request in flight, once it comes.
type Answer = Pin<Box<dyn Future<Output = Result<Response, RequestError>> + Send>>;

/// Resolves with the answer to the request in flight; never, if there is none.
async fn answered(
    pending: &mut Option<(Kind, Answer)>,
) -> Option<(Kind, Result<Response, RequestError>)> {
    match pending {
        Some((kind, answer)) => Some((*kind, answer.await)),
        None => std::future::pending().await,
    }
}

/// A verified answer: the store after it, and what it changed.
#[derive(Debug)]
struct Verified {
    store: Store,
    accepted: Accepted,
    /// The checkpoint's own execution block, when the answer was the bootstrap.
    bootstrapped: Option<TrustedL1Block>,
}

/// The light client's state machine.
#[derive(Debug)]
pub(super) struct Client {
    spec: &'static BeaconSpec,
    checkpoint: B256,
    digest: ForkDigest,
    network: NetworkHandle,
    gossip: mpsc::Receiver<Gossip>,
    /// What the network reports about this node in `Status`: follows the store.
    status: watch::Sender<StatusData>,
    trusted: mpsc::Sender<TrustedL1Block>,
    store: Option<Store>,
    /// Peers that said they do not hold the checkpoint's bootstrap.
    refused: HashSet<PeerId>,
    next_poll: Instant,
    next_committee_attempt: Instant,
    /// Epoch of the last poll for a finality update.
    finality_epoch: u64,
}

impl Client {
    pub(super) fn new(
        spec: &'static BeaconSpec,
        checkpoint: B256,
        digest: ForkDigest,
        network: NetworkHandle,
        gossip: mpsc::Receiver<Gossip>,
        status: watch::Sender<StatusData>,
        trusted: mpsc::Sender<TrustedL1Block>,
    ) -> Self {
        let now = Instant::now();
        Self {
            spec,
            checkpoint,
            digest,
            network,
            gossip,
            status,
            trusted,
            store: None,
            refused: HashSet::new(),
            next_poll: now,
            next_committee_attempt: now,
            finality_epoch: 0,
        }
    }

    /// Runs until `cancel` fires, the network stops or the consumer of trusted blocks is
    /// gone.
    ///
    /// # Errors
    ///
    /// Returns [`BeaconError::CheckpointUnavailable`] if peers do not hold the bootstrap of
    /// the configured checkpoint, and [`BeaconError::Verification`] if the verification task
    /// panics.
    pub(super) async fn run(mut self, cancel: CancellationToken) -> Result<(), BeaconError> {
        let mut tick = interval(TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The request in flight. It is awaited beside gossip, not instead of it: an answer
        // can take many seconds, and a gossip message is only forwarded while it is fresh.
        let mut pending: Option<(Kind, Answer)> = None;
        loop {
            let (peer, kind, payloads, gossip) = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                gossip = self.gossip.recv() => {
                    let Some(Gossip { id, peer, topic, data }) = gossip else {
                        return Ok(());
                    };
                    let kind = match topic {
                        Topic::Finality => Kind::Finality,
                        Topic::Optimistic => Kind::Optimistic,
                    };
                    (peer, kind, vec![data], Some(id))
                }
                Some((kind, answer)) = answered(&mut pending) => {
                    pending = None;
                    match self.payloads(kind, answer) {
                        Some((peer, payloads)) => (peer, kind, payloads, None),
                        None => continue,
                    }
                }
                _ = tick.tick(), if pending.is_none() => {
                    if let Some((kind, request)) = self.due()? {
                        let network = self.network.clone();
                        let answer = async move { network.request(request).await };
                        pending = Some((kind, Box::pin(answer)));
                    }
                    continue;
                }
            };
            if self.store.is_none() && kind != Kind::Bootstrap {
                // Gossip before the bootstrap: nothing to verify it with.
                if let Some(id) = gossip {
                    self.network.report_gossip(id, peer, Verdict::Ignore);
                }
                continue;
            }
            let (spec, checkpoint, store) = (self.spec, self.checkpoint, self.store.clone());
            let now_slot = self.spec.now_slot();
            let verifying = tokio::task::spawn_blocking(move || {
                verify(spec, checkpoint, store, kind, &payloads, now_slot)
            });
            // Not cancelled: BLS over one update takes milliseconds.
            let result = verifying.await.map_err(BeaconError::Verification)?;
            if let Some(id) = gossip {
                // Forwarded to the mesh only if it verified and is news.
                // News on the finality topic is a newer finalized block; on the optimistic
                // topic, a newer head.
                let news = |accepted: &Accepted| match kind {
                    Kind::Finality => accepted.finalized.is_some(),
                    Kind::Optimistic | Kind::Bootstrap | Kind::Updates => accepted.head.is_some(),
                };
                let verdict = match &result {
                    Ok(verified) if news(&verified.accepted) => Verdict::Accept,
                    Err(err) if err.is_peer_fault() => Verdict::Reject,
                    Ok(_) | Err(_) => Verdict::Ignore,
                };
                self.network.report_gossip(id, peer, verdict);
            }
            if !self.on_verified(peer, kind, result, &cancel).await {
                return Ok(());
            }
        }
    }

    /// What to ask for now, if anything.
    fn due(&mut self) -> Result<Option<(Kind, Request)>, BeaconError> {
        let Some(store) = &self.store else {
            if self.refused.len() >= BOOTSTRAP_REFUSALS {
                return Err(BeaconError::CheckpointUnavailable {
                    checkpoint: self.checkpoint,
                    peers: self.refused.len(),
                });
            }
            return Ok(Some((Kind::Bootstrap, Request::Bootstrap(self.checkpoint))));
        };
        let now = Instant::now();
        let now_slot = self.spec.now_slot();
        let (period, clock_period) = (store.period(), now_slot / SLOTS_PER_PERIOD);
        // With the next committee known, updates signed in the next period verify and the
        // store rotates by itself when one of them finalizes a block there.
        let last_known = period.saturating_add(u64::from(store.knows_next_committee()));
        let stuck = clock_period > last_known;
        if (stuck || !store.knows_next_committee()) && now >= self.next_committee_attempt {
            let retry = if stuck {
                CATCH_UP_RETRY
            } else {
                COMMITTEE_RETRY
            };
            self.next_committee_attempt = now + retry;
            // The network asks for no more than a peer may send at once.
            let request = Request::UpdatesByRange {
                start_period: period,
                count: clock_period.saturating_sub(period).saturating_add(1),
            };
            return Ok(Some((Kind::Updates, request)));
        }
        if now < self.next_poll {
            return Ok(None);
        }
        self.next_poll = now + POLL_INTERVAL;
        let epoch = now_slot / SLOTS_PER_EPOCH;
        if epoch == self.finality_epoch {
            return Ok(Some((Kind::Optimistic, Request::OptimisticUpdate)));
        }
        self.finality_epoch = epoch;
        Ok(Some((Kind::Finality, Request::FinalityUpdate)))
    }

    /// The data of an answer that is of our fork digest, with who sent it; `None` if there
    /// is none. A peer that answered a bootstrap request without data is counted.
    fn payloads(
        &mut self,
        kind: Kind,
        answer: Result<Response, RequestError>,
    ) -> Option<(PeerId, Vec<Bytes>)> {
        let response = match answer {
            Ok(response) => response,
            Err(err) => {
                if let RequestError::Refused(peer, _) = err
                    && kind == Kind::Bootstrap
                {
                    self.refused.insert(peer);
                }
                if kind == Kind::Updates {
                    // Nobody was asked, or nobody answered: ask again soon, not after the
                    // long wait that follows an answer.
                    self.next_committee_attempt = Instant::now() + CATCH_UP_RETRY;
                }
                debug!(?kind, %err, "light-client request not answered");
                return None;
            }
        };
        let peer = response.peer;
        if response.chunks.is_empty() && kind == Kind::Bootstrap {
            self.refused.insert(peer);
        }
        let payloads: Vec<Bytes> = response
            .chunks
            .into_iter()
            .filter(|(digest, _)| *digest == self.digest)
            .map(|(_, ssz)| ssz)
            .collect();
        (!payloads.is_empty()).then_some((peer, payloads))
    }

    /// Takes over a verified answer and passes on the blocks it vouches for. Returns `false`
    /// when their consumer is gone.
    async fn on_verified(
        &mut self,
        peer: PeerId,
        kind: Kind,
        result: Result<Verified, VerifyError>,
        cancel: &CancellationToken,
    ) -> bool {
        let verified = match result {
            Ok(verified) => verified,
            Err(err) if err.is_peer_fault() => {
                debug!(%peer, ?kind, %err, "light-client data does not verify");
                self.network.report_invalid(peer);
                return true;
            }
            Err(err) => {
                debug!(%peer, ?kind, %err, "light-client data not usable yet");
                return true;
            }
        };
        let accepted = verified.accepted;
        if let Some(block) = verified.bootstrapped {
            info!(
                checkpoint = %self.checkpoint,
                l1_block = block.number,
                period = verified.store.period(),
                "light client bootstrapped from the checkpoint"
            );
        }
        if accepted.rotated || accepted.next_committee {
            info!(
                period = verified.store.period(),
                rotated = accepted.rotated,
                knows_next = verified.store.knows_next_committee(),
                "sync committees updated"
            );
        }
        if accepted.finalized.is_some() || accepted.head.is_some() {
            self.next_poll = Instant::now() + QUIET_AFTER_UPDATE;
        }
        self.status.send_replace(verified.store.status());
        self.store = Some(verified.store);
        let blocks = [verified.bootstrapped, accepted.finalized, accepted.head];
        for block in blocks.into_iter().flatten() {
            debug!(number = block.number, hash = %block.hash, finalized = block.finalized, "trusted L1 block");
            tokio::select! {
                biased;
                () = cancel.cancelled() => return false,
                sent = self.trusted.send(block) => {
                    if sent.is_err() {
                        return false;
                    }
                }
            }
        }
        true
    }
}

/// Verifies an answer against a copy of the store. Blocking: BLS and hashing.
fn verify(
    spec: &BeaconSpec,
    checkpoint: B256,
    store: Option<Store>,
    kind: Kind,
    payloads: &[Bytes],
    now_slot: u64,
) -> Result<Verified, VerifyError> {
    let first = payloads.first().map(|ssz| &ssz[..]).unwrap_or_default();
    let Some(mut store) = store else {
        let (store, block) = Store::bootstrap(spec, checkpoint, first)?;
        return Ok(Verified {
            store,
            accepted: Accepted::default(),
            bootstrapped: Some(block),
        });
    };
    let mut accepted = Accepted::default();
    match kind {
        Kind::Updates => {
            // One update per period, oldest first: each is verified by the committee the one
            // before it proved.
            for ssz in payloads {
                let step = store.apply_update(spec, ssz, now_slot)?;
                accepted.finalized = step.finalized.or(accepted.finalized);
                accepted.head = step.head.or(accepted.head);
                accepted.next_committee |= step.next_committee;
                accepted.rotated |= step.rotated;
            }
        }
        Kind::Finality => accepted = store.apply_finality_update(spec, first, now_slot)?,
        Kind::Optimistic | Kind::Bootstrap => {
            accepted = store.apply_optimistic_update(spec, first, now_slot)?;
        }
    }
    Ok(Verified {
        store,
        accepted,
        bootstrapped: None,
    })
}
