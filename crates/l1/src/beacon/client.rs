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
//! 4. when gossip has brought nothing new for a while: ask for the optimistic update every
//!    slot, and once an epoch for the finality update.
//!
//! Each answer and gossip message is verified off the runtime (`verify`) against a copy of
//! the store, which replaces the store when it verifies. A peer whose data fails
//! verification is reported to the network, which drops it. Data is accepted under any fork
//! digest whose containers this build reads.
//!
//! Does not touch the swarm, pick peers or frame messages: see `network`.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use alloy_primitives::{B256, Bytes};
use libp2p::PeerId;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::BeaconError;
use super::network::{Gossip, NetworkHandle, Request, RequestError, Response, Topic, Verdict};
use super::spec::{BeaconSpec, SLOTS_PER_EPOCH, SLOTS_PER_PERIOD};
use super::verify::{Accepted, Kind, Store, VerifyError, verify};
use crate::TrustedL1Block;

/// How often the loop looks for something to ask.
const TICK: Duration = Duration::from_secs(1);
/// Time between two polls for the newest update: a slot.
const POLL_INTERVAL: Duration = Duration::from_secs(12);
/// How long polling stays quiet after gossip brought news: gossip delivers an update every
/// slot, and polling only fills in when it stops.
const QUIET_AFTER_GOSSIP: Duration = Duration::from_secs(30);
/// Time between two attempts to learn the next sync committee, which peers can only prove
/// once a block of the period is finalized.
const COMMITTEE_RETRY: Duration = Duration::from_secs(600);
/// The same while the store cannot follow without it: the clock is past the last period
/// whose committee is known.
const CATCH_UP_RETRY: Duration = Duration::from_secs(20);
/// Peers that must say they do not hold the checkpoint's bootstrap before the checkpoint is
/// given up as too old.
const BOOTSTRAP_REFUSALS: usize = 12;
/// The response code of a peer that does not hold what was asked: `ResourceUnavailable`
/// ([response codes]). Only it says the checkpoint is too old; other codes and empty answers
/// say nothing about it.
///
/// [response codes]: https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md#responding-side
const RESOURCE_UNAVAILABLE: u8 = 3;
/// How long the verified head may stay where it is before it is logged that it stopped
/// advancing, and the shortest time between two such warnings. Measured from when the head
/// last moved, not from the wall clock: the checkpoint's slot is old by the time it is
/// bootstrapped.
const HEAD_STALL: Duration = Duration::from_secs(300);

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

/// The light client's state machine.
#[derive(Debug)]
pub(super) struct Client {
    spec: &'static BeaconSpec,
    checkpoint: B256,
    network: NetworkHandle,
    gossip: mpsc::Receiver<Gossip>,
    trusted: mpsc::Sender<TrustedL1Block>,
    store: Option<Store>,
    /// Peers that said they do not hold the checkpoint's bootstrap.
    refused: HashSet<PeerId>,
    next_poll: Instant,
    next_committee_attempt: Instant,
    /// Epoch of the last poll for a finality update.
    finality_epoch: u64,
    /// The verified head's slot and when it was first seen there, or when it was last logged
    /// that it stopped advancing.
    head_since: Option<(u64, Instant)>,
    /// The newest finalized block and the newest head verified and not yet handed to the
    /// watcher, whose channel was full.
    unsent: [Option<TrustedL1Block>; 2],
}

impl Client {
    pub(super) fn new(
        spec: &'static BeaconSpec,
        checkpoint: B256,
        network: NetworkHandle,
        gossip: mpsc::Receiver<Gossip>,
        trusted: mpsc::Sender<TrustedL1Block>,
    ) -> Self {
        let now = Instant::now();
        Self {
            spec,
            checkpoint,
            network,
            gossip,
            trusted,
            store: None,
            refused: HashSet::new(),
            next_poll: now,
            next_committee_attempt: now,
            finality_epoch: 0,
            head_since: None,
            unsent: [None, None],
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
                    self.warn_if_behind();
                    if !self.flush() {
                        return Ok(());
                    }
                    if let Some((kind, request)) = self.due()? {
                        let network = self.network.clone();
                        let answer = async move { network.request(request).await };
                        pending = Some((kind, Box::pin(answer)));
                    }
                    continue;
                }
            };
            let (spec, checkpoint, store) = (self.spec, self.checkpoint, self.store.clone());
            let now_slot = self.spec.now_slot();
            let verifying = tokio::task::spawn_blocking(move || {
                verify(spec, checkpoint, store, kind, &payloads, now_slot)
            });
            // Not cancelled: BLS over one update takes milliseconds.
            let result = verifying.await.map_err(BeaconError::Verification)?;
            if let Some(id) = gossip {
                // Forwarded to the mesh only if it verified and is news: on the finality
                // topic a newer finalized block, on the optimistic topic a newer head; and
                // only once the sync messages of its slot had time to spread.
                let spec = self.spec;
                let news = |accepted: &Accepted| {
                    if !spec.is_due(accepted.signature_slot) {
                        return false;
                    }
                    if kind == Kind::Finality {
                        accepted.finalized.is_some()
                    } else {
                        accepted.head.is_some()
                    }
                };
                let verdict = match &result {
                    Ok((_, accepted)) if news(accepted) => {
                        self.next_poll = Instant::now() + QUIET_AFTER_GOSSIP;
                        Verdict::Accept
                    }
                    Err(err) if err.is_peer_fault() => Verdict::Reject,
                    Ok(_) | Err(_) => Verdict::Ignore,
                };
                self.network.report_gossip(id, peer, verdict);
            }
            if !self.on_verified(peer, kind, result) {
                return Ok(());
            }
        }
    }

    /// Logs, every [`HEAD_STALL`] while it lasts, that the verified head has not moved for
    /// [`HEAD_STALL`]: no peer serves updates, finality has stalled for more than a period, or
    /// the build is behind a fork.
    fn warn_if_behind(&mut self) {
        let Some(store) = &self.store else {
            return;
        };
        let head = store.head_slot();
        if let Some((slot, since)) = self.head_since
            && slot == head
            && since.elapsed() < HEAD_STALL
        {
            return;
        }
        // The head did not move: the stall has lasted [`HEAD_STALL`] since it was first seen
        // there or last logged.
        let stalled = self.head_since.is_some_and(|(slot, _)| slot == head);
        self.head_since = Some((head, Instant::now()));
        if !stalled {
            return;
        }
        let now = self.spec.now_slot();
        warn!(
            head_slot = head,
            wall_clock_slot = now,
            behind_slots = now.saturating_sub(head),
            "the light client's head has stopped advancing"
        );
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
        let stuck = clock_period > store.last_known_period();
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
            return Ok(Some((Kind::Update, request)));
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

    /// The data of an answer under a fork digest this build reads, with who sent it; `None`
    /// if there is none. A peer that answered a bootstrap request without data is counted.
    fn payloads(
        &mut self,
        kind: Kind,
        answer: Result<Response, RequestError>,
    ) -> Option<(PeerId, Vec<Bytes>)> {
        let response = match answer {
            Ok(response) => response,
            Err(err) => {
                if let RequestError::Refused(peer, RESOURCE_UNAVAILABLE) = err
                    && kind == Kind::Bootstrap
                {
                    self.refused.insert(peer);
                }
                if kind == Kind::Update {
                    // Nobody was asked, or nobody answered: ask again soon, not after the
                    // long wait that follows an answer.
                    self.next_committee_attempt = Instant::now() + CATCH_UP_RETRY;
                }
                debug!(?kind, %err, "light-client request not answered");
                return None;
            }
        };
        let peer = response.peer;
        let payloads: Vec<Bytes> = response
            .chunks
            .into_iter()
            .filter(|(digest, _)| self.spec.knows_digest(*digest))
            .map(|(_, ssz)| ssz)
            .collect();
        (!payloads.is_empty()).then_some((peer, payloads))
    }

    /// Takes over a verified answer and hands the blocks it vouches for to the watcher.
    /// Returns `false` when the watcher is gone.
    fn on_verified(
        &mut self,
        peer: PeerId,
        kind: Kind,
        result: Result<(Store, Accepted), VerifyError>,
    ) -> bool {
        let (store, accepted) = match result {
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
        let before = self
            .store
            .as_ref()
            .map(|old| (old.period(), old.knows_next_committee()));
        let after = (store.period(), store.knows_next_committee());
        if kind == Kind::Bootstrap {
            info!(
                checkpoint = %self.checkpoint,
                l1_block = accepted.finalized.map(|block| block.number),
                period = after.0,
                "light client bootstrapped from the checkpoint"
            );
        } else if before != Some(after) {
            info!(
                period = after.0,
                knows_next = after.1,
                "sync committees updated"
            );
        }
        self.store = Some(store);
        for block in [accepted.finalized, accepted.head].into_iter().flatten() {
            debug!(number = block.number, hash = %block.hash, finalized = block.finalized, "trusted L1 block");
            // A newer block of the same kind replaces one still waiting: only the newest
            // matters to the watcher.
            let slot = usize::from(!block.finalized);
            if let Some(waiting) = self.unsent.get_mut(slot) {
                *waiting = Some(block);
            }
        }
        self.flush()
    }

    /// Hands the blocks waiting to the watcher, the finalized one first, as far as its
    /// channel has room; the rest wait for the next call. Never waits, so a watcher busy with
    /// a long walk does not stop the light client. Returns `false` when the watcher is gone.
    fn flush(&mut self) -> bool {
        for waiting in &mut self.unsent {
            let Some(block) = *waiting else { continue };
            match self.trusted.try_send(block) {
                Ok(()) => *waiting = None,
                Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
            }
        }
        true
    }
}
