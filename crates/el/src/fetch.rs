//! The receipts fetcher: a queue of blocks that need receipts, served by the open sessions.
//!
//! Each queued block is asked of one peer at a time, newest block first, and each peer has at
//! most one request in flight. An answer is verified against the block's receipts root before
//! it leaves this module; a peer whose answer fails is reported to the peer set and the block
//! is asked of another peer. A block nobody can serve stays queued and is tried again, with a
//! growing wait.
//!
//! Does not open sessions or choose peers to dial (`peers`), and does not decode or verify
//! receipts itself (`wire`, `verify`).
//!
//! Peers do not announce how far back their receipts reach. When a peer answers "not held" for
//! a block well below its head, the fetcher remembers that height as the peer's floor and does
//! not ask it for older blocks again while the session lasts.
//!
//! The queue is bounded: when it is full the oldest block is dropped, counted. Per-peer state
//! exists only for open sessions.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::EMPTY_ROOT_HASH;
use alloy_primitives::{BlockHash, BlockNumber};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::{ReceiptsRequest, VerifiedReceipts};
use reth_network_peers::PeerId;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet, spawn_blocking};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::ElError;
use crate::metrics::{self, RequestOutcome};
use crate::peers::{Peers, Report, closed};
use crate::session::{RequestError, SessionContext, SessionHandle};
use crate::verify::{VerifyError, verify_receipts};

/// Most blocks waiting for receipts. About two hours of blocks; beyond it the oldest is dropped
/// and the pipeline asks again for stored blocks without receipts when it restarts.
const MAX_QUEUED: usize = 4096;
/// How often waiting blocks and rested peers are looked at again: the request spacing, so a
/// backlog moves at that pace. With nothing due this does no I/O.
const DISPATCH_TICK: Duration = REQUEST_SPACING;
/// Wait before a block is tried again after every open session failed to serve it. A new
/// block is often asked for before peers have executed it, so the first wait is short.
const FIRST_RETRY: Duration = Duration::from_secs(2);
/// Longest wait between two rounds for one block.
const MAX_RETRY: Duration = Duration::from_secs(60);
/// Pause between two requests to the same peer: this node only asks, so it asks slowly.
const REQUEST_SPACING: Duration = Duration::from_millis(200);
/// Timeouts in a row after which a peer is reported as unresponsive.
const MAX_TIMEOUTS: u32 = 3;
/// A "not held" answer sets the peer's floor only for a block at least this far below the
/// peer's head. Nearer the head it can also mean the peer has another block at that height.
const FLOOR_MARGIN: u64 = 64;
/// Most peers remembered as tried for one block before the round ends early.
const MAX_TRIED: usize = 16;

/// The receipts fetcher. [`Fetcher::run`] is its task.
#[derive(Debug)]
pub(crate) struct Fetcher {
    chain: &'static ChainSpec,
    ctx: Arc<SessionContext>,
    peers: Peers,
    requests: mpsc::Receiver<ReceiptsRequest>,
    verified: mpsc::Sender<VerifiedReceipts>,
    /// Blocks waiting for receipts, in block order. At most [`MAX_QUEUED`].
    queue: BTreeMap<Key, Pending>,
    /// What is known about each open session's peer; entries go when the session does.
    peer_states: HashMap<PeerId, PeerState>,
    /// Requests being answered and verified.
    in_flight: JoinSet<Result<Answer, JoinError>>,
}

/// A queued block: by number, so the queue is in block order, then hash.
type Key = (BlockNumber, BlockHash);

/// A block waiting for receipts.
#[derive(Debug)]
struct Pending {
    request: ReceiptsRequest,
    queued_at: Instant,
    in_flight: bool,
    /// Peers that failed to serve it in the current round.
    tried: Vec<PeerId>,
    /// Not asked for before this.
    not_before: Instant,
    /// The wait after the current round, if it fails too.
    retry_wait: Duration,
}

/// What the fetcher knows about the peer of an open session.
#[derive(Debug)]
struct PeerState {
    busy: bool,
    /// Not asked before this.
    next_request: Instant,
    /// The peer holds no receipts below this block number.
    floor: BlockNumber,
    /// Timeouts in a row.
    timeouts: u32,
    /// Whether the peer set has been told that this peer served a verified answer.
    reported_served: bool,
}

/// The end of one request to one peer.
#[derive(Debug)]
struct Answer {
    key: Key,
    peer: PeerId,
    outcome: Outcome,
}

#[derive(Debug)]
enum Outcome {
    Verified(Vec<OpReceiptEnvelope>),
    /// The peer does not hold the receipts.
    Empty,
    Invalid(VerifyError),
    Malformed(String),
    Timeout,
    /// The session ended before the answer.
    Closed,
}

impl Fetcher {
    /// Creates the fetcher. Sends nothing until [`Self::run`].
    pub(crate) fn new(
        chain: &'static ChainSpec,
        ctx: Arc<SessionContext>,
        peers: Peers,
        requests: mpsc::Receiver<ReceiptsRequest>,
        verified: mpsc::Sender<VerifiedReceipts>,
    ) -> Self {
        Self {
            chain,
            ctx,
            peers,
            requests,
            verified,
            queue: BTreeMap::new(),
            peer_states: HashMap::new(),
            in_flight: JoinSet::new(),
        }
    }

    /// Runs until `cancel` fires or either channel to the pipeline closes: queues requests,
    /// asks the open sessions, and sends on what verifies.
    ///
    /// # Errors
    ///
    /// Returns [`ElError::ChannelClosed`] if the peer set stopped while the node was running,
    /// and [`ElError::Task`] if verification panicked.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), ElError> {
        let mut retry_tick = interval(DISPATCH_TICK);
        retry_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(joined) = self.in_flight.join_next() => {
                    let answer = joined.and_then(|answer| answer).map_err(|source| {
                        ElError::Task { task: "receipts verification", source }
                    })?;
                    if !self.answered(answer, &cancel).await {
                        return Ok(());
                    }
                }
                request = self.requests.recv() => match request {
                    Some(request) => {
                        if !self.enqueue(request, &cancel).await {
                            return Ok(());
                        }
                    }
                    // The pipeline is gone: nothing left to fetch for.
                    None => return Ok(()),
                },
                alive = self.peers.changed() => {
                    if !alive {
                        return closed("sessions", &cancel);
                    }
                    self.sessions_changed();
                }
                _ = retry_tick.tick() => {}
            }
            self.dispatch();
        }
    }

    /// Queues a block. Returns `false` if the pipeline no longer takes receipts.
    async fn enqueue(&mut self, request: ReceiptsRequest, cancel: &CancellationToken) -> bool {
        // The newest block asked for is the tip this node advertises to peers; the peer set
        // opens no session before one is known.
        if !self.ctx.has_tip() {
            info!(
                number = request.block.number,
                hash = %request.block.hash,
                "first block to fetch receipts for; it is the tip advertised to execution peers"
            );
        }
        self.ctx.set_tip(request.block);
        if request.transaction_count == 0 {
            // Nothing to fetch. Not expected on an OP chain: every block has a deposit.
            if request.receipts_root != EMPTY_ROOT_HASH {
                warn!(block = ?request.block, "a block without transactions has a receipts root; skipped");
                return true;
            }
            return self.deliver(request, Vec::new(), cancel).await;
        }
        let key = (request.block.number, request.block.hash);
        if self.queue.contains_key(&key) {
            return true;
        }
        if self.queue.len() >= MAX_QUEUED {
            // Make room by dropping the oldest block that is not being asked for right now,
            // unless the new one is older still.
            let oldest = self
                .queue
                .iter()
                .find(|(_, pending)| !pending.in_flight)
                .map(|(key, _)| *key);
            metrics::queue_dropped();
            match oldest {
                Some(oldest) if oldest < key => {
                    self.queue.remove(&oldest);
                }
                Some(_) | None => return true,
            }
        }
        let now = Instant::now();
        self.queue.insert(
            key,
            Pending {
                request,
                queued_at: now,
                in_flight: false,
                tried: Vec::new(),
                not_before: now,
                retry_wait: FIRST_RETRY,
            },
        );
        metrics::queue_depth(self.queue.len());
        true
    }

    /// Gives every idle session the newest block it may be able to serve.
    fn dispatch(&mut self) {
        if self.queue.is_empty() {
            return;
        }
        let mut sessions = self.peers.sessions().to_vec();
        if sessions.is_empty() {
            return;
        }
        // The peer furthest ahead gets the newest block.
        sessions.sort_unstable_by_key(|session| std::cmp::Reverse(session.range().latest));
        let now = Instant::now();
        let canyon_time = self.chain.canyon_time;
        for session in sessions {
            let peer = session.status().peer_id;
            let state = self.peer_states.entry(peer).or_insert(PeerState {
                busy: false,
                next_request: now,
                floor: 0,
                timeouts: 0,
                reported_served: false,
            });
            if state.busy || state.next_request > now {
                continue;
            }
            // Below what the peer says it serves, or below where it answered "not held".
            let floor = state.floor.max(session.range().earliest);
            let next = self.queue.iter_mut().rev().find(|(key, pending)| {
                !pending.in_flight
                    && pending.not_before <= now
                    && key.0 >= floor
                    && !pending.tried.contains(&peer)
            });
            let Some((key, pending)) = next else {
                continue;
            };
            pending.in_flight = true;
            state.busy = true;
            let (key, request) = (*key, pending.request);
            self.in_flight
                .spawn(ask(session, key, request, canyon_time));
        }
    }

    /// Handles the end of one request. Returns `false` if the pipeline no longer takes
    /// receipts.
    async fn answered(&mut self, answer: Answer, cancel: &CancellationToken) -> bool {
        let Answer { key, peer, outcome } = answer;
        let mut timeouts = 0;
        if let Some(state) = self.peer_states.get_mut(&peer) {
            state.busy = false;
            state.next_request = Instant::now() + REQUEST_SPACING;
            state.timeouts = match outcome {
                Outcome::Timeout => state.timeouts.saturating_add(1),
                Outcome::Verified(_)
                | Outcome::Empty
                | Outcome::Invalid(_)
                | Outcome::Malformed(_)
                | Outcome::Closed => 0,
            };
            timeouts = state.timeouts;
        }
        match outcome {
            Outcome::Verified(receipts) => {
                metrics::request(RequestOutcome::Verified);
                if let Some(state) = self.peer_states.get_mut(&peer)
                    && !state.reported_served
                {
                    state.reported_served = true;
                    self.peers.report(Report::Served(peer));
                }
                let Some(pending) = self.queue.remove(&key) else {
                    return true;
                };
                metrics::queue_depth(self.queue.len());
                let waited = pending.queued_at.elapsed();
                metrics::delivered(receipts.len(), waited);
                debug!(
                    %peer,
                    number = key.0,
                    receipts = receipts.len(),
                    ?waited,
                    "receipts verified"
                );
                return self.deliver(pending.request, receipts, cancel).await;
            }
            Outcome::Empty => {
                metrics::request(RequestOutcome::Empty);
                debug!(%peer, number = key.0, "peer does not hold the receipts");
                self.note_not_held(peer, key.0);
                self.try_another(key, Some(peer));
            }
            Outcome::Invalid(err) => {
                metrics::request(RequestOutcome::Invalid);
                metrics::verification_failed(err.kind());
                warn!(%peer, number = key.0, hash = %key.1, %err, "receipts failed verification");
                self.peers.report(Report::BadData(peer));
                self.try_another(key, Some(peer));
            }
            Outcome::Malformed(reason) => {
                metrics::request(RequestOutcome::Malformed);
                warn!(%peer, number = key.0, hash = %key.1, reason, "receipts answer could not be decoded");
                self.peers.report(Report::BadData(peer));
                self.try_another(key, Some(peer));
            }
            Outcome::Timeout => {
                metrics::request(RequestOutcome::Timeout);
                debug!(%peer, number = key.0, timeouts, "receipts request timed out");
                if timeouts >= MAX_TIMEOUTS {
                    self.peers.report(Report::Unresponsive(peer));
                }
                self.try_another(key, Some(peer));
            }
            Outcome::Closed => {
                metrics::request(RequestOutcome::Closed);
                self.try_another(key, None);
            }
        }
        true
    }

    /// Sends verified receipts to the pipeline. Returns `false` if it no longer takes them or
    /// the node is shutting down.
    async fn deliver(
        &self,
        request: ReceiptsRequest,
        receipts: Vec<OpReceiptEnvelope>,
        cancel: &CancellationToken,
    ) -> bool {
        let verified = VerifiedReceipts {
            block: request.block,
            receipts,
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            sent = self.verified.send(verified) => sent.is_ok(),
        }
    }

    /// Remembers that `peer` holds no receipts for block `number`, if the block is far enough
    /// below the peer's head for that to say something about its history.
    fn note_not_held(&mut self, peer: PeerId, number: BlockNumber) {
        let sessions = self.peers.sessions();
        let Some(session) = sessions
            .iter()
            .find(|session| session.status().peer_id == peer)
        else {
            return;
        };
        let latest = session.range().latest;
        if number.saturating_add(FLOOR_MARGIN) > latest {
            return;
        }
        if let Some(state) = self.peer_states.get_mut(&peer) {
            let floor = number.saturating_add(1);
            if floor > state.floor {
                debug!(%peer, floor, "peer holds no receipts below this block");
                state.floor = floor;
            }
        }
    }

    /// Makes a block available again after `tried` (if any) failed to serve it. When no open
    /// session is left to try, the round ends: the block waits, then every peer may be asked
    /// again.
    fn try_another(&mut self, key: Key, tried: Option<PeerId>) {
        let Some(pending) = self.queue.get_mut(&key) else {
            return;
        };
        pending.in_flight = false;
        if let Some(peer) = tried {
            pending.tried.push(peer);
        }
        let sessions = self.peers.sessions();
        let another = pending.tried.len() < MAX_TRIED
            && sessions.iter().any(|session| {
                let peer = session.status().peer_id;
                !pending.tried.contains(&peer)
                    && self
                        .peer_states
                        .get(&peer)
                        .is_none_or(|state| key.0 >= state.floor)
            });
        if !another {
            pending.tried.clear();
            pending.not_before = Instant::now() + pending.retry_wait;
            pending.retry_wait = pending.retry_wait.saturating_mul(2).min(MAX_RETRY);
        }
    }

    /// A session opened or ended: forgets peers that are gone and, if a peer is new, lets
    /// waiting blocks be asked for at once, since it may hold them.
    fn sessions_changed(&mut self) {
        let sessions = self.peers.sessions();
        self.peer_states.retain(|peer, _| {
            sessions
                .iter()
                .any(|session| session.status().peer_id == *peer)
        });
        let new_peer = sessions
            .iter()
            .any(|session| !self.peer_states.contains_key(&session.status().peer_id));
        if new_peer {
            let now = Instant::now();
            for pending in self.queue.values_mut() {
                pending.not_before = now;
            }
        }
    }
}

/// Asks one peer for one block's receipts and verifies the answer on a blocking thread.
async fn ask(
    session: SessionHandle,
    key: Key,
    request: ReceiptsRequest,
    canyon_time: u64,
) -> Result<Answer, JoinError> {
    let peer = session.status().peer_id;
    let outcome = match session.receipts(request.block.hash).await {
        Ok(receipts) if receipts.is_empty() => Outcome::Empty,
        Ok(receipts) => {
            let verified = spawn_blocking(move || {
                verify_receipts(&request, &receipts, canyon_time).map(|()| receipts)
            })
            .await?;
            match verified {
                Ok(receipts) => Outcome::Verified(receipts),
                Err(err) => Outcome::Invalid(err),
            }
        }
        Err(RequestError::Timeout) => Outcome::Timeout,
        Err(RequestError::SessionClosed) => Outcome::Closed,
        Err(RequestError::Malformed(reason)) => Outcome::Malformed(reason),
    };
    Ok(Answer { key, peer, outcome })
}
