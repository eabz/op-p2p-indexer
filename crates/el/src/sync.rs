//! Range sync: fetches a range of blocks from execution peers and verifies it, so a node
//! without history can get it from one that has it.
//!
//! ```text
//! anchor ─▶ [walk]  headers downwards, each the parent the one above names ─▶ checkpoints
//!           [fetch] per segment between two checkpoints: headers, bodies, receipts
//!                   ─▶ verify (hash chain, transactions root, receipts root) ─▶ batches, ascending
//! ```
//!
//! - **Walk.** The only trusted input is the anchor: the hash of the last block of the range.
//!   Headers are fetched in pages going down from it and each must be the block its child
//!   names as parent. Every [`SEGMENT_BLOCKS`]-th hash is kept as a checkpoint, and reported
//!   so the binary can save it: a restart continues from the lowest one.
//! - **Fetch.** Once the checkpoints reach the first block, the segments between them are
//!   fetched in ascending order, a few at a time on different sessions, and handed on strictly
//!   in order. A segment's headers are verified by the hash chain down from its checkpoint;
//!   bodies and receipts against the roots in those headers.
//!
//! # The bytes are never re-encoded
//!
//! A block hash is the keccak of the header bytes received. The transactions root is computed
//! over each transaction's bytes as they sit in the body received, the ommers hash over the
//! ommers bytes. Header and body leave this module as those same bytes ([`EncodedBlock`]),
//! next to the typed block decoded from them. Receipts are the exception: eth/69 sends them
//! without their bloom, so they are typed, the bloom is rebuilt from the logs, and the list is
//! accepted only if it hashes to the header's receipts root under the rule of the block's era
//! (`verify`); the stored form is encoded once from that verified list.
//!
//! Does not open sessions or choose peers to dial (`peers`), and does not store anything: the
//! pipeline stores the batches and the binary saves the progress. It uses the sessions the
//! receipts fetcher uses, with at most one request in flight on each, so a request for a new
//! block's receipts is never queued behind it: the session routes answers by request id.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use alloy_consensus::proofs::ordered_trie_root_with_encoder;
use alloy_consensus::{BlockBody, EMPTY_ROOT_HASH, Header};
use alloy_primitives::{B256, BlockNumber, Bytes, keccak256};
use alloy_rlp::Decodable;
use op_alloy_consensus::{OpBlock, OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_primitives::{
    BlockRef, EncodedBlock, ReceiptsRequest, SyncRange, SyncState, SyncedBlock, decode_transaction,
    encode_receipts,
};
use reth_network_peers::PeerId;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet, spawn_blocking};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::ElError;
use crate::metrics::{self, SyncOutcome};
use crate::peers::{Peers, Report, closed};
use crate::session::{RequestError, SessionHandle};
use crate::verify::verify_receipts;

/// Blocks between two checkpoints: what one session fetches as a unit and the size of a batch
/// handed to the pipeline. A segment is held in memory until it is handed on.
const SEGMENT_BLOCKS: u64 = 256;
/// Headers asked for in one request of the walk. Peers answer with at most 1024.
const HEADERS_PER_REQUEST: u64 = 1024;
/// Segments fetched or waiting to be handed on at once. Bounds memory, and how far the fetch
/// runs ahead of a pipeline that stores slowly.
const MAX_SEGMENTS_AHEAD: usize = 8;
/// Pause between two requests to the same peer.
const REQUEST_SPACING: Duration = Duration::from_millis(50);
/// How often rested peers and waiting segments are looked at again. Does no I/O.
const DISPATCH_TICK: Duration = Duration::from_secs(1);
/// How often progress is logged.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
/// How long a peer is left alone after it did not hold what was asked.
const NOT_HELD_REST: Duration = Duration::from_secs(60);
/// How long a peer is left alone after a timeout.
const TIMEOUT_REST: Duration = Duration::from_secs(10);
/// Timeouts in a row after which a peer is reported as unresponsive.
const MAX_TIMEOUTS: u32 = 3;

/// A range sync to run: what to fetch, where it stopped, and where its output goes.
#[derive(Debug)]
pub struct RangeSync {
    /// The range and its trusted anchor.
    pub range: SyncRange,
    /// What an earlier run saved for this range; the default for a new one.
    pub state: SyncState,
    /// Receives the verified blocks in ascending order, in batches of consecutive blocks. The
    /// sync waits when it is full and stops when it closes.
    pub blocks: mpsc::Sender<Vec<SyncedBlock>>,
    /// Receives blocks whose hash is verified, to be saved and given back in
    /// [`SyncState::checkpoints`] on the next start. The sync waits when it is full and stops
    /// when it closes.
    pub checkpoints: mpsc::Sender<Vec<BlockRef>>,
}

/// The range syncer. [`Syncer::run`] is its task.
#[derive(Debug)]
pub(crate) struct Syncer {
    canyon_time: u64,
    peers: Peers,
    /// First block still to hand on when this run started.
    first: BlockNumber,
    anchor: BlockRef,
    blocks: mpsc::Sender<Vec<SyncedBlock>>,
    saved: mpsc::Sender<Vec<BlockRef>>,
    /// Verified hashes at or above [`Self::next_emit`], the anchor among them.
    checkpoints: BTreeMap<BlockNumber, B256>,
    /// Whether a page of the walk is being fetched.
    walking: bool,
    /// First block not yet assigned to a segment.
    next_assign: BlockNumber,
    /// First block not yet handed on.
    next_emit: BlockNumber,
    /// Segments that failed and wait for another peer, by first block.
    waiting: BTreeMap<BlockNumber, Segment>,
    /// Verified segments waiting for the ones below them, by first block.
    ready: BTreeMap<BlockNumber, Vec<SyncedBlock>>,
    /// Segments assigned and not yet handed on. At most [`MAX_SEGMENTS_AHEAD`].
    outstanding: usize,
    /// What is known about each open session's peer; entries go when the session does.
    peer_states: HashMap<PeerId, PeerState>,
    jobs: JoinSet<Result<Done, JoinError>>,
}

/// Consecutive blocks ending at a checkpoint.
#[derive(Debug, Clone, Copy)]
struct Segment {
    first: BlockNumber,
    top: BlockRef,
}

#[derive(Debug)]
struct PeerState {
    busy: bool,
    /// Not asked before this.
    next_request: Instant,
    /// Timeouts in a row.
    timeouts: u32,
}

/// Something for one session to fetch.
#[derive(Debug, Clone, Copy)]
enum Job {
    /// A page of the walk, down from this block.
    Walk(BlockRef),
    Segment(Segment),
}

/// The end of one job on one session.
#[derive(Debug)]
struct Done {
    peer: PeerId,
    result: JobResult,
}

#[derive(Debug)]
enum JobResult {
    /// A page of the walk from this block, with the checkpoints it verified.
    Walk(BlockRef, Result<Vec<BlockRef>, Failure>),
    Segment(Segment, Result<Vec<SyncedBlock>, Failure>),
}

/// Why a job did not produce verified data.
#[derive(Debug, thiserror::Error)]
enum Failure {
    /// The peer answered with nothing: it does not hold the blocks.
    #[error("not held")]
    NotHeld,
    /// The answer is not what the trusted hashes commit to: the peer's fault.
    #[error("failed verification: {0}")]
    Invalid(String),
    /// The answer could not be decoded as the message asked for: the peer's fault.
    #[error("malformed answer: {0}")]
    Malformed(String),
    /// The bytes are the right ones (they hash to what the chain commits to) but this build
    /// cannot read them: not the peer's fault.
    #[error("verified data this build cannot read: {0}")]
    Unsupported(String),
    #[error("timeout")]
    Timeout,
    /// The session ended before the answer.
    #[error("session closed")]
    Closed,
}

impl From<RequestError> for Failure {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::Timeout => Self::Timeout,
            RequestError::SessionClosed => Self::Closed,
            RequestError::Malformed(reason) => Self::Malformed(reason),
        }
    }
}

impl Failure {
    const fn outcome(&self) -> SyncOutcome {
        match self {
            Self::NotHeld => SyncOutcome::NotHeld,
            Self::Invalid(_) => SyncOutcome::Invalid,
            Self::Malformed(_) => SyncOutcome::Malformed,
            Self::Unsupported(_) => SyncOutcome::Unsupported,
            Self::Timeout => SyncOutcome::Timeout,
            Self::Closed => SyncOutcome::Closed,
        }
    }
}

/// A header whose bytes hash to what its child (or the anchor) names.
#[derive(Debug)]
struct VerifiedHeader {
    hash: B256,
    raw: Bytes,
    header: Header,
}

impl Syncer {
    /// Creates the syncer. Sends nothing until [`Self::run`].
    pub(crate) fn new(canyon_time: u64, peers: Peers, sync: RangeSync) -> Self {
        let RangeSync {
            range,
            state,
            blocks,
            checkpoints: saved,
        } = sync;
        let first = state.stored_to.map_or(range.from, |stored| {
            stored.saturating_add(1).max(range.from)
        });
        let mut checkpoints: BTreeMap<BlockNumber, B256> = state
            .checkpoints
            .into_iter()
            .filter(|checkpoint| (first..range.anchor.number).contains(&checkpoint.number))
            .map(|checkpoint| (checkpoint.number, checkpoint.hash))
            .collect();
        checkpoints.insert(range.anchor.number, range.anchor.hash);
        Self {
            canyon_time,
            peers,
            first,
            anchor: range.anchor,
            blocks,
            saved,
            checkpoints,
            walking: false,
            next_assign: first,
            next_emit: first,
            waiting: BTreeMap::new(),
            ready: BTreeMap::new(),
            outstanding: 0,
            peer_states: HashMap::new(),
            jobs: JoinSet::new(),
        }
    }

    /// Runs until `cancel` fires. When the range is complete, or nothing takes its output any
    /// more, it stops fetching and waits for `cancel`, so the rest of the network carries on.
    ///
    /// # Errors
    ///
    /// Returns [`ElError::ChannelClosed`] if the peer set stopped while the node was running,
    /// and [`ElError::Task`] if verification panicked.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), ElError> {
        if self.is_complete() {
            info!(
                anchor = self.anchor.number,
                "range sync has nothing left to fetch"
            );
            cancel.cancelled().await;
            return Ok(());
        }
        info!(
            first = self.first,
            anchor = self.anchor.number,
            checkpoints = self.checkpoints.len(),
            "range sync starting"
        );
        let mut tick = interval(DISPATCH_TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut progress = interval(PROGRESS_INTERVAL);
        progress.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(joined) = self.jobs.join_next() => {
                    let done = joined.and_then(|done| done).map_err(|source| {
                        ElError::Task { task: "range sync verification", source }
                    })?;
                    if !self.finished(done, &cancel).await {
                        break;
                    }
                }
                alive = self.peers.changed() => {
                    if !alive {
                        return closed("sessions", &cancel);
                    }
                    let sessions = self.peers.sessions();
                    self.peer_states.retain(|peer, _| {
                        sessions.iter().any(|session| session.status().peer_id == *peer)
                    });
                }
                _ = tick.tick() => {}
                _ = progress.tick() => self.log_progress(),
            }
            if self.is_complete() {
                info!(anchor = self.anchor.number, "range sync complete");
                break;
            }
            self.dispatch();
        }
        // In-flight jobs have nowhere to deliver.
        self.jobs.shutdown().await;
        cancel.cancelled().await;
        Ok(())
    }

    const fn is_complete(&self) -> bool {
        self.next_emit > self.anchor.number
    }

    /// The lowest verified hash, where the walk continues. `None` once the checkpoints reach
    /// far enough down for the first segment.
    fn walk_from(&self) -> Option<BlockRef> {
        let (number, hash) = self.checkpoints.first_key_value()?;
        let below = number.saturating_sub(self.first);
        (below >= SEGMENT_BLOCKS).then_some(BlockRef {
            number: *number,
            hash: *hash,
        })
    }

    /// Gives every idle session something to fetch: the next page of the walk while it lasts,
    /// then segments, failed ones first.
    fn dispatch(&mut self) {
        let sessions = self.peers.sessions();
        let now = Instant::now();
        for session in sessions.iter() {
            let peer = session.status().peer_id;
            let state = self.peer_states.entry(peer).or_insert(PeerState {
                busy: false,
                next_request: now,
                timeouts: 0,
            });
            if state.busy || state.next_request > now {
                continue;
            }
            let Some(job) = self.next_job(session) else {
                continue;
            };
            if let Some(state) = self.peer_states.get_mut(&peer) {
                state.busy = true;
            }
            match job {
                Job::Walk(start) => {
                    let (session, first) = (session.clone(), self.first);
                    self.jobs.spawn(async move {
                        let result = walk(&session, start, first).await;
                        Ok(Done {
                            peer,
                            result: JobResult::Walk(start, result),
                        })
                    });
                }
                Job::Segment(segment) => {
                    self.jobs
                        .spawn(fetch_segment(session.clone(), segment, self.canyon_time));
                }
            }
        }
    }

    /// Picks what an idle session fetches next, and marks it as taken. `None` if there is
    /// nothing the peer holds, or nothing to do right now.
    fn next_job(&mut self, session: &SessionHandle) -> Option<Job> {
        {
            let range = session.range();
            let holds = |first: BlockNumber, last: BlockNumber| {
                range.earliest <= first && last <= range.latest
            };
            if let Some(start) = self.walk_from() {
                // One page at a time: each starts where the one before ended.
                let page_first = start
                    .number
                    .saturating_sub(HEADERS_PER_REQUEST - 1)
                    .max(self.first);
                if self.walking || !holds(page_first, start.number) {
                    return None;
                }
                self.walking = true;
                return Some(Job::Walk(start));
            }
            let waiting = self
                .waiting
                .values()
                .find(|segment| holds(segment.first, segment.top.number))
                .copied();
            if let Some(segment) = waiting {
                self.waiting.remove(&segment.first);
                return Some(Job::Segment(segment));
            }
            if self.outstanding >= MAX_SEGMENTS_AHEAD {
                return None;
            }
            let (number, hash) = self.checkpoints.range(self.next_assign..).next()?;
            let segment = Segment {
                first: self.next_assign,
                top: BlockRef {
                    number: *number,
                    hash: *hash,
                },
            };
            if !holds(segment.first, segment.top.number) {
                return None;
            }
            self.next_assign = segment.top.number.saturating_add(1);
            self.outstanding = self.outstanding.saturating_add(1);
            Some(Job::Segment(segment))
        }
    }

    /// Handles the end of one job. Returns `false` when the sync has to stop: the node is
    /// shutting down or nothing takes its output.
    async fn finished(&mut self, done: Done, cancel: &CancellationToken) -> bool {
        let Done { peer, result } = done;
        let failure = match result {
            JobResult::Walk(start, result) => {
                self.walking = false;
                match result {
                    Ok(checkpoints) => {
                        metrics::sync_request(SyncOutcome::Verified);
                        debug!(%peer, from = start.number, checkpoints = checkpoints.len(), "headers verified");
                        for checkpoint in &checkpoints {
                            self.checkpoints.insert(checkpoint.number, checkpoint.hash);
                        }
                        if self.walk_from().is_none() {
                            info!(
                                segments = self.checkpoints.len(),
                                "header chain verified down to the first block; fetching blocks"
                            );
                        }
                        self.rested(peer, None);
                        return send(&self.saved, checkpoints, "checkpoints", cancel).await;
                    }
                    Err(failure) => {
                        warn_failure(peer, start.number, start.number, &failure);
                        failure
                    }
                }
            }
            JobResult::Segment(segment, result) => match result {
                Ok(blocks) => {
                    metrics::sync_request(SyncOutcome::Verified);
                    self.ready.insert(segment.first, blocks);
                    self.rested(peer, None);
                    return self.hand_on(cancel).await;
                }
                Err(failure) => {
                    warn_failure(peer, segment.first, segment.top.number, &failure);
                    self.waiting.insert(segment.first, segment);
                    failure
                }
            },
        };
        metrics::sync_request(failure.outcome());
        self.rested(peer, Some(&failure));
        true
    }

    /// Frees `peer` for its next request and, after a failure, decides how long it is left
    /// alone and whether the peer set hears about it.
    fn rested(&mut self, peer: PeerId, failure: Option<&Failure>) {
        let Some(state) = self.peer_states.get_mut(&peer) else {
            return;
        };
        state.busy = false;
        let rest = match failure {
            None | Some(Failure::Closed) => REQUEST_SPACING,
            Some(Failure::NotHeld | Failure::Unsupported(_)) => NOT_HELD_REST,
            Some(Failure::Invalid(_) | Failure::Malformed(_)) => {
                self.peers.report(Report::BadData(peer));
                NOT_HELD_REST
            }
            Some(Failure::Timeout) => TIMEOUT_REST,
        };
        state.next_request = Instant::now() + rest;
        state.timeouts = if matches!(failure, Some(Failure::Timeout)) {
            state.timeouts.saturating_add(1)
        } else {
            0
        };
        if state.timeouts >= MAX_TIMEOUTS {
            state.timeouts = 0;
            self.peers.report(Report::Unresponsive(peer));
        }
    }

    /// Hands on every verified segment that is next in order. Returns `false` when the sync
    /// has to stop.
    async fn hand_on(&mut self, cancel: &CancellationToken) -> bool {
        while let Some(blocks) = self.ready.remove(&self.next_emit) {
            let Some(last) = blocks.last().map(|block| block.block.header.number) else {
                continue;
            };
            let count = blocks.len();
            if !send(&self.blocks, blocks, "blocks", cancel).await {
                return false;
            }
            metrics::sync_blocks(count, last);
            self.next_emit = last.saturating_add(1);
            self.outstanding = self.outstanding.saturating_sub(1);
            self.checkpoints = self.checkpoints.split_off(&self.next_emit);
        }
        true
    }

    fn log_progress(&self) {
        if let Some(lowest) = self.walk_from() {
            info!(
                verified_down_to = lowest.number,
                first = self.first,
                "range sync: verifying the header chain"
            );
        } else {
            info!(
                next = self.next_emit,
                anchor = self.anchor.number,
                in_progress = self.outstanding,
                sessions = self.peer_states.len(),
                "range sync: fetching blocks"
            );
        }
    }
}

/// Sends `value` on a channel of the sync's output, waiting for room. Returns `false` if the
/// node is shutting down or the receiver is gone.
async fn send<T>(
    channel: &mpsc::Sender<T>,
    value: T,
    name: &'static str,
    cancel: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        sent = channel.send(value) => {
            if sent.is_err() {
                warn!(channel = name, "nothing takes the range sync's output; it stops");
            }
            sent.is_ok()
        }
    }
}

fn warn_failure(peer: PeerId, first: BlockNumber, last: BlockNumber, failure: &Failure) {
    match failure {
        Failure::Invalid(_) | Failure::Malformed(_) => {
            warn!(%peer, first, last, %failure, "range sync: peer's answer rejected");
        }
        Failure::Unsupported(_) => {
            error!(%peer, first, last, %failure, "range sync cannot read verified blocks");
        }
        Failure::NotHeld | Failure::Timeout | Failure::Closed => {
            debug!(%peer, first, last, %failure, "range sync: request failed");
        }
    }
}

/// Fetches one page of headers going down from `start` and returns the checkpoints in it,
/// highest first: every [`SEGMENT_BLOCKS`]-th block below `start`, and the parent of the
/// page's last header, where the next page starts.
async fn walk(
    session: &SessionHandle,
    start: BlockRef,
    first: BlockNumber,
) -> Result<Vec<BlockRef>, Failure> {
    let limit = start
        .number
        .saturating_sub(first)
        .saturating_add(1)
        .min(HEADERS_PER_REQUEST);
    let raw = session.headers(start.hash, limit).await?;
    let headers = verify_chain(raw, start, limit)?;
    let mut checkpoints: Vec<BlockRef> = headers
        .iter()
        .step_by(usize::try_from(SEGMENT_BLOCKS).unwrap_or(usize::MAX))
        .skip(1)
        .map(|header| BlockRef {
            number: header.header.number,
            hash: header.hash,
        })
        .collect();
    if let Some(last) = headers.last()
        && last.header.number > first
    {
        checkpoints.push(BlockRef {
            number: last.header.number.saturating_sub(1),
            hash: last.header.parent_hash,
        });
    }
    Ok(checkpoints)
}

/// Checks that `raw` are consecutive headers going down from `top`: the first hashes to
/// `top.hash`, each next one to the parent hash of the one before. Returns them as received,
/// highest first.
fn verify_chain(
    raw: Vec<Bytes>,
    top: BlockRef,
    limit: u64,
) -> Result<Vec<VerifiedHeader>, Failure> {
    if raw.is_empty() {
        return Err(Failure::NotHeld);
    }
    if u64::try_from(raw.len()).unwrap_or(u64::MAX) > limit {
        return Err(Failure::Invalid(format!(
            "{} headers for a request of {limit}",
            raw.len()
        )));
    }
    let mut expected = top;
    let mut headers = Vec::with_capacity(raw.len());
    for bytes in raw {
        let hash = keccak256(&bytes);
        if hash != expected.hash {
            return Err(Failure::Invalid(format!(
                "header at block {} hashes to {hash}, not {}",
                expected.number, expected.hash
            )));
        }
        // From here the bytes are the chain's: what cannot be read is not the peer's fault.
        let mut rest: &[u8] = &bytes;
        let header = Header::decode(&mut rest)
            .ok()
            .filter(|_| rest.is_empty())
            .ok_or_else(|| Failure::Unsupported(format!("header {hash} does not decode")))?;
        if header.number != expected.number {
            return Err(Failure::Unsupported(format!(
                "block {hash} has number {}, not {}: the anchor's number is wrong",
                header.number, expected.number
            )));
        }
        expected = BlockRef {
            // Nothing lies below block 0; a header after it fails the hash check.
            number: header.number.saturating_sub(1),
            hash: header.parent_hash,
        };
        headers.push(VerifiedHeader {
            hash,
            raw: bytes,
            header,
        });
    }
    Ok(headers)
}

/// Fetches and verifies one segment on one session, one request at a time.
async fn fetch_segment(
    session: SessionHandle,
    segment: Segment,
    canyon_time: u64,
) -> Result<Done, JoinError> {
    let peer = session.status().peer_id;
    let result = match fetch_parts(&session, segment).await {
        Ok((headers, bodies, receipts)) => {
            spawn_blocking(move || assemble(headers, bodies, receipts, canyon_time)).await?
        }
        Err(failure) => Err(failure),
    };
    Ok(Done {
        peer,
        result: JobResult::Segment(segment, result),
    })
}

/// A segment's headers (verified, ascending), its bodies in the same order, and the receipts
/// of the blocks that have any, by block hash. Bodies and receipts are not verified yet.
type Parts = (
    Vec<VerifiedHeader>,
    Vec<Bytes>,
    HashMap<B256, Vec<OpReceiptEnvelope>>,
);

async fn fetch_parts(session: &SessionHandle, segment: Segment) -> Result<Parts, Failure> {
    let wanted = segment
        .top
        .number
        .saturating_sub(segment.first)
        .saturating_add(1);
    let mut headers: Vec<VerifiedHeader> = Vec::new();
    let mut next = segment.top;
    loop {
        let have = u64::try_from(headers.len()).unwrap_or(u64::MAX);
        if have >= wanted {
            break;
        }
        let limit = wanted - have;
        let raw = session.headers(next.hash, limit).await?;
        let page = verify_chain(raw, next, limit)?;
        if let Some(last) = page.last() {
            next = BlockRef {
                number: last.header.number.saturating_sub(1),
                hash: last.header.parent_hash,
            };
        }
        headers.extend(page);
        sleep(REQUEST_SPACING).await;
    }
    headers.reverse();

    let hashes: Vec<B256> = headers.iter().map(|header| header.hash).collect();
    let mut bodies: Vec<Bytes> = Vec::with_capacity(hashes.len());
    while let Some(missing) = hashes.get(bodies.len()..).filter(|rest| !rest.is_empty()) {
        let answer = session.bodies(missing.to_vec()).await?;
        if answer.is_empty() {
            return Err(Failure::NotHeld);
        }
        if answer.len() > missing.len() {
            return Err(Failure::Invalid(format!(
                "{} bodies for a request of {}",
                answer.len(),
                missing.len()
            )));
        }
        bodies.extend(answer);
        sleep(REQUEST_SPACING).await;
    }

    // A block without transactions has no receipts to ask for, and its entry in an answer
    // could not be told from "not held".
    let with_receipts: Vec<B256> = headers
        .iter()
        .filter(|header| header.header.receipts_root != EMPTY_ROOT_HASH)
        .map(|header| header.hash)
        .collect();
    let mut receipts: HashMap<B256, Vec<OpReceiptEnvelope>> = HashMap::new();
    while let Some(missing) = with_receipts
        .get(receipts.len()..)
        .filter(|rest| !rest.is_empty())
    {
        let answer = session.receipts_of(missing.to_vec()).await?;
        if answer.is_empty() {
            return Err(Failure::NotHeld);
        }
        if answer.len() > missing.len() {
            return Err(Failure::Invalid(format!(
                "receipts of {} blocks for a request of {}",
                answer.len(),
                missing.len()
            )));
        }
        receipts.extend(missing.iter().copied().zip(answer));
        sleep(REQUEST_SPACING).await;
    }
    Ok((headers, bodies, receipts))
}

/// Verifies every body and receipt list against its header and builds the blocks, ascending.
///
/// CPU work proportional to the segment: runs on a blocking thread.
fn assemble(
    headers: Vec<VerifiedHeader>,
    bodies: Vec<Bytes>,
    mut receipts: HashMap<B256, Vec<OpReceiptEnvelope>>,
    canyon_time: u64,
) -> Result<Vec<SyncedBlock>, Failure> {
    headers
        .into_iter()
        .zip(bodies)
        .map(|(header, body)| {
            let receipts = receipts.remove(&header.hash).unwrap_or_default();
            assemble_block(header, body, receipts, canyon_time)
        })
        .collect()
}

fn assemble_block(
    verified: VerifiedHeader,
    body: Bytes,
    receipts: Vec<OpReceiptEnvelope>,
    canyon_time: u64,
) -> Result<SyncedBlock, Failure> {
    let VerifiedHeader { hash, raw, header } = verified;
    let number = header.number;
    let invalid = |what: &str| Failure::Invalid(format!("block {number} ({hash}): {what}"));

    let parts = split_body(&body).ok_or_else(|| invalid("body is not a block body"))?;
    let root = ordered_trie_root_with_encoder(&parts.transactions, |leaf, out| {
        out.extend_from_slice(leaf);
    });
    if root != header.transactions_root {
        return Err(invalid("transactions do not hash to the transactions root"));
    }
    if keccak256(parts.ommers) != header.ommers_hash {
        return Err(invalid("ommers do not hash to the ommers hash"));
    }
    // An OP Stack block has no withdrawals: the list is absent before Canyon and empty after.
    // (From Isthmus the header's withdrawals root is a storage root, not the list's.)
    if parts.withdrawals != header.withdrawals_root.is_some() {
        return Err(invalid("withdrawals do not match the header"));
    }
    let request = ReceiptsRequest {
        block: BlockRef { number, hash },
        receipts_root: header.receipts_root,
        timestamp_secs: header.timestamp,
        transaction_count: parts.transactions.len(),
    };
    verify_receipts(&request, &receipts, canyon_time).map_err(|err| invalid(&err.to_string()))?;

    // Verified; the typed block is read from the same bytes.
    let unsupported = |what: &str| Failure::Unsupported(format!("block {number} ({hash}): {what}"));
    if !parts.ommers_empty {
        return Err(unsupported("it has ommers"));
    }
    let transactions = parts
        .transactions
        .iter()
        .map(|leaf| decode_transaction(leaf))
        .collect::<Result<Vec<OpTxEnvelope>, _>>()
        .map_err(|err| unsupported(&format!("a transaction does not decode: {err}")))?;
    Ok(SyncedBlock {
        block: OpBlock {
            header,
            body: BlockBody {
                transactions,
                ommers: Vec::new(),
                withdrawals: parts.withdrawals.then(Default::default),
            },
        },
        encoded: EncodedBlock {
            hash,
            header: raw,
            body,
            receipts: Some(encode_receipts(&receipts)),
        },
        receipts,
    })
}

/// A block body cut into the byte ranges its header commits to.
struct BodyParts<'a> {
    /// Each transaction as the transactions trie holds it ([EIP-2718]): the RLP list of a
    /// legacy transaction, or the type byte and payload of a typed one.
    ///
    /// [EIP-2718]: https://eips.ethereum.org/EIPS/eip-2718
    transactions: Vec<&'a [u8]>,
    /// The RLP list of ommers, whose keccak is the header's ommers hash.
    ommers: &'a [u8],
    ommers_empty: bool,
    /// Whether the body has a withdrawals list. It must be empty.
    withdrawals: bool,
}

/// Cuts a body, `[transactions, ommers]` or `[transactions, ommers, withdrawals]`, without
/// decoding what is inside. `None` if it has another shape, or withdrawals.
fn split_body(body: &[u8]) -> Option<BodyParts<'_>> {
    let mut rest = body;
    let outer = item(&mut rest)?;
    if !outer.list || !rest.is_empty() {
        return None;
    }
    let mut fields = outer.payload;
    let list = item(&mut fields).filter(|list| list.list)?;
    let ommers = item(&mut fields).filter(|ommers| ommers.list)?;
    let withdrawals = if fields.is_empty() {
        false
    } else {
        let withdrawals = item(&mut fields)?;
        if !withdrawals.list || !withdrawals.payload.is_empty() || !fields.is_empty() {
            return None;
        }
        true
    };

    let mut transactions = Vec::new();
    let mut entries = list.payload;
    while !entries.is_empty() {
        let entry = item(&mut entries)?;
        // In a body a typed transaction is wrapped in an RLP string; the trie holds what is
        // inside. A legacy transaction is a list, held as it is.
        transactions.push(if entry.list {
            entry.whole
        } else {
            entry.payload
        });
    }
    Some(BodyParts {
        transactions,
        ommers: ommers.whole,
        ommers_empty: ommers.payload.is_empty(),
        withdrawals,
    })
}

/// One RLP item at the start of a buffer.
struct Item<'a> {
    list: bool,
    /// The item with its RLP header.
    whole: &'a [u8],
    payload: &'a [u8],
}

/// Takes the next RLP item off `buf`. `None` if `buf` does not start with a complete item.
fn item<'a>(buf: &mut &'a [u8]) -> Option<Item<'a>> {
    let start = *buf;
    let mut after_header = start;
    let header = alloy_rlp::Header::decode(&mut after_header).ok()?;
    let (payload, rest) = after_header.split_at_checked(header.payload_length)?;
    let (whole, _) = start.split_at_checked(start.len().checked_sub(rest.len())?)?;
    *buf = rest;
    Some(Item {
        list: header.list,
        whole,
        payload,
    })
}
