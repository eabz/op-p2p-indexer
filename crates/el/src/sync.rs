//! Range sync: fetches a range of blocks from execution peers and verifies it, so a node
//! without history can get it from one that has it.
//!
//! ```text
//! anchor ─▶ [walk]  headers downwards, each the parent the one above names ─▶ checkpoints
//!           [fetch] per segment between two checkpoints: headers, bodies, receipts
//!                   ─▶ verified against the hash chain and the headers' roots ─▶ batches, ascending
//! ```
//!
//! - **Walk** (`headers`). The only trusted input is the anchor: the hash of the last block
//!   of the range. Headers are fetched in pages going down from it and each must be the block
//!   its child names as parent. Every [`SEGMENT_BLOCKS`]-th hash is kept as a checkpoint and
//!   reported, so the binary can save it: a restart continues from the lowest one.
//! - **Fetch** (`segment`). Once the checkpoints reach the first block, the segments between
//!   them are fetched in ascending order, a few at a time on different sessions, and handed on
//!   strictly in order.
//! - `schedule` paces the peers. The syncer here decides what each one fetches.
//!
//! # The bytes are never re-encoded
//!
//! A block hash is the keccak of the header bytes received. The transactions root is computed
//! over each transaction's bytes as they sit in the body received, the ommers hash over the
//! ommers bytes. Header and body leave this module as those same buffers ([`EncodedBlock`]).
//! Receipts are the exception: eth/69 sends them without their bloom, so they are typed, the
//! bloom is rebuilt from the logs, and the list is accepted only if it hashes to the header's
//! receipts root under the rule of the block's era; the stored form is encoded once from that
//! verified list.
//!
//! Does not open sessions or choose peers to dial (`peers`), and does not store anything: the
//! pipeline stores the batches and the binary saves the checkpoints. It uses the sessions the
//! receipts fetcher uses, with one request at a time on each; the session routes answers by
//! request id, so a request for a new block's receipts is not queued behind it.
//!
//! The headers of the range are downloaded twice, once by the walk and once per segment: the
//! walk runs from the top and the fetch from the bottom, and holding the walk's headers until
//! the fetch reaches them would mean keeping the whole chain's.

mod headers;
mod schedule;
mod segment;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use alloy_primitives::{B256, BlockNumber};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::{BlockRef, EncodedBlock, SyncRange};
use reth_network_peers::PeerId;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};

use self::headers::HEADERS_PER_REQUEST;
use self::schedule::Schedule;
use crate::ElError;
use crate::metrics::{self, SyncOutcome};
use crate::peers::{Peers, Report, closed};
use crate::session::{RequestError, SessionHandle};

/// Blocks between two checkpoints: what one session fetches as a unit and the size of a batch
/// handed to the pipeline. A segment is held in memory until it is handed on.
const SEGMENT_BLOCKS: u64 = 256;
/// Segments fetched or waiting to be handed on at once. Bounds memory, and how far the fetch
/// runs ahead of a pipeline that stores slowly.
const MAX_SEGMENTS_AHEAD: usize = 8;
/// How often progress is logged, and how often the sessions are looked at again when nothing
/// else happens (a peer's announced range can change without a session opening or ending).
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
/// How often it is said that no connected peer serves the blocks needed.
const STARVED_INTERVAL: Duration = Duration::from_mins(1);
/// Peers that must say they do not hold the anchor before it is given up, and then only once
/// every peer that says it holds the anchor's height has: a block no peer serves (one a reorg
/// left behind, mostly) would otherwise keep the round open for ever.
const ANCHOR_REFUSALS: usize = 3;
/// "Not held" answers in a row for blocks before Bedrock after which an indexer is dropped:
/// it advertises them and does not serve them, and holds the indexer slot.
const MAX_INDEXER_MISSES: u32 = 3;

/// What a range sync fetches, known once its anchor is.
#[derive(Debug)]
pub struct SyncPlan {
    /// The first block to fetch and the trusted anchor at the top.
    pub range: SyncRange,
    /// Blocks whose hash an earlier run verified from the same anchor; empty for a new sync.
    /// Fetching resumes from them without walking the chain again.
    pub checkpoints: Vec<BlockRef>,
    /// The block the range must extend: the parent the first block has to name. `None` when
    /// the range starts the chain (an empty archive takes any first block).
    pub extends: Option<BlockRef>,
    /// Told how the round ended, once it has; dropped unanswered if the node stops first.
    pub ended: oneshot::Sender<RoundEnd>,
}

/// How a round of the range sync ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundEnd {
    /// Every block up to the anchor was handed on.
    Complete,
    /// At least three peers, and every peer that says it holds the anchor's height, said
    /// they do not hold the anchor and none served it: the round was given up, with nothing
    /// handed on (no block is verified before the anchor is).
    AnchorUnavailable,
    /// The chain down from the anchor does not reach [`SyncPlan::extends`]: the anchor is not
    /// a descendant of the block the range must extend. Given up before anything was handed
    /// on.
    NotLinked,
}

/// A range sync to run: where its plan comes from and where its output goes.
#[derive(Debug)]
pub struct RangeSync {
    /// Receives what to fetch: the anchor may be a block the node has yet to learn. Plans are
    /// run one after the other, each to its end; nothing is fetched before the first arrives,
    /// and the sync is over when the sender is dropped.
    pub plans: mpsc::Receiver<SyncPlan>,
    /// Receives the verified blocks in ascending order, in batches of consecutive blocks. The
    /// sync waits when it is full and stops when it closes.
    pub blocks: mpsc::Sender<Vec<EncodedBlock>>,
    /// Receives blocks whose hash is verified, to be saved and given back in
    /// [`SyncPlan::checkpoints`] on the next start. The sync waits when it is full and stops when
    /// it closes.
    pub verified: mpsc::Sender<Vec<BlockRef>>,
}

/// Runs the range sync `sync` until `cancel` fires: fetches each plan it is given, one after
/// the other. When no more plans come, or nothing takes its output any more, it stops
/// fetching and waits for `cancel`, so the rest of the network carries on.
///
/// # Errors
///
/// Returns [`ElError::Sync`] if blocks of the chain cannot be read by this build,
/// [`ElError::ChannelClosed`] if the peer set stopped while the node was running, and
/// [`ElError::Task`] if verification panicked.
pub(crate) async fn run(
    chain: &'static ChainSpec,
    peers: Peers,
    sync: RangeSync,
    cancel: CancellationToken,
) -> Result<(), ElError> {
    let RangeSync {
        mut plans,
        blocks,
        verified,
    } = sync;
    loop {
        let plan = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            plan = plans.recv() => plan,
        };
        // No more plans: nothing left to sync.
        let Some(mut plan) = plan else { break };
        let ended = std::mem::replace(&mut plan.ended, oneshot::channel().0);
        let syncer = Syncer::new(chain, peers.clone(), plan, blocks.clone(), verified.clone());
        let Some(end) = syncer.run(&cancel).await? else {
            break;
        };
        // The planner may have stopped waiting: nothing to tell.
        let _told = ended.send(end);
    }
    cancel.cancelled().await;
    Ok(())
}

/// The range syncer.
#[derive(Debug)]
struct Syncer {
    canyon_time: u64,
    /// Blocks below this are asked of op-p2p-indexers only: the chain's Bedrock block.
    indexers_only_below: BlockNumber,
    peers: Peers,
    schedule: Schedule,
    /// First block of the range.
    first: BlockNumber,
    anchor: BlockRef,
    blocks: mpsc::Sender<Vec<EncodedBlock>>,
    saved: mpsc::Sender<Vec<BlockRef>>,
    /// Verified hashes at or above [`Self::next_emit`], the anchor among them.
    checkpoints: BTreeMap<BlockNumber, B256>,
    /// Whether the checkpoints reach down to the first segment. Never unset: the walk is done
    /// once, however far the fetch has got since.
    walked: bool,
    /// Whether a page of the walk is being fetched.
    walking: bool,
    /// First block not yet assigned to a segment.
    next_assign: BlockNumber,
    /// First block not yet handed on.
    next_emit: BlockNumber,
    /// Segments that failed and wait for another peer, by first block.
    waiting: BTreeMap<BlockNumber, Segment>,
    /// Verified segments waiting for the ones below them, by first block.
    ready: BTreeMap<BlockNumber, (Segment, Vec<EncodedBlock>)>,
    /// Segments assigned and not yet handed on. At most [`MAX_SEGMENTS_AHEAD`].
    outstanding: usize,
    jobs: JoinSet<Done>,
    /// When it was last said that no peer serves what is needed.
    starved_warned: Option<Instant>,
    /// Whether a page of headers from the anchor down has verified: some peer serves it.
    anchor_served: bool,
    /// Peers that said they do not hold the anchor, while none has served it.
    anchor_refused: HashSet<PeerId>,
    /// "Not held" answers in a row for blocks before Bedrock, per indexer.
    indexer_misses: HashMap<PeerId, u32>,
    /// The block the first block must name as parent; `None` for any.
    extends: Option<BlockRef>,
    /// How the round ends before it is complete, once that is known: the chain down from the
    /// anchor turned out not to reach `extends`.
    given_up: Option<RoundEnd>,
}

/// Consecutive blocks ending at a checkpoint.
#[derive(Debug, Clone, Copy)]
struct Segment {
    first: BlockNumber,
    top: BlockRef,
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
    /// A page of the walk from this block, with the checkpoints it verified and, once it
    /// reaches the first block, that block's parent.
    Walk(BlockRef, Result<(Vec<BlockRef>, Option<BlockRef>), Failure>),
    Segment(Segment, Result<Vec<EncodedBlock>, Failure>),
}

/// Why a job did not produce verified data.
#[derive(Debug, thiserror::Error)]
enum Failure {
    /// The peer answered with nothing, or left out the block asked for: it does not hold it.
    #[error("not held")]
    NotHeld,
    /// The answer is present and is not what the trusted hashes commit to: the peer's fault.
    #[error("failed verification: {0}")]
    Invalid(String),
    /// The answer could not be decoded as the message asked for: the peer's fault.
    #[error("malformed answer: {0}")]
    Malformed(String),
    /// Receipts that cannot be decoded: malformed, or of a kind this build does not know. The
    /// peer may be honest, so it is not banned for it.
    #[error("receipts cannot be decoded: {0}")]
    Undecodable(String),
    /// The bytes are the right ones (they hash to what the chain commits to) but this build
    /// cannot read them. No peer can do better: the sync cannot continue.
    #[error("{0}")]
    Unsupported(String),
    #[error("timeout")]
    Timeout,
    /// The session ended before the answer.
    #[error("session closed")]
    Closed,
    /// Verification panicked.
    #[error("verification task failed")]
    Panicked(#[source] JoinError),
}

impl From<RequestError> for Failure {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::Timeout => Self::Timeout,
            RequestError::SessionClosed => Self::Closed,
            RequestError::Malformed(reason) => Self::Malformed(reason),
            excess @ RequestError::Excess { .. } => Self::Invalid(excess.to_string()),
        }
    }
}

impl Failure {
    const fn outcome(&self) -> SyncOutcome {
        match self {
            Self::NotHeld => SyncOutcome::NotHeld,
            Self::Invalid(_) => SyncOutcome::Invalid,
            Self::Malformed(_) | Self::Undecodable(_) => SyncOutcome::Malformed,
            Self::Unsupported(_) | Self::Panicked(_) => SyncOutcome::Unsupported,
            Self::Timeout => SyncOutcome::Timeout,
            Self::Closed => SyncOutcome::Closed,
        }
    }
}

impl Syncer {
    /// Creates the syncer for `plan`. Sends nothing until [`Self::run`].
    fn new(
        chain: &'static ChainSpec,
        peers: Peers,
        plan: SyncPlan,
        blocks: mpsc::Sender<Vec<EncodedBlock>>,
        saved: mpsc::Sender<Vec<BlockRef>>,
    ) -> Self {
        let SyncPlan {
            range: SyncRange {
                from: first,
                anchor,
            },
            checkpoints,
            extends,
            ended: _,
        } = plan;
        let mut checkpoints: BTreeMap<BlockNumber, B256> = checkpoints
            .into_iter()
            .filter(|checkpoint| (first..anchor.number).contains(&checkpoint.number))
            .map(|checkpoint| (checkpoint.number, checkpoint.hash))
            .collect();
        checkpoints.insert(anchor.number, anchor.hash);
        let mut syncer = Self {
            canyon_time: chain.canyon_time(),
            indexers_only_below: chain.bedrock_block,
            peers,
            schedule: Schedule::default(),
            first,
            anchor,
            blocks,
            saved,
            checkpoints,
            walked: false,
            walking: false,
            next_assign: first,
            next_emit: first,
            waiting: BTreeMap::new(),
            ready: BTreeMap::new(),
            outstanding: 0,
            jobs: JoinSet::new(),
            starved_warned: None,
            anchor_served: false,
            anchor_refused: HashSet::new(),
            indexer_misses: HashMap::new(),
            extends,
            given_up: None,
        };
        syncer.walked = syncer.walk_from().is_none();
        // Checkpoints below the anchor were verified from it: a peer served it. A range short
        // enough to need no walk has none, and learns it from its first verified segment.
        syncer.anchor_served = syncer.checkpoints.len() > 1;
        syncer
    }

    /// Fetches the plan's range. Returns how the round ended, or `None` if it stopped before:
    /// the node is shutting down or nothing takes its output.
    async fn run(mut self, cancel: &CancellationToken) -> Result<Option<RoundEnd>, ElError> {
        if self.is_complete() {
            info!(
                first = self.first,
                anchor = self.anchor.number,
                "range sync has nothing to fetch"
            );
            return Ok(Some(RoundEnd::Complete));
        }
        info!(
            first = self.first,
            anchor = self.anchor.number,
            checkpoints = self.checkpoints.len(),
            header_chain_verified = self.walked,
            "range sync starting"
        );
        let mut progress = interval(PROGRESS_INTERVAL);
        progress.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let complete = loop {
            // A resting peer becomes free without anything else happening.
            let wake = self.schedule.next_wake(Instant::now());
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(None),
                Some(joined) = self.jobs.join_next() => {
                    let done = joined.map_err(|source| {
                        ElError::Task { task: "range sync", source }
                    })?;
                    if !self.finished(done, cancel).await? {
                        break None;
                    }
                }
                alive = self.peers.changed() => {
                    if !alive {
                        return closed("sessions", cancel).map(|()| None);
                    }
                    let sessions = self.peers.sessions();
                    self.schedule.retain(&sessions);
                    self.indexer_misses
                        .retain(|peer, _| sessions.iter().any(|session| session.peer_id() == *peer));
                }
                () = async {
                    match wake {
                        Some(at) => sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {}
                _ = progress.tick() => self.log_progress(),
            }
            if self.is_complete() {
                info!(
                    first = self.first,
                    anchor = self.anchor.number,
                    "range sync complete"
                );
                break Some(RoundEnd::Complete);
            }
            if let Some(end) = self.given_up {
                break Some(end);
            }
            if self.anchor_unavailable() {
                warn!(
                    anchor = self.anchor.number,
                    hash = %self.anchor.hash,
                    peers = self.anchor_refused.len(),
                    "range sync gives up its anchor: no peer serves it"
                );
                break Some(RoundEnd::AnchorUnavailable);
            }
            self.dispatch();
        };
        // In-flight jobs have nowhere to deliver.
        self.jobs.shutdown().await;
        Ok(complete)
    }

    /// Whether the anchor is to be given up: [`ANCHOR_REFUSALS`] peers said they do not hold
    /// it, none served it, and every open session whose peer says it holds its height has been
    /// asked.
    fn anchor_unavailable(&self) -> bool {
        if self.anchor_served || self.anchor_refused.len() < ANCHOR_REFUSALS {
            return false;
        }
        let number = self.anchor.number;
        self.peers.sessions().iter().all(|session| {
            !self.serves(session, number, number)
                || self.anchor_refused.contains(&session.status().peer_id)
        })
    }

    /// Whether `session`'s peer may be asked for blocks `first` to `last`: it says it holds
    /// them, and blocks before Bedrock are asked of op-p2p-indexers only, which share them
    /// with each other; other peers are left alone for those.
    fn serves(&self, session: &SessionHandle, first: BlockNumber, last: BlockNumber) -> bool {
        let range = session.range();
        session.is_askable()
            && range.earliest <= first
            && last <= range.latest
            && (first >= self.indexers_only_below || session.status().indexer)
    }

    /// Records the header chain reaching the first block, whose parent is `below`: it must be
    /// the block the range extends. Returns `false` if it is not.
    fn linked(&mut self, below: BlockRef) -> bool {
        if self.extends.is_none_or(|extends| extends == below) {
            return true;
        }
        warn!(
            first = self.first,
            names = %below.hash,
            expected = ?self.extends,
            anchor = self.anchor.number,
            "range sync gives up its anchor: its chain does not reach the archive's last block"
        );
        self.given_up = Some(RoundEnd::NotLinked);
        false
    }

    const fn is_complete(&self) -> bool {
        self.next_emit > self.anchor.number
    }

    /// The lowest verified hash, where the walk continues. `None` once the walk is done: the
    /// checkpoints reach far enough down for the first segment.
    fn walk_from(&self) -> Option<BlockRef> {
        if self.walked {
            return None;
        }
        let (number, hash) = self.checkpoints.first_key_value()?;
        (number.saturating_sub(self.first) >= SEGMENT_BLOCKS).then_some(BlockRef {
            number: *number,
            hash: *hash,
        })
    }

    /// Gives every free session something to fetch.
    fn dispatch(&mut self) {
        let sessions = self.peers.sessions();
        let now = Instant::now();
        for session in sessions.iter() {
            let peer = session.status().peer_id;
            if !self.schedule.is_free(peer, now) {
                continue;
            }
            let Some(job) = self.next_job(session) else {
                continue;
            };
            self.schedule.started(peer);
            let (session, first, canyon_time) = (session.clone(), self.first, self.canyon_time);
            let job = async move {
                let result = match job {
                    Job::Walk(start) => {
                        JobResult::Walk(start, headers::walk(&session, start, first).await)
                    }
                    Job::Segment(segment) => JobResult::Segment(
                        segment,
                        segment::fetch(&session, segment, canyon_time).await,
                    ),
                };
                Done { peer, result }
            };
            self.jobs.spawn(job.in_current_span());
        }
        self.warn_if_starved(sessions.len(), now);
    }

    /// Picks what a free session fetches next, and marks it as taken: the next page of the
    /// walk while it lasts, then segments, failed ones first. `None` if there is nothing the
    /// peer holds, or nothing to do right now.
    fn next_job(&mut self, session: &SessionHandle) -> Option<Job> {
        let holds = |first: BlockNumber, last: BlockNumber| self.serves(session, first, last);
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

    /// Says, at most once per [`STARVED_INTERVAL`], that sessions are open and free but none
    /// serves the blocks needed next.
    fn warn_if_starved(&mut self, sessions: usize, now: Instant) {
        if sessions == 0 || !self.jobs.is_empty() || !self.schedule.all_free(now) {
            return;
        }
        if self
            .starved_warned
            .is_some_and(|warned| now.duration_since(warned) < STARVED_INTERVAL)
        {
            return;
        }
        self.starved_warned = Some(now);
        let (needed_from, needed_to) = match self.walk_from() {
            Some(start) => (self.first, start.number),
            None => (self.next_emit, self.anchor.number),
        };
        warn!(
            needed_from,
            needed_to,
            peers = sessions,
            "range sync is waiting: no connected execution peer says it serves these blocks"
        );
    }

    /// Handles the end of one job. Returns `false` when the sync has to stop: the node is
    /// shutting down or nothing takes its output.
    async fn finished(&mut self, done: Done, cancel: &CancellationToken) -> Result<bool, ElError> {
        let Done { peer, result } = done;
        let (first, last, failure) = match result {
            JobResult::Walk(start, Ok((checkpoints, below))) => {
                self.walking = false;
                self.anchor_served = true;
                if below.is_some_and(|below| !self.linked(below)) {
                    return Ok(true);
                }
                self.succeeded(peer);
                debug!(%peer, from = start.number, checkpoints = checkpoints.len(), "headers verified");
                for checkpoint in &checkpoints {
                    self.checkpoints.insert(checkpoint.number, checkpoint.hash);
                }
                if self.walk_from().is_none() {
                    self.walked = true;
                    info!(
                        segments = self.checkpoints.len(),
                        "header chain verified down to the first block; fetching blocks"
                    );
                }
                return Ok(send(&self.saved, checkpoints, "checkpoints", cancel).await);
            }
            JobResult::Segment(segment, Ok(blocks)) => {
                self.succeeded(peer);
                self.anchor_served = true;
                // The lowest segment's first block names the block the range extends: checked
                // here too, for a range with no walk.
                if segment.first == self.first
                    && let Some(block) = blocks.first()
                {
                    let header: Option<alloy_consensus::Header> =
                        alloy_rlp::decode_exact(&block.header).ok();
                    let below = header.map(|header| BlockRef {
                        number: header.number.saturating_sub(1),
                        hash: header.parent_hash,
                    });
                    if below.is_some_and(|below| !self.linked(below)) {
                        return Ok(true);
                    }
                }
                self.ready.insert(segment.first, (segment, blocks));
                return Ok(self.hand_on(cancel).await);
            }
            JobResult::Walk(start, Err(failure)) => {
                self.walking = false;
                if start == self.anchor
                    && !self.anchor_served
                    && matches!(failure, Failure::NotHeld)
                {
                    self.anchor_refused.insert(peer);
                }
                (start.number, start.number, failure)
            }
            JobResult::Segment(segment, Err(failure)) => {
                if segment.top == self.anchor
                    && !self.anchor_served
                    && matches!(failure, Failure::NotHeld)
                {
                    self.anchor_refused.insert(peer);
                }
                self.waiting.insert(segment.first, segment);
                (segment.first, segment.top.number, failure)
            }
        };
        metrics::sync_request(failure.outcome());
        if let Some(report) = self.schedule.finished(peer, Some(&failure)) {
            self.peers.report(report);
        }
        // Only indexers are asked for blocks before Bedrock.
        if first < self.indexers_only_below && matches!(failure, Failure::NotHeld) {
            let misses = self.indexer_misses.entry(peer).or_default();
            *misses = misses.saturating_add(1);
            if *misses >= MAX_INDEXER_MISSES {
                self.indexer_misses.remove(&peer);
                self.peers.report(Report::NotHolding(peer));
            }
        }
        match failure {
            Failure::Unsupported(reason) => Err(ElError::Sync(reason)),
            Failure::Panicked(source) => Err(ElError::Task {
                task: "range sync verification",
                source,
            }),
            Failure::Invalid(_) | Failure::Malformed(_) | Failure::Undecodable(_) => {
                warn!(%peer, first, last, %failure, "range sync: peer's answer rejected");
                Ok(true)
            }
            Failure::NotHeld | Failure::Timeout | Failure::Closed => {
                debug!(%peer, first, last, %failure, "range sync: request failed");
                Ok(true)
            }
        }
    }

    fn succeeded(&mut self, peer: PeerId) {
        metrics::sync_request(SyncOutcome::Verified);
        self.indexer_misses.remove(&peer);
        // A success is never reported to the peer set.
        let _report = self.schedule.finished(peer, None);
    }

    /// Hands on every verified segment that is next in order. Returns `false` when the sync
    /// has to stop.
    async fn hand_on(&mut self, cancel: &CancellationToken) -> bool {
        while let Some((segment, blocks)) = self.ready.remove(&self.next_emit) {
            let count = blocks.len();
            if !send(&self.blocks, blocks, "blocks", cancel).await {
                return false;
            }
            metrics::sync_blocks(count, segment.top.number);
            self.next_emit = segment.top.number.saturating_add(1);
            self.outstanding = self.outstanding.saturating_sub(1);
            self.checkpoints = self.checkpoints.split_off(&self.next_emit);
        }
        true
    }

    fn log_progress(&self) {
        if let Some(lowest) = self.walk_from() {
            // Sessions whose peer says it holds the next page of the walk.
            let page_first = lowest
                .number
                .saturating_sub(HEADERS_PER_REQUEST - 1)
                .max(self.first);
            let sessions = self.peers.sessions();
            let usable = sessions
                .iter()
                .filter(|session| self.serves(session, page_first, lowest.number))
                .count();
            let waiting = match (usable, lowest == self.anchor) {
                (0, true) => "range sync: waiting for a peer that holds the anchor",
                (0, false) => "range sync: waiting for a peer that holds these headers",
                _ => "range sync: verifying the header chain",
            };
            info!(
                verified_down_to = lowest.number,
                first = self.first,
                peers = usable,
                sessions = sessions.len(),
                "{waiting}"
            );
        } else {
            info!(
                next = self.next_emit,
                anchor = self.anchor.number,
                in_progress = self.outstanding,
                sessions = self.schedule.len(),
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
