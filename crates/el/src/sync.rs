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

use std::collections::BTreeMap;
use std::time::Duration;

use alloy_primitives::{B256, BlockNumber};
use op_indexer_primitives::{BlockRef, EncodedBlock, SyncRange};
use reth_network_peers::PeerId;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use self::headers::HEADERS_PER_REQUEST;
use self::schedule::Schedule;
use crate::ElError;
use crate::metrics::{self, SyncOutcome};
use crate::peers::{Peers, closed};
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

/// What a range sync fetches, known once its anchor is.
#[derive(Debug)]
pub struct SyncPlan {
    /// The first block to fetch and the trusted anchor at the top.
    pub range: SyncRange,
    /// Blocks whose hash an earlier run verified from the same anchor; empty for a new sync.
    /// Fetching resumes from them without walking the chain again.
    pub checkpoints: Vec<BlockRef>,
}

/// A range sync to run: where its plan comes from and where its output goes.
#[derive(Debug)]
pub struct RangeSync {
    /// Receives what to fetch, once: the anchor may be a block the node has yet to learn.
    /// Nothing is fetched before it arrives, or at all if the sender is dropped.
    pub plan: oneshot::Receiver<SyncPlan>,
    /// Receives the verified blocks in ascending order, in batches of consecutive blocks. The
    /// sync waits when it is full and stops when it closes.
    pub blocks: mpsc::Sender<Vec<EncodedBlock>>,
    /// Receives blocks whose hash is verified, to be saved and given back in
    /// [`SyncPlan::checkpoints`] on the next start. The sync waits when it is full and stops when
    /// it closes.
    pub verified: mpsc::Sender<Vec<BlockRef>>,
}

/// Runs the range sync `sync` until `cancel` fires: waits for its plan, then fetches. When the
/// range is complete, no plan comes, or nothing takes its output any more, it stops fetching
/// and waits for `cancel`, so the rest of the network carries on.
///
/// # Errors
///
/// Returns [`ElError::Sync`] if blocks of the chain cannot be read by this build,
/// [`ElError::ChannelClosed`] if the peer set stopped while the node was running, and
/// [`ElError::Task`] if verification panicked.
pub(crate) async fn run(
    canyon_time: u64,
    peers: Peers,
    sync: RangeSync,
    cancel: CancellationToken,
) -> Result<(), ElError> {
    let RangeSync {
        plan,
        blocks,
        verified,
    } = sync;
    let plan = tokio::select! {
        biased;
        () = cancel.cancelled() => return Ok(()),
        plan = plan => plan,
    };
    let Ok(plan) = plan else {
        // Nothing to sync.
        cancel.cancelled().await;
        return Ok(());
    };
    Syncer::new(canyon_time, peers, plan, blocks, verified)
        .run(cancel)
        .await
}

/// The range syncer.
#[derive(Debug)]
struct Syncer {
    canyon_time: u64,
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
    /// A page of the walk from this block, with the checkpoints it verified.
    Walk(BlockRef, Result<Vec<BlockRef>, Failure>),
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
        canyon_time: u64,
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
        } = plan;
        let mut checkpoints: BTreeMap<BlockNumber, B256> = checkpoints
            .into_iter()
            .filter(|checkpoint| (first..anchor.number).contains(&checkpoint.number))
            .map(|checkpoint| (checkpoint.number, checkpoint.hash))
            .collect();
        checkpoints.insert(anchor.number, anchor.hash);
        let mut syncer = Self {
            canyon_time,
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
        };
        syncer.walked = syncer.walk_from().is_none();
        syncer
    }

    async fn run(mut self, cancel: CancellationToken) -> Result<(), ElError> {
        if self.is_complete() {
            info!(
                first = self.first,
                anchor = self.anchor.number,
                "range sync has nothing to fetch"
            );
            cancel.cancelled().await;
            return Ok(());
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
        loop {
            // A resting peer becomes free without anything else happening.
            let wake = self.schedule.next_wake(Instant::now());
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(joined) = self.jobs.join_next() => {
                    let done = joined.map_err(|source| {
                        ElError::Task { task: "range sync", source }
                    })?;
                    if !self.finished(done, &cancel).await? {
                        break;
                    }
                }
                alive = self.peers.changed() => {
                    if !alive {
                        return closed("sessions", &cancel);
                    }
                    self.schedule.retain(&self.peers.sessions());
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
            self.jobs.spawn(async move {
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
            });
        }
        self.warn_if_starved(sessions.len(), now);
    }

    /// Picks what a free session fetches next, and marks it as taken: the next page of the
    /// walk while it lasts, then segments, failed ones first. `None` if there is nothing the
    /// peer holds, or nothing to do right now.
    fn next_job(&mut self, session: &SessionHandle) -> Option<Job> {
        let range = session.range();
        let holds =
            |first: BlockNumber, last: BlockNumber| range.earliest <= first && last <= range.latest;
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
            JobResult::Walk(start, Ok(checkpoints)) => {
                self.walking = false;
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
                self.ready.insert(segment.first, (segment, blocks));
                return Ok(self.hand_on(cancel).await);
            }
            JobResult::Walk(start, Err(failure)) => {
                self.walking = false;
                (start.number, start.number, failure)
            }
            JobResult::Segment(segment, Err(failure)) => {
                self.waiting.insert(segment.first, segment);
                (segment.first, segment.top.number, failure)
            }
        };
        metrics::sync_request(failure.outcome());
        if let Some(report) = self.schedule.finished(peer, Some(&failure)) {
            self.peers.report(report);
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
