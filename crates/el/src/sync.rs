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
//!   of the range. A *skeleton* is fetched first, one page at a time: the hash of every
//!   1,024th block going down, a thousand of them per request (`GetBlockHeaders` with a skip).
//!   Those are claims. The 1,024-block gaps below each are then fetched in parallel on every
//!   session, each as a hash chain down from its claimed top, and *linked* from the top down:
//!   a gap is accepted only once its top is the hash the gap above it names as parent (the
//!   anchor for the first), so the trust still runs from the anchor down; a gap fetched from a
//!   wrong claim is fetched again from the trusted hash. Every [`SEGMENT_BLOCKS`]-th hash is
//!   kept as a checkpoint and reported as it is linked, so the binary can save it: a restart
//!   continues from the lowest one.
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
//! receipts fetcher uses, with a few requests at a time on each (`schedule`); the session
//! routes answers by request id, so a request for a new block's receipts is not queued behind
//! them. Faster peers are given work first.
//!
//! The headers of the range are downloaded twice, once by the walk and once per segment: the
//! walk runs from the top and the fetch from the bottom, and holding the walk's headers until
//! the fetch reaches them would mean keeping the whole chain's.

mod fill;
mod headers;
mod schedule;
mod segment;

pub(crate) use fill::run as run_fills;

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
use crate::peers::{Peers, Report, closed};
use crate::session::{RequestError, SessionHandle};

/// Blocks between two checkpoints, and the fewest blocks one session fetches as a unit (a
/// segment, handed to the pipeline as one batch). A segment is held in memory until it is
/// handed on.
const SEGMENT_BLOCKS: u64 = 256;
/// The most blocks of one segment: what peers answer in one request (1,024 headers, bodies or
/// blocks of receipts). Segments of light blocks span several checkpoints, so a request
/// carries as many blocks as a peer answers at once: the 200 ms between two requests to a
/// peer, not the size of an answer, is what bounds a sync of light blocks.
const MAX_SEGMENT_BLOCKS: u64 = 1024;
/// The bytes a segment aims at, from the blocks' size so far: one answer of bodies or
/// receipts (peers stop an answer past 2 MiB). Heavier blocks keep segments of
/// [`SEGMENT_BLOCKS`], whose answers come in several pages.
const SEGMENT_TARGET_BYTES: u64 = 2 << 20;
/// Segments fetched or waiting to be handed on at once: enough for every session to keep a
/// few requests in flight. Bounds how far the fetch runs ahead of a pipeline that stores
/// slowly; [`MAX_READY_BYTES`] bounds the memory.
const MAX_SEGMENTS_AHEAD: usize = 32;
/// Bytes of verified segments waiting to be handed on, past which no segment is started: a
/// dozen OP Mainnet segments (about 19 MB each), hundreds of Unichain ones.
const MAX_READY_BYTES: usize = 256 << 20;
/// Gaps of the walk fetched or waiting to be linked at once, below the lowest verified block:
/// 256 of 1,024 blocks. Each kept gap holds a handful of hashes.
const MAX_GAPS_AHEAD: u64 = 256;
/// How often progress is logged, and how often the sessions are looked at again when nothing
/// else happens (a peer's announced range can change without a session opening or ending).
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
/// How often it is said that no connected peer serves the blocks needed.
const STARVED_INTERVAL: Duration = Duration::from_mins(1);
/// Peers that must say they do not hold the anchor before it is given up, and then only once
/// every peer that says it holds the anchor's height has: a block no peer serves (one a reorg
/// left behind, mostly) would otherwise keep the round open for ever.
const ANCHOR_REFUSALS: usize = 3;
/// How long a round waits with no open session whose peer says it holds the anchor before it
/// gives the anchor up, so the next round can anchor where peers are.
const ANCHOR_UNSERVED: Duration = Duration::from_mins(2);
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
        peers.report(Report::Syncing(true));
        let ended_as = syncer.run(&cancel).await;
        peers.report(Report::Syncing(false));
        let Some(end) = ended_as? else {
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
    /// Claimed hashes of every 1,024th block below where the walk started, from the skeleton.
    skeleton: BTreeMap<BlockNumber, B256>,
    /// Where the next page of the skeleton starts; `None` once it reaches the first block.
    skeleton_from: Option<BlockRef>,
    /// Whether a page of the skeleton is being fetched.
    skeleton_busy: bool,
    /// Gaps of the walk fetched and not yet linked, by top.
    gaps: BTreeMap<BlockNumber, Gap>,
    /// Gaps being fetched, by top.
    gaps_busy: HashSet<BlockNumber>,
    /// Bytes of the segments in `ready`.
    ready_bytes: usize,
    /// Bytes per block of the segments fetched so far, smoothed; 0 before the first.
    block_bytes: u64,
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
    /// Time spent waiting for the pipeline to take a batch since progress was last logged:
    /// time the fetch was held up by storing.
    store_wait: Duration,
    jobs: JoinSet<Done>,
    /// When it was last said that no peer serves what is needed.
    starved_warned: Option<Instant>,
    /// Whether a page of headers from the anchor down has verified: some peer serves it.
    anchor_served: bool,
    /// Peers that said they do not hold the anchor, while none has served it.
    anchor_refused: HashSet<PeerId>,
    /// Since when no open session's peer says it holds the anchor; `None` while one does, or
    /// once it was served.
    anchor_unserved_since: Option<Instant>,
    /// "Not held" answers in a row for blocks before Bedrock, per indexer.
    indexer_misses: HashMap<PeerId, u32>,
    /// The block the first block must name as parent; `None` for any.
    extends: Option<BlockRef>,
    /// How the round ends before it is complete, once that is known: the chain down from the
    /// anchor turned out not to reach `extends`.
    given_up: Option<RoundEnd>,
}

/// A gap of the walk, fetched as a hash chain down from a claimed top.
#[derive(Debug)]
struct Gap {
    /// The top's hash it was fetched from: the gap is linked only if it is the trusted one.
    top_hash: B256,
    /// Its checkpoints, highest first (see `headers::walk`).
    checkpoints: Vec<BlockRef>,
    /// The parent of its lowest block, when it reaches the first block of the range.
    below: Option<BlockRef>,
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
    /// A page of the skeleton, down from this block.
    Skeleton(BlockRef),
    /// A gap of the walk, down from this block.
    Gap(BlockRef),
    Segment(Segment),
}

/// The end of one job on one session.
#[derive(Debug)]
struct Done {
    peer: PeerId,
    result: JobResult,
    /// How long the job took, for the peer's speed.
    took: Duration,
}

#[derive(Debug)]
enum JobResult {
    /// A page of the skeleton from this block: the claimed hashes below it.
    Skeleton(BlockRef, Result<Vec<BlockRef>, Failure>),
    /// A gap of the walk from this block, with the checkpoints it verified and, once it
    /// reaches the first block, that block's parent.
    Gap(BlockRef, Result<(Vec<BlockRef>, Option<BlockRef>), Failure>),
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
            skeleton: BTreeMap::new(),
            skeleton_from: None,
            skeleton_busy: false,
            gaps: BTreeMap::new(),
            gaps_busy: HashSet::new(),
            ready_bytes: 0,
            block_bytes: 0,
            next_assign: first,
            next_emit: first,
            waiting: BTreeMap::new(),
            ready: BTreeMap::new(),
            outstanding: 0,
            store_wait: Duration::ZERO,
            jobs: JoinSet::new(),
            starved_warned: None,
            anchor_served: false,
            anchor_refused: HashSet::new(),
            anchor_unserved_since: None,
            indexer_misses: HashMap::new(),
            extends,
            given_up: None,
        };
        syncer.walked = syncer.walk_from().is_none();
        syncer.skeleton_from = syncer.walk_from();
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
                _ = progress.tick() => {
                    self.log_progress();
                    // A report may have been dropped: the peer set hears it again.
                    self.peers.report(Report::Syncing(true));
                }
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
            if self.anchor_unserved() {
                warn!(
                    anchor = self.anchor.number,
                    advertised = ?self.peers.advertised_latest(),
                    "range sync gives up its anchor: no peer has said it holds it for 2 minutes"
                );
                break Some(RoundEnd::AnchorUnavailable);
            }
            self.dispatch();
        };
        // In-flight jobs have nowhere to deliver.
        self.jobs.shutdown().await;
        Ok(complete)
    }

    /// Whether no open session's peer has said it holds the anchor for [`ANCHOR_UNSERVED`].
    fn anchor_unserved(&mut self) -> bool {
        let number = self.anchor.number;
        let served = self.anchor_served
            || self
                .peers
                .sessions()
                .iter()
                .any(|session| self.serves(session, number, number));
        if served {
            self.anchor_unserved_since = None;
            return false;
        }
        let since = *self.anchor_unserved_since.get_or_insert_with(Instant::now);
        since.elapsed() >= ANCHOR_UNSERVED
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
        // The fastest peers first, so they get the work when there is little.
        let mut order: Vec<(f64, &SessionHandle)> = sessions
            .iter()
            .map(|session| (self.schedule.rate(&session.status().peer_id), session))
            .collect();
        order.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
        for (_, session) in order {
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
                let started = Instant::now();
                let result = match job {
                    Job::Skeleton(from) => {
                        JobResult::Skeleton(from, headers::skeleton(&session, from, first).await)
                    }
                    Job::Gap(top) => JobResult::Gap(top, headers::walk(&session, top, first).await),
                    Job::Segment(segment) => JobResult::Segment(
                        segment,
                        segment::fetch(&session, segment, canyon_time).await,
                    ),
                };
                Done {
                    peer,
                    result,
                    took: started.elapsed(),
                }
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
        if let Some(lowest) = self.walk_from() {
            // The skeleton first, one page at a time: each starts where the one before ended.
            if !self.skeleton_busy
                && let Some(from) = self.skeleton_from
                && holds(
                    from.number
                        .saturating_sub(HEADERS_PER_REQUEST)
                        .max(self.first),
                    from.number,
                )
            {
                self.skeleton_busy = true;
                return Some(Job::Skeleton(from));
            }
            // Then a gap: the one below the lowest verified block, from its trusted hash, or one
            // further down from its claimed top, within reach of the linking.
            let reach = lowest
                .number
                .saturating_sub(MAX_GAPS_AHEAD.saturating_mul(HEADERS_PER_REQUEST));
            let claimed = self
                .skeleton
                .range(reach..lowest.number)
                .rev()
                .map(|(number, hash)| BlockRef {
                    number: *number,
                    hash: *hash,
                });
            let top = std::iter::once(lowest).chain(claimed).find(|top| {
                let gap_first = self.gap_first(top.number);
                !self.gaps.contains_key(&top.number)
                    && !self.gaps_busy.contains(&top.number)
                    && holds(gap_first, top.number)
            })?;
            self.gaps_busy.insert(top.number);
            return Some(Job::Gap(top));
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
        if self.outstanding >= MAX_SEGMENTS_AHEAD || self.ready_bytes >= MAX_READY_BYTES {
            return None;
        }
        // The furthest checkpoint within the segment's span, and at least the next one.
        let end = self.next_assign.saturating_add(self.segment_span());
        let mut tops = self.checkpoints.range(self.next_assign..);
        let next = tops.next()?;
        let (number, hash) = tops
            .take_while(|(number, _)| **number < end)
            .last()
            .unwrap_or(next);
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

    /// Blocks a new segment spans: [`SEGMENT_TARGET_BYTES`] of blocks of the size seen so far,
    /// in whole checkpoints, from [`SEGMENT_BLOCKS`] to [`MAX_SEGMENT_BLOCKS`].
    fn segment_span(&self) -> u64 {
        if self.block_bytes == 0 {
            return SEGMENT_BLOCKS;
        }
        let blocks = SEGMENT_TARGET_BYTES / self.block_bytes;
        (blocks / SEGMENT_BLOCKS * SEGMENT_BLOCKS).clamp(SEGMENT_BLOCKS, MAX_SEGMENT_BLOCKS)
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
        let Done { peer, result, took } = done;
        let (first, last, failure) = match result {
            JobResult::Skeleton(from, Ok(claims)) => {
                self.skeleton_fetched(peer, from, &claims, took);
                return Ok(true);
            }
            JobResult::Gap(top, Ok((checkpoints, below))) => {
                self.gap_fetched(
                    peer,
                    top,
                    Gap {
                        top_hash: top.hash,
                        checkpoints,
                        below,
                    },
                    took,
                );
                return self.link(cancel).await;
            }
            JobResult::Segment(segment, Ok(blocks)) => {
                self.succeeded(peer, blocks.len(), took);
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
                let bytes = batch_bytes(&blocks);
                self.measured_size(bytes, blocks.len());
                self.ready_bytes = self.ready_bytes.saturating_add(bytes);
                self.ready.insert(segment.first, (segment, blocks));
                return Ok(self.hand_on(cancel).await);
            }
            JobResult::Skeleton(from, Err(failure)) => {
                self.skeleton_busy = false;
                (from.number, from.number, failure)
            }
            JobResult::Gap(top, Err(failure)) => {
                self.gaps_busy.remove(&top.number);
                if top == self.anchor && !self.anchor_served && matches!(failure, Failure::NotHeld)
                {
                    self.anchor_refused.insert(peer);
                }
                let gap_first = self.gap_first(top.number);
                (gap_first, top.number, failure)
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

    /// Records a job of `blocks` blocks that verified in `took`.
    fn succeeded(&mut self, peer: PeerId, blocks: usize, took: Duration) {
        self.indexer_misses.remove(&peer);
        self.schedule.measured(peer, blocks, took);
        // A success is never reported to the peer set.
        let _report = self.schedule.finished(peer, None);
    }

    /// Records the size of `blocks` verified blocks of `bytes` bytes, for [`Self::segment_span`].
    fn measured_size(&mut self, bytes: usize, blocks: usize) {
        let (Ok(bytes), Ok(blocks)) = (u64::try_from(bytes), u64::try_from(blocks)) else {
            return;
        };
        let Some(size) = bytes.checked_div(blocks) else {
            return;
        };
        // Smoothed as peers' rates are: a quarter of the new segment.
        self.block_bytes = if self.block_bytes == 0 {
            size.max(1)
        } else {
            self.block_bytes
                .saturating_mul(3)
                .saturating_add(size)
                .div_ceil(4)
        };
    }

    /// The first block of the gap below `top`: a page of the walk, not below the first block.
    fn gap_first(&self, top: BlockNumber) -> BlockNumber {
        top.saturating_sub(HEADERS_PER_REQUEST - 1).max(self.first)
    }

    /// Records a page of the skeleton: its claims, and where the next page starts.
    fn skeleton_fetched(
        &mut self,
        peer: PeerId,
        from: BlockRef,
        claims: &[BlockRef],
        took: Duration,
    ) {
        self.skeleton_busy = false;
        self.succeeded(peer, claims.len(), took);
        debug!(%peer, from = from.number, claims = claims.len(), "skeleton fetched");
        // Below the last claim the next page starts; past the first block, none.
        self.skeleton_from = claims
            .last()
            .copied()
            .filter(|last| last.number.saturating_sub(self.first) >= HEADERS_PER_REQUEST);
        self.skeleton
            .extend(claims.iter().map(|claim| (claim.number, claim.hash)));
    }

    /// Keeps a fetched gap for [`Self::link`].
    fn gap_fetched(&mut self, peer: PeerId, top: BlockRef, gap: Gap, took: Duration) {
        self.gaps_busy.remove(&top.number);
        if top == self.anchor {
            self.anchor_served = true;
        }
        let blocks = usize::try_from(HEADERS_PER_REQUEST).unwrap_or(usize::MAX);
        self.succeeded(peer, blocks, took);
        debug!(%peer, from = top.number, checkpoints = gap.checkpoints.len(), "headers verified");
        self.gaps.insert(top.number, gap);
    }

    /// Links the fetched gaps from the top down: each is accepted once its top is the lowest
    /// verified hash, and its checkpoints become verified in turn. A gap fetched from a wrong
    /// claim is dropped, and that gap is fetched again from the trusted hash. Returns `false`
    /// when the sync has to stop.
    async fn link(&mut self, cancel: &CancellationToken) -> Result<bool, ElError> {
        while let Some(lowest) = self.walk_from() {
            let Some(gap) = self.gaps.remove(&lowest.number) else {
                break;
            };
            if gap.top_hash != lowest.hash {
                debug!(
                    number = lowest.number,
                    "skeleton claim does not link; fetching the gap again from the verified hash"
                );
                self.skeleton.insert(lowest.number, lowest.hash);
                break;
            }
            if gap.below.is_some_and(|below| !self.linked(below)) {
                return Ok(true);
            }
            for checkpoint in &gap.checkpoints {
                self.checkpoints.insert(checkpoint.number, checkpoint.hash);
            }
            if self.walk_from().is_none() {
                self.walked = true;
                info!(
                    segments = self.checkpoints.len(),
                    "header chain verified down to the first block; fetching blocks"
                );
            }
            if !send(&self.saved, gap.checkpoints, "checkpoints", cancel).await {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Hands on every verified segment that is next in order. Returns `false` when the sync
    /// has to stop.
    async fn hand_on(&mut self, cancel: &CancellationToken) -> bool {
        while let Some((segment, blocks)) = self.ready.remove(&self.next_emit) {
            self.ready_bytes = self.ready_bytes.saturating_sub(batch_bytes(&blocks));
            let started = Instant::now();
            let sent = send(&self.blocks, blocks, "blocks", cancel).await;
            self.store_wait = self.store_wait.saturating_add(started.elapsed());
            if !sent {
                return false;
            }
            self.next_emit = segment.top.number.saturating_add(1);
            self.outstanding = self.outstanding.saturating_sub(1);
            self.checkpoints = self.checkpoints.split_off(&self.next_emit);
        }
        true
    }

    fn log_progress(&mut self) {
        if let Some(lowest) = self.walk_from() {
            // Sessions whose peer says it holds the next page of the walk.
            let page_first = self.gap_first(lowest.number);
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
            // The oldest block any askable peer still holds: a gap starting below it cannot be
            // filled from these peers (they prune).
            let held_from = sessions
                .iter()
                .filter(|session| session.is_askable())
                .map(|session| session.range().earliest)
                .min();
            if usable == 0 && held_from.is_some_and(|earliest| page_first < earliest) {
                warn!(
                    verified_down_to = lowest.number,
                    first = self.first,
                    peers_hold_from = ?held_from,
                    sessions = sessions.len(),
                    "range sync: no peer holds blocks this old (their history starts later); \
                     waiting for a peer that does, such as an archive node or an op-p2p-indexer"
                );
                return;
            }
            info!(
                verified_down_to = lowest.number,
                first = self.first,
                peers = usable,
                sessions = sessions.len(),
                advertised = ?self.peers.advertised_latest(),
                "{waiting}"
            );
        } else {
            let store_wait = std::mem::take(&mut self.store_wait);
            info!(
                next = self.next_emit,
                anchor = self.anchor.number,
                in_progress = self.outstanding,
                fetching = self.jobs.len(),
                segment_blocks = self.segment_span(),
                ready = self.ready.len(),
                store_wait_ms = store_wait.as_millis(),
                sessions = self.schedule.len(),
                "range sync: fetching blocks"
            );
        }
    }
}

/// The bytes of a batch of blocks: headers, bodies and receipts.
fn batch_bytes(blocks: &[EncodedBlock]) -> usize {
    blocks
        .iter()
        .map(|block| {
            block
                .header
                .len()
                .saturating_add(block.body.len())
                .saturating_add(block.receipts.as_ref().map_or(0, |receipts| receipts.len()))
        })
        .fold(0, usize::saturating_add)
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
