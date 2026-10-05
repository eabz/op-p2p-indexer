//! Range planning and promotion gating; does not own network or pipeline lifecycles.

use crate::Archive;
use alloy_primitives::BlockNumber;
use op_indexer_el::{Peers, RoundEnd, SyncPlan};
use op_indexer_p2p::{NodeStore, StoreError};
use op_indexer_primitives::{BlockRef, L1Heads, SyncRange};
use op_indexer_storage::{UnsafeStore, unsafe_store::MemoryStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, warn};

/// How far below the head the archive may be and still be extended by promotion: the most
/// blocks the unsafe store returns in one read back from a head. A range sync round is planned
/// only for a larger gap.
const CAUGHT_UP_BLOCKS: u64 = 1024;
/// Rest after a round is given up before the next starts, doubled for each round given up in
/// a row.
const ABANDONED_ANCHOR_WAIT: Duration = Duration::from_mins(1);
/// The longest rest after a round is given up.
const ABANDONED_ANCHOR_MAX_WAIT: Duration = Duration::from_mins(30);
/// How long the archive may take to reach the anchor of a round the execution network has
/// fetched completely. Longer, the round's blocks were left out (they did not extend the
/// archive), and the round is given up.
const ROUND_STORE_TIMEOUT: Duration = Duration::from_mins(2);
/// How often a failing read of the archive or the node store is warned about while retried.
const RETRY_WARN_INTERVAL: Duration = Duration::from_mins(1);
/// How far below the gossiped head a round anchors when no safe head is known: the anchor is
/// a block an unsafe reorg will not replace in practice (they are a few blocks deep).
const ANCHOR_DEPTH: u64 = 64;
/// How often the archive is asked whether a round of the range sync has reached its end, and
/// whether it holds the safe block of the L1 heads promotion waits for.
const SYNC_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// What planning the range sync needs.
pub(crate) struct SyncInputs<A> {
    /// The archive the sync fills: its last block is where each round starts.
    pub(crate) archive: A,
    /// Where a block [`ANCHOR_DEPTH`] below the gossiped head is looked up.
    pub(crate) unsafe_store: MemoryStore,
    /// The unsafe head gossip delivers.
    pub(crate) gossip_head: watch::Receiver<Option<BlockRef>>,
    /// The L1 heads, before promotion sees them: the safe block is the preferred anchor.
    pub(crate) l1_heads: watch::Receiver<L1Heads>,
    /// The committed safe block's number, which the pipeline publishes.
    pub(crate) committed: watch::Receiver<BlockNumber>,
    /// Whether the L1 side runs: rounds are then anchored on safe heads only.
    pub(crate) l1: bool,
}

/// How a round ended, as the planner sees it.
enum RoundOutcome {
    /// The archive holds the anchor.
    Stored,
    /// The anchor was given up: no peer served it, or its chain does not reach the archive.
    Abandoned,
    /// The node is stopping.
    Stop,
}

/// The rest after a round was given up: when the next one may start, and how long the
/// rest was, doubled if the next is given up too.
struct Rest {
    until: tokio::time::Instant,
    wait: Duration,
}

/// Plans the range sync, round by round, for as long as the node runs, and hands each plan to
/// the execution network: from the block after the archive's last one up to an anchor whose
/// hash is trusted.
///
/// The sync only closes the gaps gossip cannot (see [`next_anchor`] for when a round is
/// planned and what it is anchored on); promotion extends the archive otherwise. The first
/// anchor is the one an unfinished round of an earlier run was working towards, so its
/// verified checkpoints are kept. After a round is given up the next waits
/// [`ABANDONED_ANCHOR_WAIT`], doubled for each round given up in a row. Reads of the archive
/// and the node store that fail are retried; the planner ends only when the node stops.
pub(crate) async fn plan_sync<A: Archive>(
    store: Arc<NodeStore>,
    mut inputs: SyncInputs<A>,
    peers: Peers,
    plans: mpsc::Sender<SyncPlan>,
) {
    let mut resume = {
        let store = Arc::clone(&store);
        let read = retried(&plans, "its saved anchor", move || store.sync_anchor());
        match read.await {
            Some(anchor) => anchor,
            None => return,
        }
    };
    let mut rest: Option<Rest> = None;
    loop {
        let Some(tip) = archive_tip(&inputs.archive, &plans).await else {
            return;
        };
        let from = tip.map_or(0, |tip| tip.number.saturating_add(1));
        // An unfinished round is finished first.
        let resumed = resume.take().filter(|anchor| anchor.number >= from);
        let anchor = match resumed {
            Some(anchor) => anchor,
            None => {
                match next_anchor(&mut inputs, &peers, from, rest.as_ref(), &plans).await {
                    Some(Some(anchor)) => anchor,
                    // Something moved, or a wait ended: look again.
                    Some(None) => continue,
                    None => return,
                }
            }
        };
        let checkpoints = {
            let store = Arc::clone(&store);
            let read = retried(&plans, "its checkpoints", move || {
                store.sync_checkpoints(anchor)
            });
            match read.await {
                Some(checkpoints) => checkpoints,
                None => return,
            }
        };
        info!(
            from,
            to = anchor.number,
            anchor = %anchor.hash,
            resumed = resumed.is_some(),
            "range sync planned: fetching these blocks from execution peers"
        );
        let (ended, end) = oneshot::channel();
        let plan = SyncPlan {
            range: SyncRange { from, anchor },
            checkpoints,
            extends: tip,
            ended,
        };
        // The execution network is gone if this fails: the node is shutting down.
        if plans.send(plan).await.is_err() {
            return;
        }
        match round(&inputs.archive, anchor, end, &plans).await {
            RoundOutcome::Stored => rest = None,
            RoundOutcome::Abandoned => {
                let wait = rest.as_ref().map_or(ABANDONED_ANCHOR_WAIT, |earlier| {
                    earlier
                        .wait
                        .saturating_mul(2)
                        .min(ABANDONED_ANCHOR_MAX_WAIT)
                });
                rest = Some(Rest {
                    until: tokio::time::Instant::now() + wait,
                    wait,
                });
            }
            RoundOutcome::Stop => return,
        }
    }
}

/// The anchor of a round from `from`, if one is needed now (see [`plan_sync`]); otherwise
/// waits for the heads to move or the rest after a round given up to end, and returns
/// `Some(None)` so the archive is looked at again. `None` when the node stops.
///
/// With the L1 side a round is needed while the archive is [`CAUGHT_UP_BLOCKS`] or more below
/// the safe head, or below the committed safe block, and is anchored on the safe head only:
/// everything the sync writes is then committed on L1, so no reorg can leave it behind.
/// Without it a round is needed while the archive is that far below the gossiped head, and is
/// anchored on the gossiped block [`ANCHOR_DEPTH`] below it, or lower, at the newest block
/// peers say they hold ([`Peers::advertised_latest`]): peers announce their range only every
/// few minutes, so an anchor at the head would wait on a height none of them has announced.
/// An unsafe reorg deeper than the anchor would leave the archive on a dead branch, which only
/// rebuilding the archive repairs.
async fn next_anchor<A: Archive>(
    inputs: &mut SyncInputs<A>,
    peers: &Peers,
    from: BlockNumber,
    rest: Option<&Rest>,
    plans: &mpsc::Sender<SyncPlan>,
) -> Option<Option<BlockRef>> {
    let tip = from.checked_sub(1);
    let safe = inputs.l1_heads.borrow_and_update().safe;
    let head = *inputs.gossip_head.borrow_and_update();
    let committed = *inputs.committed.borrow_and_update();
    let far = |number: BlockNumber| number.saturating_sub(from) >= CAUGHT_UP_BLOCKS;
    let resting = rest
        .map(|rest| rest.until)
        .filter(|until| *until > tokio::time::Instant::now());
    let anchor = if resting.is_some() {
        None
    } else if inputs.l1 {
        let behind_committed = tip.is_some_and(|tip| tip < committed);
        safe.filter(|safe| safe.number >= from && (far(safe.number) || behind_committed))
    } else {
        match head.filter(|head| far(head.number)) {
            Some(head) => {
                let advertised = peers.advertised_latest();
                anchor_below(&inputs.unsafe_store, head, advertised, from).await
            }
            None => None,
        }
    };
    if anchor.is_some() {
        return Some(anchor);
    }
    let rest = async {
        match resting {
            Some(until) => tokio::time::sleep_until(until).await,
            None => std::future::pending().await,
        }
    };
    // A closed channel is the node stopping.
    tokio::select! {
        () = plans.closed() => return None,
        changed = inputs.l1_heads.changed() => changed.ok()?,
        changed = inputs.gossip_head.changed() => changed.ok()?,
        changed = inputs.committed.changed() => changed.ok()?,
        () = rest => {}
    }
    Some(None)
}

/// The gossiped canonical block [`ANCHOR_DEPTH`] below `head`, or lower at `advertised`, the
/// newest block peers say they hold; `None` without a peer saying so, below `from`, or if the
/// unsafe store does not hold the chain that far down (right after a start: then the next head
/// is tried).
async fn anchor_below(
    unsafe_store: &MemoryStore,
    head: BlockRef,
    advertised: Option<BlockNumber>,
    from: BlockNumber,
) -> Option<BlockRef> {
    let height = head.number.checked_sub(ANCHOR_DEPTH)?.min(advertised?);
    if height < from {
        return None;
    }
    match unsafe_store.canonical(height).await {
        Ok(block) => block.map(|block| BlockRef {
            number: height,
            hash: block.hash,
        }),
        Err(err) => {
            debug!(%err, height, "no range sync anchor at this height yet");
            None
        }
    }
}

/// Waits until the archive holds `anchor`, or the execution network gives the round up
/// (`end`).
async fn round<A: Archive>(
    archive: &A,
    anchor: BlockRef,
    mut end: oneshot::Receiver<RoundEnd>,
    plans: &mpsc::Sender<SyncPlan>,
) -> RoundOutcome {
    let mut fetching = true;
    // Set once the round is fetched: the pipeline has that long to store it.
    let mut fetched_at: Option<tokio::time::Instant> = None;
    loop {
        let Some(tip) = archive_tip(archive, plans).await else {
            return RoundOutcome::Stop;
        };
        if tip.is_some_and(|tip| tip.number >= anchor.number) {
            return RoundOutcome::Stored;
        }
        if fetched_at.is_some_and(|at| at.elapsed() >= ROUND_STORE_TIMEOUT) {
            warn!(
                anchor = anchor.number,
                archive_tip = ?tip,
                "range sync round fetched but not stored: its blocks do not extend the archive"
            );
            return RoundOutcome::Abandoned;
        }
        tokio::select! {
            () = plans.closed() => return RoundOutcome::Stop,
            ended = &mut end, if fetching => {
                fetching = false;
                match ended {
                    Ok(RoundEnd::AnchorUnavailable | RoundEnd::NotLinked) => {
                        return RoundOutcome::Abandoned;
                    }
                    // The pipeline is storing the last batches.
                    Ok(RoundEnd::Complete) => fetched_at = Some(tokio::time::Instant::now()),
                    // The network is stopping, which `plans.closed()` shows.
                    Err(_) => {}
                }
            }
            () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
        }
    }
}

/// Hands the L1 heads to promotion. With the range sync on (`gate`: the archive and the
/// committed safe block's number), a head is held while the archive is more than
/// [`CAUGHT_UP_BLOCKS`] below its safe block, or below the committed safe block: promotion
/// could not read the gap from the unsafe store, and with the L1 side a sync round closes it.
/// The hold is looked at again every [`SYNC_POLL_INTERVAL`] against the archive as it grows,
/// so a head can be released while a round is still storing: the range task then leaves out
/// the blocks promotion appended first. Otherwise, or if
/// the archive cannot be read, heads go straight to promotion. The finalized head is held
/// with the safe one: promotion takes them together. Ends when the pipeline or the L1 source
/// is gone.
pub(crate) async fn forward_l1_heads<A: Archive>(
    mut heads: watch::Receiver<L1Heads>,
    promotion: watch::Sender<L1Heads>,
    gate: Option<(A, watch::Receiver<BlockNumber>)>,
) {
    loop {
        let current = *heads.borrow_and_update();
        let held = match (&gate, current.safe) {
            (Some((archive, committed)), Some(safe)) => match archive.range().await {
                Ok(range) => {
                    let tip = range.map_or(0, |(_, tip)| tip.number);
                    tip.saturating_add(CAUGHT_UP_BLOCKS) < safe.number || tip < *committed.borrow()
                }
                Err(err) => {
                    debug!(%err, "cannot read the archive; the L1 heads go to promotion");
                    false
                }
            },
            _ => false,
        };
        if !held {
            promotion.send_if_modified(|sent| {
                let changed = *sent != current;
                *sent = current;
                changed
            });
        }
        tokio::select! {
            () = promotion.closed() => return,
            changed = heads.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = tokio::time::sleep(SYNC_POLL_INTERVAL), if held => {}
        }
    }
}

/// The archive's last block, `Some(None)` for an empty archive. A failing read is retried
/// every [`SYNC_POLL_INTERVAL`] and warned about once per [`RETRY_WARN_INTERVAL`]. `None`
/// when the node stops (`plans` closes).
async fn archive_tip<A: Archive>(
    archive: &A,
    plans: &mpsc::Sender<SyncPlan>,
) -> Option<Option<BlockRef>> {
    let mut warned: Option<tokio::time::Instant> = None;
    loop {
        match archive.range().await {
            Ok(range) => return Some(range.map(|(_, tip)| tip)),
            Err(err) => {
                if warned.is_none_or(|at| at.elapsed() >= RETRY_WARN_INTERVAL) {
                    warned = Some(tokio::time::Instant::now());
                    warn!(%err, "range sync: the block archive cannot be read; retrying");
                }
            }
        }
        tokio::select! {
            () = plans.closed() => return None,
            () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
        }
    }
}

/// Runs the node-store read `read` on a blocking thread until it succeeds, retried like
/// [`archive_tip`]. `None` when the node stops.
async fn retried<T, F>(plans: &mpsc::Sender<SyncPlan>, what: &'static str, read: F) -> Option<T>
where
    T: Send + 'static,
    F: Fn() -> Result<T, StoreError> + Clone + Send + 'static,
{
    let mut warned: Option<tokio::time::Instant> = None;
    loop {
        let attempt = tokio::task::spawn_blocking(read.clone()).await;
        let err = match attempt {
            Ok(Ok(value)) => return Some(value),
            Ok(Err(err)) => err.to_string(),
            Err(err) => err.to_string(),
        };
        if warned.is_none_or(|at| at.elapsed() >= RETRY_WARN_INTERVAL) {
            warned = Some(tokio::time::Instant::now());
            warn!(%err, what, "range sync: cannot read the node store; retrying");
        }
        tokio::select! {
            () = plans.closed() => return None,
            () = tokio::time::sleep(SYNC_POLL_INTERVAL) => {}
        }
    }
}
