//! The commitment task: turns dispute games verified on L1 into the safe and finalized heads
//! promotion follows.
//!
//! A game is a claim, made on L1, about the output root of an L2 block. The L1 side has
//! verified that the game exists in an L1 block it trusts; it has not looked at our chain.
//! Here the claim is compared with our own block at that height: the output root is computed
//! from our header (state root, the message passer's storage root, block hash) and must equal
//! the claimed one. Only then does that block, number and hash, become a head:
//!
//! - **safe**: the highest block a recent game on the L1 chain the L1 side follows claims and
//!   that matches;
//! - **finalized**: the same, among the games whose L1 block is finalized.
//!
//! Every recent game is judged, not only the newest: anyone who posts the bond can create a
//! game, and one that claims a block that is not ours must not hide the honest ones. A claim
//! that does not match our block is logged as an error with both values (once per game and
//! block) and counted: either L1 commits to another chain than the one we hold, or the claim is
//! wrong. Games are judged again whenever they change and every [`RECHECK_INTERVAL`], so a game
//! about a block we do not hold yet, or one whose height our canonical chain has since changed
//! at, is decided by the block we hold then.
//!
//! **Heads only move up, across runs too.** The heads start at what the committed store
//! recorded and are never published below it: after a restart the L1 side finds older games
//! first, and promoting on them would undo committed work. A recorded finalized head above
//! the safe one (left by a rollback) is not taken over: the finalized head starts unknown.
//!
//! **A game that matched is remembered** (the whole game and our block's hash) while it is
//! recent, and not read again: when its L1 block finalizes, minutes after it raised the safe
//! head, promotion has usually pruned the block from the unsafe store, and without the archive
//! it could not be read. The memory starts empty: after a restart, a finalized game at or
//! below the committed safe head is judged from the archive.
//!
//! Our block is read from the unsafe store (the canonical block at that height) or, when it
//! has been promoted or imported, from the archive. Does not fetch anything from L1 and does
//! not promote: it only publishes [`L1Heads`].
//!
//! "Safe" here means "a bonded claim on L1 equals our block", not that the block's batch is on
//! L1: the game's type is not checked against the type the chain's portal respects, and anyone
//! who posts the bond can create a game. A game can still be challenged, and a head set by a
//! game whose L1 block is later reorged out is not lowered. "Finalized" means the claim's L1
//! block is finalized, not that the game has resolved.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use alloy_consensus::Header;
use alloy_primitives::{B256, BlockNumber, keccak256};
use op_indexer_primitives::{
    BlockRead, BlockRef, BlockStart, ClaimMismatch, L1Games, L1Heads, ReadLimits, VerifiedGame,
};
use op_indexer_storage::{ArchiveStore, Store, UnsafeStore};
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::metrics::{self, GameOutcome};
use crate::retry::{retry, settle};
use crate::{PipelineError, PipelineError::Storage};

/// How often the games are judged again without a change: a game about a block we do not
/// hold yet, or about a height our canonical chain has changed at.
const RECHECK_INTERVAL: Duration = Duration::from_secs(12);
/// Mismatches remembered so each is logged once; past it the set starts over, which only
/// repeats a log line.
const MAX_LOGGED_MISMATCHES: usize = 256;

/// What is remembered of past verdicts. Keyed by the whole game, not its address: the
/// factory's addresses follow its nonce, so after an L1 reorg another claim can get the same
/// address.
#[derive(Debug, Default)]
struct Verdicts {
    /// The recent games that matched, with our block's hash.
    matched: HashMap<VerifiedGame, B256>,
    /// The disagreements already logged, with our block's hash.
    logged: HashSet<(VerifiedGame, B256)>,
}

/// What is known of our block at a height: enough to compute its output root.
#[derive(Debug, Clone, Copy)]
struct OurBlock {
    hash: B256,
    timestamp: u64,
    state_root: B256,
    /// From Isthmus on, the storage root of the message passer.
    withdrawals_root: Option<B256>,
}

/// Checks the games `games` holds against our chain, each time they change and every
/// [`RECHECK_INTERVAL`], and publishes the heads on `heads`, starting from `committed`, the
/// heads the committed store recorded, until the L1 side drops its sender or `cancel` fires.
/// `isthmus_time` is the chain's Isthmus activation: claims about blocks before it cannot be
/// checked.
///
/// # Errors
///
/// Returns [`PipelineError::Storage`] if a store fails in a way retrying cannot fix while our
/// block is read.
pub(crate) async fn run<U: UnsafeStore, A: ArchiveStore>(
    unsafe_store: U,
    archive: Option<A>,
    mut games: watch::Receiver<L1Games>,
    isthmus_time: u64,
    heads: watch::Sender<L1Heads>,
    committed: L1Heads,
    cancel: CancellationToken,
) -> Result<(), PipelineError> {
    // Never below what is committed: promotion would take a lower head for an L1 reorg. This
    // task is the only writer of `heads`, so what it published last is `held`. A finalized
    // head above the safe one is what a rollback left: not a block of the safe chain.
    let mut held = committed;
    held.finalized = held.finalized.filter(|finalized| {
        held.safe
            .is_some_and(|safe| finalized.number <= safe.number)
    });
    let mut verdicts = Verdicts::default();
    let mut recheck = interval(RECHECK_INTERVAL);
    recheck.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let current = games.borrow_and_update().clone();
        let Some(next) = judge_all(
            &unsafe_store,
            archive.as_ref(),
            &current,
            isthmus_time,
            held,
            &mut verdicts,
            &cancel,
        )
        .await?
        else {
            return Ok(());
        };
        held = next;
        // Both heads in one update: promotion never sees one moved without the other.
        heads.send_if_modified(|heads| {
            let changed = *heads != next;
            *heads = next;
            changed
        });
        let changed = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            changed = games.changed() => changed.is_ok(),
            _ = recheck.tick() => true,
        };
        // A closed channel is the L1 side stopping.
        if !changed {
            return Ok(());
        }
    }
}

/// Judges the recent games above `current`, highest claimed block first, and returns the
/// heads they raise `current` to: the safe head to the highest matching block, the finalized
/// head to the highest matching block of a game in a finalized L1 block. `None` if
/// cancellation ended a read.
async fn judge_all<U: UnsafeStore, A: ArchiveStore>(
    unsafe_store: &U,
    archive: Option<&A>,
    games: &L1Games,
    isthmus_time: u64,
    current: L1Heads,
    verdicts: &mut Verdicts,
    cancel: &CancellationToken,
) -> Result<Option<L1Heads>, PipelineError> {
    let mut next = current;
    verdicts
        .matched
        .retain(|game, _| games.recent.contains(game));
    let mut candidates: Vec<&VerifiedGame> = games.recent.iter().collect();
    candidates.sort_by_key(|game| std::cmp::Reverse(game.l2_block));
    for game in candidates {
        let finalized = games
            .finalized_l1_block
            .is_some_and(|finalized| game.l1_block.number <= finalized);
        let raises = |head: Option<BlockRef>| head.is_none_or(|head| game.l2_block > head.number);
        // Below both heads: neither this game nor any lower one raises anything.
        if !raises(next.safe) && !raises(next.finalized) {
            break;
        }
        // Below the safe head and not final: only a finalized game can still raise anything.
        if !raises(next.safe) && !finalized {
            continue;
        }
        let hash = if let Some(hash) = verdicts.matched.get(game) {
            *hash
        } else {
            let Some(ours) = our_block(unsafe_store, archive, game.l2_block, cancel).await? else {
                return Ok(None);
            };
            let Some(ours) = ours else {
                // Ahead of what we hold: judged again later.
                debug!(l2_block = game.l2_block, "a game's block is not held yet");
                continue;
            };
            if !judge(game, finalized, isthmus_time, ours, &mut verdicts.logged) {
                continue;
            }
            verdicts.matched.insert(*game, ours.hash);
            ours.hash
        };
        let head = BlockRef {
            number: game.l2_block,
            hash,
        };
        if finalized && raises(next.finalized) {
            next.finalized = Some(head);
        }
        // A finalized block is safe too: the safe head is never left below it.
        if raises(next.safe) {
            next.safe = Some(head);
        }
    }
    if next != current {
        info!(safe = ?next.safe, finalized = ?next.finalized, "L1 heads raised by dispute games");
    }
    Ok(Some(next))
}

/// Compares `game` with our block. Returns whether they agree; logs a disagreement once per
/// game and block.
fn judge(
    game: &VerifiedGame,
    finalized: bool,
    isthmus_time: u64,
    ours: OurBlock,
    logged: &mut HashSet<(VerifiedGame, B256)>,
) -> bool {
    let checked = game.check(
        ours.hash,
        (ours.timestamp, isthmus_time),
        ours.state_root,
        ours.withdrawals_root,
    );
    let Err(err) = checked else {
        metrics::game(GameOutcome::Matched);
        info!(
            l2_block = game.l2_block,
            hash = %ours.hash,
            finalized,
            l1_block = game.l1_block.number,
            game = %game.game,
            "a dispute game on L1 commits to our block"
        );
        return true;
    };
    if logged.len() >= MAX_LOGGED_MISMATCHES {
        logged.clear();
    }
    if !logged.insert((*game, ours.hash)) {
        return false;
    }
    if let ClaimMismatch::NoStorageRoot { .. } = err {
        // Before Isthmus the header does not carry what the claim commits to.
        metrics::game(GameOutcome::Unchecked);
        warn!(
            l2_block = game.l2_block,
            game = %game.game,
            %err,
            "a dispute game cannot be checked against our block; it does not raise a head"
        );
    } else {
        metrics::game(GameOutcome::Mismatch);
        error!(
            l2_block = game.l2_block,
            our_hash = %ours.hash,
            l1_block = game.l1_block.number,
            game = %game.game,
            game_type = game.game_type,
            finalized,
            %err,
            "a dispute game on L1 does NOT match our block: it does not raise a head"
        );
    }
    false
}

/// Reads our block at height `number` from the unsafe store or the archive. The outer
/// `None` means cancellation ended a retry; the inner one that neither store holds it.
async fn our_block<U: UnsafeStore, A: ArchiveStore>(
    unsafe_store: &U,
    archive: Option<&A>,
    number: BlockNumber,
    cancel: &CancellationToken,
) -> Result<Option<Option<OurBlock>>, PipelineError> {
    const CANONICAL: &str = "unsafe canonical";
    const READ: &str = "archive read";
    let canonical = retry(cancel, Store::Unsafe, CANONICAL, || {
        unsafe_store.canonical(number)
    });
    let Some(canonical) = settle(canonical.await, CANONICAL)? else {
        return Ok(None);
    };
    if let Some(block) = canonical {
        return Ok(Some(Some(OurBlock::new(block.hash, &block.block.header))));
    }
    let Some(archive) = archive else {
        return Ok(Some(None));
    };
    let one = ReadLimits {
        items: 1,
        bytes: usize::MAX,
    };
    let header = retry(cancel, Store::Archive, READ, || {
        let headers = BlockRead::Headers {
            start: BlockStart::Number(number),
            step: 1,
            rising: true,
        };
        archive.read(headers, one, None)
    });
    let Some(mut header) = settle(header.await, READ)? else {
        return Ok(None);
    };
    let Some(raw) = header.pop() else {
        return Ok(Some(None));
    };
    // The archive checked that these bytes hash to the block's hash when it stored them.
    let header: Header = alloy_rlp::decode_exact(&raw).map_err(|_err| Storage {
        operation: READ,
        source: op_indexer_storage::StorageError::InvalidData {
            store: Store::Archive,
            what: "header",
            block: Some(keccak256(&raw)),
            source: None,
        },
    })?;
    Ok(Some(Some(OurBlock::new(keccak256(&raw), &header))))
}

impl OurBlock {
    const fn new(hash: B256, header: &Header) -> Self {
        Self {
            hash,
            timestamp: header.timestamp,
            state_root: header.state_root,
            withdrawals_root: header.withdrawals_root,
        }
    }
}
