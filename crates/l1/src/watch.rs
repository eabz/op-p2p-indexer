//! The watcher: follows the L1 chain down from trusted block hashes and finds the dispute
//! games the chain's factory created.
//!
//! A trusted L1 block (its number and hash, from the beacon light client) is the only thing
//! taken on trust. From it the watcher walks the headers down, each one the parent named by
//! its child, until it reaches a block it already knows. A block holds a game only if (1) its
//! header's logs bloom may contain the factory's `DisputeGameCreated` event, and (2) one of
//! its transactions was sent to the factory. The bloom is tested first because it costs
//! nothing, but on mainnet it is a weak filter: blooms are about three quarters full, and
//! about one header in six passes (measured 2026-10-04). So the transactions are fetched next
//! (checked against the header), and only a block with a transaction to the factory has its
//! receipts fetched (checked too); the game is read from the event and that call (`create`,
//! or `createWithInitData` for a format that says so, Base's games).
//! A game created through another contract is not found: it would be refused anyway.
//!
//! It publishes the most recent games ([`MAX_RECENT_GAMES`]) on the chain linked by parent
//! hashes down from the newest block walked, and the highest finalized L1 block on that
//! chain. A game in a block below a break in that chain (a block not walked yet, or one of
//! another chain) is held back until the break is walked, and dropped if its block is dropped
//! first: so every game at or below the finalized block is on the finalized chain. Every
//! recent game is kept, not only the newest: anyone who posts the bond can create a game, so
//! the newest may claim a block that is not ours. It does not check a game's claim against
//! our own blocks: that is done where those blocks are.
//!
//! **Junk games.** The bound of [`MAX_RECENT_GAMES`] is a liveness lever: 64 games created
//! within the finality lag (64 bonds) push an honest game out before its L1 block is
//! finalized, so the finalized head waits for the next honest one. Heads never go down. The
//! first walk after a start likewise ends at the first game old enough to be finalized, even
//! if that game is junk.
//!
//! A block counts as walked only once it has been scanned, so a block whose transactions or
//! receipts could not be read is read again by a later walk. A walk that stops part-way is
//! resumed from where it stopped when the next trusted block arrives, until a reorg or
//! another walk has linked the hole.
//!
//! **Reorgs.** A trusted head is not final. When a new trusted head does not build on the
//! blocks walked before, the blocks it replaces are forgotten with their games, so the newest
//! game can become an older one. A finalized block the walked chain does not hold replaces
//! it the same way. Finalized blocks are never replaced.
//!
//! **Start.** Nothing is stored between runs. The first trusted block is the light client's
//! checkpoint, a finalized block; the first heads follow within seconds. The first walk goes
//! down until a game old enough to be finalized has been found, or [`MAX_BACKFILL_BLOCKS`]
//! have been walked, so after a start recent games are known again without waiting for new
//! ones. A head more than [`MAX_BACKFILL_BLOCKS`] above the checkpoint does not connect to it:
//! the games in between are not seen.
//!
//! **Trusted blocks are coalesced.** A walk can take a while; the blocks told meanwhile wait
//! in the channel, and only the last head and the newest finalized block of what waited are
//! followed.

use std::collections::BTreeMap;
use std::ops::Range;

use alloy_consensus::{Header, Sealed, Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{B256, BlockNumber, Bloom, BloomInput, Bytes};
use op_indexer_chainspec::{ChainSpec, CreatedGame, created_topic};
use op_indexer_primitives::{BlockRef, L1Games, VerifiedGame};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::TrustedL1Block;
use crate::fetch::{Fetcher, Stop};

/// Headers asked for in one request while walking down: half of what peers answer at most.
const HEADER_BATCH: u64 = 512;
/// Most blocks walked down from one trusted head: a day of 12-second blocks. Games are
/// created every hour or few, so the first walk normally ends long before; later walks cover
/// the few blocks since the last trusted head.
const MAX_BACKFILL_BLOCKS: usize = 7_200;
/// The first walk ends at a game this many blocks below the trusted head: old enough to be
/// in a finalized block (finality trails the head by two to three epochs of 32 blocks).
const FINALITY_MARGIN_BLOCKS: u64 = 128;
/// Most L1 blocks remembered above the finalized one. Finality normally trails by about a
/// hundred blocks; this only bounds memory while L1 does not finalize.
const MAX_TRACKED_BLOCKS: u64 = 16_384;
/// Most games published: about a day of a proposer's games, plus room for games others
/// created.
const MAX_RECENT_GAMES: usize = 64;

/// Follows trusted L1 blocks and publishes the games found.
#[derive(Debug)]
pub(crate) struct Watcher {
    chain: &'static ChainSpec,
    /// What a header's logs bloom must contain for its block to hold a game: the factory's
    /// address and the event's topic.
    needle: Bloom,
    fetcher: Fetcher,
    trusted: mpsc::Receiver<TrustedL1Block>,
    /// The newest trusted block, which the L1 sessions advertise as their head.
    head: watch::Sender<Option<BlockRef>>,
    games: watch::Sender<L1Games>,
    /// The L1 blocks walked and scanned, by number: the canonical chain as far as it is known.
    blocks: BTreeMap<BlockNumber, Walked>,
    /// The games found, by the number of the L1 block that created them, in block order.
    found: BTreeMap<BlockNumber, Vec<VerifiedGame>>,
    /// The highest finalized L1 block number told whose hash is on the linked chain.
    finalized: Option<BlockNumber>,
    /// Walks that stopped part-way, below blocks they recorded: where each continues.
    resumes: Vec<Resume>,
}

/// A block walked and scanned.
#[derive(Debug, Clone, Copy)]
struct Walked {
    hash: B256,
    parent: B256,
}

/// Where a walk starts, or continues after it stopped: the block not yet scanned, and what
/// the walk is for.
#[derive(Debug, Clone, Copy)]
struct Resume {
    number: BlockNumber,
    hash: B256,
    /// The block above, the child of `hash`, once the walk has recorded it. A stopped walk is
    /// kept only then (before, it left no hole), and continues only while that block is still
    /// walked and the block at `number` is not `hash`.
    child: Option<B256>,
    /// The trusted block the walk began at.
    head: BlockNumber,
    /// Whether it was the first walk, which ends at a game old enough to be finalized.
    first: bool,
}

impl Watcher {
    pub(crate) fn new(
        chain: &'static ChainSpec,
        fetcher: Fetcher,
        trusted: mpsc::Receiver<TrustedL1Block>,
        head: watch::Sender<Option<BlockRef>>,
        games: watch::Sender<L1Games>,
    ) -> Self {
        let mut needle = Bloom::ZERO;
        needle.accrue(BloomInput::Raw(chain.dispute_game_factory.as_slice()));
        needle.accrue(BloomInput::Raw(created_topic().as_slice()));
        Self {
            chain,
            needle,
            fetcher,
            trusted,
            head,
            games,
            blocks: BTreeMap::new(),
            found: BTreeMap::new(),
            finalized: None,
            resumes: Vec::new(),
        }
    }

    /// Runs until `cancel` fires or the source of trusted blocks closes its channel.
    pub(crate) async fn run(mut self, cancel: CancellationToken) {
        loop {
            let block = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                block = self.trusted.recv() => block,
            };
            // A closed channel is the light client shutting down.
            let Some(block) = block else { return };
            // What waited meanwhile: only the newest head and the newest finalized block count.
            let (mut head, mut finalized) = (None, None);
            for block in
                std::iter::once(block).chain(std::iter::from_fn(|| self.trusted.try_recv().ok()))
            {
                let slot = if block.finalized {
                    &mut finalized
                } else {
                    &mut head
                };
                // The last told is the light client's newest view, even when a reorg lowered it.
                *slot = Some(block);
            }
            // The head first: its walk covers the finalized block, which is then linked.
            for block in [head, finalized].into_iter().flatten() {
                match self.follow(block, &cancel).await {
                    Ok(()) => {}
                    Err(Stop::Cancelled) => return,
                    // A head that was replaced before any peer was asked: the next one follows.
                    Err(Stop::NotHeld) => warn!(
                        number = block.number,
                        hash = %block.hash,
                        "no L1 peer serves a trusted block; waiting for the next one"
                    ),
                }
            }
        }
    }

    /// Takes one trusted block into account: walks down from it, continues a walk that
    /// stopped, scans what is new, and publishes the games.
    async fn follow(
        &mut self,
        block: TrustedL1Block,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        let TrustedL1Block {
            number,
            hash,
            finalized,
        } = block;
        // Sessions advertise the newest block known; it only moves forward.
        self.head.send_if_modified(|head| {
            let newer = head.is_none_or(|head| number > head.number);
            if newer {
                *head = Some(BlockRef { number, hash });
            }
            newer
        });
        // A head below blocks walked before, or a finalized block the walked chain does not
        // hold: what is above it was replaced.
        if !finalized || self.hash_at(number).is_some_and(|known| known != hash) {
            self.forget_above(number);
        }
        // Walked already (a finalized block was a head earlier), or below everything kept
        // (an old block told late): nothing to read.
        let known = self.hash_at(number) == Some(hash);
        let below = self
            .blocks
            .first_key_value()
            .is_some_and(|(first, _)| number < *first);
        let mut walked = if known || below {
            Ok(())
        } else {
            let start = Resume {
                number,
                hash,
                child: None,
                head: number,
                first: self.blocks.is_empty(),
            };
            self.walk(start, cancel).await
        };
        // Walks that stopped earlier continue, below what was just walked. One that stops
        // again is kept, from where it stopped.
        while walked.is_ok()
            && let Some(resume) = self.resumes.pop()
        {
            if is_open(&self.blocks, &resume) {
                walked = self.walk(resume, cancel).await;
            }
        }
        // A finalized block counts only once its hash is on the linked chain: then every
        // block below it on that chain is its ancestor.
        if finalized && self.hash_at(number) == Some(hash) && number >= self.unlinked().end {
            self.finalized = self.finalized.max(Some(number));
        }
        // What was walked before a stop stays: the next trusted block connects to it.
        self.prune();
        self.publish();
        walked
    }

    /// Walks the headers down from `start` until a known block is reached, scanning each new
    /// one for games. A `first` walk ends at a game old enough to be finalized. If it stops
    /// below a block it (or the walk it continues) recorded, where it stopped is kept so a
    /// later call continues it; one that recorded nothing leaves no hole, and the next trusted
    /// block walks it again.
    async fn walk(&mut self, start: Resume, cancel: &CancellationToken) -> Result<(), Stop> {
        let Resume {
            number,
            head,
            first,
            ..
        } = start;
        // The headers down to the newest block known, if the head builds on it.
        let gap = self
            .blocks
            .range(..number)
            .next_back()
            .map(|(top, _)| number.saturating_sub(*top));
        let mut limit = gap.map_or(HEADER_BATCH, |gap| gap.min(HEADER_BATCH));
        let mut walked = 0_usize;
        // Where the walk continues if it stops: `resume.hash` is the next block to read.
        let mut resume = start;
        while walked < MAX_BACKFILL_BLOCKS {
            let headers = self
                .fetcher
                .headers(resume.hash, limit, cancel)
                .await
                .inspect_err(|_| self.resumes.extend(resume.child.map(|_| resume)))?;
            limit = HEADER_BATCH;
            for block in headers {
                let at = block.number;
                let games = self
                    .scan(&block, cancel)
                    .await
                    .inspect_err(|_| self.resumes.extend(resume.child.map(|_| resume)))?;
                // Recorded only now that it is scanned. Another block known at this height
                // was replaced, with its games.
                let walked_block = Walked {
                    hash: block.hash(),
                    parent: block.parent_hash,
                };
                if self.blocks.insert(at, walked_block).is_some() {
                    self.found.remove(&at);
                }
                resume = Resume {
                    number: at.saturating_sub(1),
                    hash: block.parent_hash,
                    child: Some(block.hash()),
                    ..resume
                };
                walked = walked.saturating_add(1);
                if !games.is_empty() {
                    for game in &games {
                        info!(
                            l1_block = at,
                            game = %game.game,
                            game_type = game.game_type,
                            l2_block = game.l2_block,
                            "dispute game found on L1"
                        );
                    }
                    // Published when the walk ends or stops: mid-walk, the blocks below the
                    // one not yet connected are unlinked, and the games would shrink to those
                    // above it.
                    self.found.insert(at, games);
                    // After a start: enough is known once a game old enough to be finalized
                    // has been found.
                    if first && at.saturating_add(FINALITY_MARGIN_BLOCKS) <= head {
                        return Ok(());
                    }
                }
                // The parent is known: the walk has reached the chain walked before.
                let connected = at
                    .checked_sub(1)
                    .is_none_or(|parent| self.hash_at(parent) == Some(resume.hash));
                if connected {
                    return Ok(());
                }
            }
        }
        if !first {
            warn!(
                walked,
                "an L1 head does not connect to the blocks known within the walk limit; \
                 games in between are not seen"
            );
        }
        Ok(())
    }

    /// Looks for the games the factory created in `block`, in the order they were created.
    async fn scan(
        &mut self,
        block: &Sealed<Header>,
        cancel: &CancellationToken,
    ) -> Result<Vec<VerifiedGame>, Stop> {
        // The bloom has no false negatives; on mainnet about four blocks in five end here.
        if !block.logs_bloom.contains(&self.needle) {
            return Ok(Vec::new());
        }
        let transactions = self.fetcher.transactions(block, cancel).await?;
        // Most of the rest end here: no transaction was sent to the factory.
        let factory = self.chain.dispute_game_factory;
        let to_factory = |transaction: &Bytes| {
            TxEnvelope::decode_2718(&mut transaction.as_ref())
                .is_ok_and(|transaction| transaction.to() == Some(factory))
        };
        if !transactions.iter().any(to_factory) {
            return Ok(Vec::new());
        }
        let receipts = self
            .fetcher
            .receipts(block, transactions.len(), cancel)
            .await?;
        let l1_block = BlockRef {
            number: block.number,
            hash: block.hash(),
        };
        let mut games = Vec::new();
        for (transaction, receipt) in transactions.iter().zip(&receipts) {
            let created = receipt
                .logs()
                .iter()
                .filter(|log| log.address == factory)
                .filter_map(|log| CreatedGame::from_topics(log.topics()));
            for created in created {
                match self.game(l1_block, transaction, &created) {
                    Ok(game) => games.push(game),
                    Err(reason) => warn!(
                        l1_block = l1_block.number,
                        game = %created.game,
                        game_type = created.game_type,
                        reason,
                        "a dispute game on L1 cannot be read; it is passed over"
                    ),
                }
            }
        }
        if games.is_empty() {
            debug!(
                l1_block = l1_block.number,
                "the bloom matched, but the block has no game"
            );
        }
        Ok(games)
    }

    /// Reads what `created` claims from the transaction that emitted its event.
    fn game(
        &self,
        l1_block: BlockRef,
        transaction: &Bytes,
        created: &CreatedGame,
    ) -> Result<VerifiedGame, String> {
        let transaction = TxEnvelope::decode_2718(&mut transaction.as_ref())
            .map_err(|err| format!("its transaction cannot be decoded: {err}"))?;
        let claim = self
            .chain
            .game_claim(created, transaction.to(), transaction.input())
            .map_err(|err| err.to_string())?;
        Ok(VerifiedGame {
            l1_block,
            game: created.game,
            game_type: created.game_type,
            l2_block: claim.l2_block,
            output_root: claim.output_root,
            timestamp: claim.timestamp,
        })
    }

    /// The hash of the block walked at `number`.
    fn hash_at(&self, number: BlockNumber) -> Option<B256> {
        self.blocks.get(&number).map(|block| block.hash)
    }

    /// The blocks kept that are not on the chain linked by parent hashes down from the newest
    /// block walked: from the lowest block kept up to that chain's lowest block, where a
    /// block not walked yet or one of another chain breaks it. Empty when all are linked.
    fn unlinked(&self) -> Range<BlockNumber> {
        let mut blocks = self.blocks.iter().rev();
        let Some((&top, newest)) = blocks.next() else {
            return 0..0;
        };
        let (mut floor, mut parent) = (top, newest.parent);
        for (&number, block) in blocks {
            if number.saturating_add(1) != floor || block.hash != parent {
                break;
            }
            (floor, parent) = (number, block.parent);
        }
        let first = self
            .blocks
            .first_key_value()
            .map_or(floor, |(first, _)| *first);
        first..floor
    }

    /// Forgets the blocks above `number` and their games.
    fn forget_above(&mut self, number: BlockNumber) {
        let above = number.saturating_add(1);
        drop(self.blocks.split_off(&above));
        drop(self.found.split_off(&above));
    }

    /// Drops what is no longer needed: blocks below the finalized one (or beyond the tracking
    /// limit), games beyond the most recent [`MAX_RECENT_GAMES`], and stopped walks that are
    /// closed.
    fn prune(&mut self) {
        let Some((&top, _)) = self.blocks.last_key_value() else {
            return;
        };
        let floor = self
            .finalized
            .unwrap_or(0)
            .max(top.saturating_sub(MAX_TRACKED_BLOCKS));
        // Games of blocks about to be dropped that are not on the linked chain go with them:
        // once their blocks are gone, nothing could tell.
        let unlinked = self.unlinked();
        let dropped = unlinked.start..unlinked.end.min(floor);
        self.found.retain(|at, _| !dropped.contains(at));
        self.blocks = self.blocks.split_off(&floor);
        let blocks = &self.blocks;
        self.resumes
            .retain(|resume| resume.number >= floor && is_open(blocks, resume));
        let mut kept = 0_usize;
        let oldest_kept = self.found.iter().rev().find_map(|(at, games)| {
            kept = kept.saturating_add(games.len());
            (kept >= MAX_RECENT_GAMES).then_some(*at)
        });
        if let Some(oldest) = oldest_kept {
            self.found = self.found.split_off(&oldest);
        }
    }

    /// Publishes the games on the linked chain, if they changed. Games below every block
    /// kept count: their blocks were on the linked chain when they were dropped.
    fn publish(&self) {
        let unlinked = self.unlinked();
        let mut recent: Vec<VerifiedGame> = self
            .found
            .iter()
            .filter(|(at, _)| !unlinked.contains(at))
            .flat_map(|(_, games)| games.iter().copied())
            .collect();
        let excess = recent.len().saturating_sub(MAX_RECENT_GAMES);
        recent.drain(..excess);
        let games = L1Games {
            recent,
            finalized_l1_block: self.finalized,
        };
        self.games.send_if_modified(|current| {
            let changed = *current != games;
            if changed {
                *current = games;
            }
            changed
        });
    }
}

/// Whether a stopped walk still has something to do: the block it recorded last is still
/// walked, and the block at `number` is not the one it names as parent (none, or one of
/// another chain). A reorg or another walk closes it.
fn is_open(blocks: &BTreeMap<BlockNumber, Walked>, resume: &Resume) -> bool {
    let child = blocks.get(&resume.number.saturating_add(1));
    resume.child.is_some()
        && child.map(|child| child.hash) == resume.child
        && blocks
            .get(&resume.number)
            .is_none_or(|block| block.hash != resume.hash)
}
