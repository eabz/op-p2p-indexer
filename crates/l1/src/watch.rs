//! The watcher: follows the L1 chain down from trusted block hashes and finds the dispute
//! games the chain's factory created.
//!
//! A trusted L1 block (its number and hash, from the beacon light client) is the only thing
//! taken on trust. From it the watcher walks the headers down, each one the parent named by
//! its child, until it reaches a block it already knows. For each new header it tests the
//! logs bloom for the factory's `DisputeGameCreated` event; only when the bloom may hold it
//! does it fetch the block's transactions and receipts (both checked against the header) and
//! read the game from the event and the `create` call that emitted it.
//!
//! It publishes the newest game on the chain it has walked and the newest game in a
//! finalized block. It does not check a game's claim against our own blocks: that is done
//! where those blocks are.
//!
//! **Reorgs.** A trusted head is not final. When a new trusted head does not build on the
//! blocks walked before, the blocks it replaces are forgotten with their games, so the newest
//! game can become an older one. Finalized blocks are never replaced.
//!
//! **Start.** Nothing is stored between runs. The first trusted head is walked down until a
//! game old enough to be finalized has been found, or [`MAX_BACKFILL_BLOCKS`] have been
//! walked: after a start both games are known again without waiting for new ones.

use std::collections::BTreeMap;

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
    /// The L1 blocks walked, by number: the canonical chain as far as it is known.
    blocks: BTreeMap<BlockNumber, B256>,
    /// The games found, by the number of the L1 block that created them.
    found: BTreeMap<BlockNumber, VerifiedGame>,
    /// The highest finalized L1 block number told.
    finalized: Option<BlockNumber>,
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

    /// Takes one trusted block into account: walks down from it, scans what is new, and
    /// publishes the games.
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
        if finalized {
            self.finalized = self.finalized.max(Some(number));
        } else {
            // A head below blocks walked before: what is above it was replaced.
            self.forget_above(number);
        }
        // Walked already (a finalized block was a head earlier), or below everything kept
        // (an old block told late): nothing to read.
        let known = self.blocks.get(&number) == Some(&hash);
        let below = self
            .blocks
            .first_key_value()
            .is_some_and(|(first, _)| number < *first);
        if !known && !below {
            let walked = self.walk(number, hash, cancel).await;
            // What was walked before a stop stays: the next trusted block connects to it.
            self.prune();
            self.publish();
            return walked;
        }
        self.prune();
        self.publish();
        Ok(())
    }

    /// Walks the headers down from the block `number` with hash `from` until a known block is
    /// reached, scanning each new one for games.
    async fn walk(
        &mut self,
        number: BlockNumber,
        from: B256,
        cancel: &CancellationToken,
    ) -> Result<(), Stop> {
        let first_walk = self.blocks.is_empty();
        // The headers down to the newest block known, if the head builds on it.
        let gap = self
            .blocks
            .last_key_value()
            .map(|(top, _)| number.saturating_sub(*top));
        let mut limit = gap.unwrap_or(HEADER_BATCH).clamp(1, HEADER_BATCH);
        let (mut next, mut walked) = (from, 0_usize);
        while walked < MAX_BACKFILL_BLOCKS {
            let headers = self.fetcher.headers(next, limit, cancel).await?;
            limit = HEADER_BATCH;
            for block in headers {
                let at = block.number;
                // Another block was known at this height: it was replaced, with its game.
                if self.blocks.insert(at, block.hash()).is_some() {
                    self.found.remove(&at);
                }
                next = block.parent_hash;
                walked = walked.saturating_add(1);
                if let Some(game) = self.scan(&block, cancel).await? {
                    info!(
                        l1_block = at,
                        game = %game.game,
                        game_type = game.game_type,
                        l2_block = game.l2_block,
                        "dispute game found on L1"
                    );
                    self.found.insert(at, game);
                    // After a start: enough is known once a game old enough to be finalized
                    // has been found.
                    if first_walk && at.saturating_add(FINALITY_MARGIN_BLOCKS) <= number {
                        return Ok(());
                    }
                }
                // The parent is known: the walk has reached the chain walked before.
                let connected = at
                    .checked_sub(1)
                    .is_none_or(|parent| self.blocks.get(&parent) == Some(&next));
                if connected {
                    return Ok(());
                }
            }
        }
        if !first_walk {
            warn!(
                walked,
                "an L1 head does not connect to the blocks known within the walk limit; \
                 games in between are not seen"
            );
        }
        Ok(())
    }

    /// Looks for the newest game the factory created in `block`.
    async fn scan(
        &self,
        block: &Sealed<Header>,
        cancel: &CancellationToken,
    ) -> Result<Option<VerifiedGame>, Stop> {
        // The bloom has no false negatives: most blocks end here.
        if !block.logs_bloom.contains(&self.needle) {
            return Ok(None);
        }
        let transactions = self.fetcher.transactions(block, cancel).await?;
        let receipts = self
            .fetcher
            .receipts(block, transactions.len(), cancel)
            .await?;
        let l1_block = BlockRef {
            number: block.number,
            hash: block.hash(),
        };
        let factory = self.chain.dispute_game_factory;
        let mut newest = None;
        for (transaction, receipt) in transactions.iter().zip(&receipts) {
            let created = receipt
                .logs()
                .iter()
                .filter(|log| log.address == factory)
                .filter_map(|log| CreatedGame::from_topics(log.topics()));
            for created in created {
                match self.game(l1_block, transaction, &created) {
                    Ok(game) => newest = Some(game),
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
        if newest.is_none() {
            debug!(
                l1_block = l1_block.number,
                "the bloom matched, but the block has no game"
            );
        }
        Ok(newest)
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

    /// Forgets the blocks above `number` and their games.
    fn forget_above(&mut self, number: BlockNumber) {
        let above = number.saturating_add(1);
        drop(self.blocks.split_off(&above));
        drop(self.found.split_off(&above));
    }

    /// Drops what is no longer needed: blocks below the finalized one (or beyond the tracking
    /// limit), and every finalized game but the newest.
    fn prune(&mut self) {
        let Some((&top, _)) = self.blocks.last_key_value() else {
            return;
        };
        let floor = self
            .finalized
            .unwrap_or(0)
            .max(top.saturating_sub(MAX_TRACKED_BLOCKS));
        self.blocks = self.blocks.split_off(&floor);
        if let Some(newest_final) = self.newest_finalized().map(|game| game.l1_block.number) {
            self.found = self.found.split_off(&newest_final);
        }
    }

    /// The newest game in a finalized block.
    fn newest_finalized(&self) -> Option<&VerifiedGame> {
        let finalized = self.finalized?;
        self.found
            .range(..=finalized)
            .next_back()
            .map(|(_, game)| game)
    }

    /// Publishes the games, if they changed.
    fn publish(&self) {
        let games = L1Games {
            newest: self.found.last_key_value().map(|(_, game)| *game),
            finalized: self.newest_finalized().copied(),
        };
        self.games.send_if_modified(|current| {
            let changed = *current != games;
            *current = games;
            changed
        });
    }
}
