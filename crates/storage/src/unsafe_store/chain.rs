//! The unsafe chain's state and fork choice (docs/storage.md section 3.2): every stored block,
//! the canonical chain by height, the head and the L1 heads. The rules are the Lua scripts' of
//! the Redis store this replaced, step for step, so reorgs, fills and gaps come out the same.
//!
//! Pure state: no I/O, no locking, no events stream. The store owns one behind a lock, and
//! journals what changes.

use std::collections::{BTreeMap, HashMap};

use alloy_primitives::{Address, BlockHash, BlockNumber, Bytes};
use op_indexer_primitives::{BlockRef, BlockSource, EncodedBlock, L1Heads, Reorg, UnsafeEvent};

use super::layout::{MAX_REORG_DEPTH, RETENTION_HEIGHTS_PER_INSERT, RETENTION_SECS};
use crate::InvalidBlockReason;

/// Bytes a stored block costs besides its encodings and senders: its map entries and fields.
const BLOCK_OVERHEAD_BYTES: usize = 256;

/// One stored block, in its consensus encoding.
#[derive(Debug, Clone)]
pub(super) struct Stored {
    pub(super) number: BlockNumber,
    pub(super) parent_hash: BlockHash,
    pub(super) timestamp: u64,
    /// Header, body and receipts (`None` until they are attached).
    pub(super) encoded: EncodedBlock,
    pub(super) senders: Vec<Address>,
    pub(super) source: BlockSource,
    /// Insertion order: the journal's key, by which a restart replays the blocks.
    pub(super) seq: u64,
}

impl Stored {
    /// What the block takes in memory, roughly.
    pub(super) fn bytes(&self) -> usize {
        let encoded = &self.encoded;
        let receipts = encoded
            .receipts
            .as_ref()
            .map_or(0, |receipts| receipts.len());
        encoded.header.len()
            + encoded.body.len()
            + receipts
            + self.senders.len() * Address::len_bytes()
            + BLOCK_OVERHEAD_BYTES
    }

    fn block_ref(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.encoded.hash,
        }
    }
}

/// What an insert did.
#[derive(Debug)]
pub(super) enum Inserted {
    /// Already stored, or at or below the safe head: nothing changed.
    Skipped,
    /// Stored, with the events fork choice emitted.
    Stored(Vec<UnsafeEvent>),
}

/// The unsafe chain.
#[derive(Debug, Default)]
pub(super) struct Chain {
    blocks: HashMap<BlockHash, Stored>,
    /// The hashes stored at each height.
    heights: BTreeMap<BlockNumber, Vec<BlockHash>>,
    /// At most one hash per height.
    canonical: BTreeMap<BlockNumber, BlockHash>,
    /// Can outlive its block, which a prune may remove.
    head: Option<BlockRef>,
    heads: L1Heads,
    /// What the stored blocks take, roughly.
    bytes: usize,
    /// The newest block timestamp seen: retention counts back from it.
    newest_timestamp: u64,
}

impl Chain {
    pub(super) fn head(&self) -> Option<BlockRef> {
        self.head
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn len(&self) -> usize {
        self.blocks.len()
    }

    pub(super) fn get(&self, hash: &BlockHash) -> Option<&Stored> {
        self.blocks.get(hash)
    }

    /// The canonical hash at `number`.
    pub(super) fn canonical_at(&self, number: BlockNumber) -> Option<BlockHash> {
        self.canonical.get(&number).copied()
    }

    /// The lowest height with a canonical block.
    pub(super) fn lowest(&self) -> Option<BlockNumber> {
        self.canonical.keys().next().copied()
    }

    /// The canonical entries from `from` on, rising or falling.
    pub(super) fn canonical_from(
        &self,
        from: BlockNumber,
        rising: bool,
    ) -> Box<dyn Iterator<Item = (BlockNumber, BlockHash)> + '_> {
        if rising {
            Box::new(self.canonical.range(from..).map(|(n, h)| (*n, *h)))
        } else {
            Box::new(self.canonical.range(..=from).rev().map(|(n, h)| (*n, *h)))
        }
    }

    /// The stored canonical block with `hash`, if it is canonical.
    pub(super) fn canonical_block(&self, hash: &BlockHash) -> Option<&Stored> {
        self.blocks
            .get(hash)
            .filter(|block| self.canonical.get(&block.number) == Some(hash))
    }

    /// Records the L1 heads; a `None` head leaves the one recorded.
    pub(super) fn set_heads(&mut self, heads: L1Heads) {
        self.heads.safe = heads.safe.or(self.heads.safe);
        self.heads.finalized = heads.finalized.or(self.heads.finalized);
    }

    pub(super) const fn heads(&self) -> L1Heads {
        self.heads
    }

    /// Stores `block` and applies fork choice.
    ///
    /// # Errors
    ///
    /// [`InvalidBlockReason::ParentNumber`] if its number is not its parent's plus one.
    pub(super) fn insert(&mut self, block: Stored) -> Result<Inserted, InvalidBlockReason> {
        let (number, hash, parent_hash) = (block.number, block.encoded.hash, block.parent_hash);
        if self.blocks.contains_key(&hash) || self.le_safe(Some(number)) {
            return Ok(Inserted::Skipped);
        }
        // The parent's number is known if it is stored, or is the head (which can outlive its
        // block). A block that is not exactly one above it is refused.
        let parent = self
            .blocks
            .get(&parent_hash)
            .map(|parent| parent.number)
            .or_else(|| {
                self.head
                    .filter(|head| head.hash == parent_hash)
                    .map(|head| head.number)
            });
        if parent.is_some_and(|parent| parent.checked_add(1) != Some(number)) {
            return Err(InvalidBlockReason::ParentNumber);
        }
        self.newest_timestamp = self.newest_timestamp.max(block.timestamp);
        self.bytes = self.bytes.saturating_add(block.bytes());
        self.heights.entry(number).or_default().push(hash);
        self.blocks.insert(hash, block);

        let mut events = Vec::new();
        self.fork_choice(&mut events, number, hash, parent_hash);
        Ok(Inserted::Stored(events))
    }

    /// Steps 4 to 8: decides whether the stored block becomes canonical.
    fn fork_choice(
        &mut self,
        events: &mut Vec<UnsafeEvent>,
        number: BlockNumber,
        hash: BlockHash,
        parent_hash: BlockHash,
    ) {
        let new = BlockRef { number, hash };
        match self.head {
            None => self.move_head(events, new, false),
            Some(head) if parent_hash == head.hash => self.move_head(events, new, false),
            Some(head) if number >= head.number.saturating_add(2) => {
                self.jump(events, head, new, parent_hash);
            }
            Some(head) if number <= head.number && !self.canonical.contains_key(&number) => {
                let fills = number.checked_add(1).is_some_and(|above| {
                    self.canonical_at(above)
                        .and_then(|above_hash| self.parent_at(above_hash, Some(above)))
                        == Some(hash)
                });
                if fills {
                    self.fill(events, head, new, parent_hash);
                }
            }
            Some(_) => {}
        }
    }

    /// Makes `new` the head and emits `head`.
    fn move_head(&mut self, events: &mut Vec<UnsafeEvent>, new: BlockRef, gap: bool) {
        self.canonical.insert(new.number, new.hash);
        self.head = Some(new);
        events.push(UnsafeEvent::NewHead { head: new, gap });
    }

    /// Step 6: `new` is two or more heights above the head and becomes the head. The stored
    /// part of its ancestry becomes canonical with it, replacing whatever else held those
    /// heights; heights it cannot account for stay a gap. Nothing at or below the safe height
    /// is touched.
    fn jump(
        &mut self,
        events: &mut Vec<UnsafeEvent>,
        head: BlockRef,
        new: BlockRef,
        parent_hash: BlockHash,
    ) {
        // Stored ancestors of the new block, highest first. `height` is `None` below block 0.
        let mut path = Vec::new();
        let (mut cursor, mut height) = (parent_hash, new.number.checked_sub(1));
        for _ in 0..MAX_REORG_DEPTH {
            let below = height.and_then(|h| self.canonical_at(h));
            if self.is_ancestor(cursor, height, below) || self.le_safe(height) {
                break;
            }
            let Some(parent) = self.parent_at(cursor, height) else {
                break;
            };
            let Some(h) = height else { break };
            path.push(BlockRef {
                number: h,
                hash: cursor,
            });
            cursor = parent;
            height = h.checked_sub(1);
        }
        let below = height.and_then(|h| self.canonical_at(h));
        let ancestor = self
            .is_ancestor(cursor, height, below)
            .then(|| {
                height.map(|number| BlockRef {
                    number,
                    hash: cursor,
                })
            })
            .flatten();
        // The walk reached the safe height without meeting the chain: the block above it does
        // not build on what L1 committed, whether or not the safe block is still stored.
        let contradicts_l1 = ancestor.is_none() && self.le_safe(height);
        if contradicts_l1 && path.is_empty() {
            // That block is the new one itself: it stays a side block.
            return;
        }

        let first_replaced = height.map_or(0, |h| h.saturating_add(1));
        let mut replaced: Vec<BlockHash> = self
            .canonical
            .split_off(&first_replaced)
            .into_values()
            .rev()
            .collect();
        if contradicts_l1 {
            // The lowest block of the path stays out; its height is a gap.
            path.pop();
        } else if ancestor.is_none()
            && let (Some(below), Some(h)) = (below, height)
        {
            // The path ends at a block whose parent is unknown, so the entry below it belongs
            // to another branch.
            replaced.push(below);
            self.canonical.remove(&h);
        }
        for block in &path {
            self.canonical.insert(block.number, block.hash);
        }
        if !replaced.is_empty() {
            events.push(UnsafeEvent::Reorg(Reorg {
                common_ancestor: ancestor,
                old_head: head,
                new_head: new,
                replaced,
            }));
        }
        self.move_head(events, new, ancestor.is_none());
    }

    /// Step 7: `new` closes a gap below the head. Fills downward through stored parents,
    /// replacing or removing entries that turn out to belong to another branch, but never one
    /// at or below the safe height. Wherever it stops, the entry below the last block it wrote
    /// is that block's parent or absent.
    fn fill(
        &mut self,
        events: &mut Vec<UnsafeEvent>,
        head: BlockRef,
        new: BlockRef,
        parent_hash: BlockHash,
    ) {
        let mut replaced = Vec::new();
        let mut ancestor = None;
        // The block to make canonical next, its height, its parent, and the entry it replaces.
        let (mut block, mut height, mut parent) = (new.hash, new.number, parent_hash);
        let mut previous: Option<BlockHash> = None;
        for step in 1..=MAX_REORG_DEPTH {
            let below_height = height.checked_sub(1);
            let below = below_height.and_then(|h| self.canonical_at(h));
            let committed = self.le_safe(below_height);
            if let Some(previous) = previous {
                replaced.push(previous);
                self.canonical.remove(&height);
            }
            // A block whose parent is not what L1 committed stays out: its height is a gap.
            // That holds whether or not the safe block is still stored.
            let parent_is_ancestor = self.is_ancestor(parent, below_height, below);
            if committed && !parent_is_ancestor {
                break;
            }
            self.canonical.insert(height, block);
            events.push(UnsafeEvent::Filled(BlockRef {
                number: height,
                hash: block,
            }));
            if parent_is_ancestor {
                ancestor = below_height.map(|number| BlockRef {
                    number,
                    hash: parent,
                });
                break;
            }
            // At the last step nothing more is written, so no entry is left unchecked against
            // the one below it.
            let grandparent = (!committed && step < MAX_REORG_DEPTH)
                .then(|| self.parent_at(parent, below_height))
                .flatten();
            let (Some(grandparent), Some(below_height)) = (grandparent, below_height) else {
                if let (Some(below), Some(h)) = (below, below_height) {
                    replaced.push(below);
                    self.canonical.remove(&h);
                }
                break;
            };
            (block, height, parent, previous) = (parent, below_height, grandparent, below);
        }
        if !replaced.is_empty() {
            events.push(UnsafeEvent::Reorg(Reorg {
                common_ancestor: ancestor,
                old_head: head,
                new_head: head,
                replaced,
            }));
        }
    }

    /// Whether the block `hash` at `height` is where a walk meets the canonical chain: it is
    /// `canonical`, the entry at that height, or the height has no entry and it is the safe
    /// head, which stays the ancestor after it has been pruned.
    fn is_ancestor(
        &self,
        hash: BlockHash,
        height: Option<BlockNumber>,
        canonical: Option<BlockHash>,
    ) -> bool {
        canonical == Some(hash)
            || (canonical.is_none()
                && self
                    .heads
                    .safe
                    .is_some_and(|safe| height == Some(safe.number) && hash == safe.hash))
    }

    /// The parent hash of `hash` if it is stored at `height`: walks stop at a block that is
    /// missing or whose stored number is not the height they computed.
    fn parent_at(&self, hash: BlockHash, height: Option<BlockNumber>) -> Option<BlockHash> {
        self.blocks
            .get(&hash)
            .filter(|block| Some(block.number) == height)
            .map(|block| block.parent_hash)
    }

    /// Whether `height` is at or below the safe head; below block 0 (`None`) always is, as
    /// height -1 is at or below the Lua scripts' "no safe head" of -1.
    fn le_safe(&self, height: Option<BlockNumber>) -> bool {
        match (height, self.heads.safe) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(height), Some(safe)) => height <= safe.number,
        }
    }

    /// Attaches `receipts` to block `block`.
    ///
    /// # Errors
    ///
    /// [`InvalidBlockReason::StoredNumber`] if it is stored under another number.
    pub(super) fn set_receipts(
        &mut self,
        block: BlockRef,
        receipts: Bytes,
    ) -> Result<Option<UnsafeEvent>, InvalidBlockReason> {
        let Some(stored) = self.blocks.get_mut(&block.hash) else {
            return Ok(None);
        };
        if stored.number != block.number {
            return Err(InvalidBlockReason::StoredNumber);
        }
        let before = stored.bytes();
        stored.encoded.receipts = Some(receipts);
        let after = stored.bytes();
        self.bytes = self.bytes.saturating_sub(before).saturating_add(after);
        Ok(Some(UnsafeEvent::Receipts(block)))
    }

    /// Removes every block at or below `up_to`, canonical and side blocks. Returns their
    /// insertion order, the journal's key.
    pub(super) fn prune(&mut self, up_to: BlockNumber) -> Vec<u64> {
        let kept = self.heights.split_off(&up_to.saturating_add(1));
        let removed = std::mem::replace(&mut self.heights, kept);
        let kept = self.canonical.split_off(&up_to.saturating_add(1));
        self.canonical = kept;
        self.remove(removed.into_values().flatten())
    }

    /// Removes the lowest heights, the canonical entries with them, while their blocks are
    /// older than [`RETENTION_SECS`] before the newest block (a few per call, as the Redis
    /// store's keys expired), then while the blocks take more than `max_bytes`, but never the
    /// head's height. Returns the insertion order of the blocks removed.
    pub(super) fn retain(&mut self, max_bytes: usize) -> Vec<u64> {
        let horizon = self.newest_timestamp.saturating_sub(RETENTION_SECS);
        let mut removed = Vec::new();
        for _ in 0..RETENTION_HEIGHTS_PER_INSERT {
            let expired = self.lowest_height().is_some_and(|(_, hashes)| {
                hashes
                    .iter()
                    .filter_map(|hash| self.blocks.get(hash))
                    .all(|block| block.timestamp < horizon)
            });
            if !expired {
                break;
            }
            removed.extend(self.pop_lowest_height());
        }
        while self.bytes > max_bytes
            && self
                .lowest_height()
                .is_some_and(|(number, _)| Some(number) != self.head.map(|head| head.number))
        {
            removed.extend(self.pop_lowest_height());
        }
        removed
    }

    fn lowest_height(&self) -> Option<(BlockNumber, &Vec<BlockHash>)> {
        self.heights
            .first_key_value()
            .map(|(n, hashes)| (*n, hashes))
    }

    fn pop_lowest_height(&mut self) -> Vec<u64> {
        let Some((number, hashes)) = self.heights.pop_first() else {
            return Vec::new();
        };
        self.canonical.remove(&number);
        self.remove(hashes)
    }

    fn remove(&mut self, hashes: impl IntoIterator<Item = BlockHash>) -> Vec<u64> {
        let mut removed = Vec::new();
        for block in hashes
            .into_iter()
            .filter_map(|hash| self.blocks.remove(&hash))
        {
            self.bytes = self.bytes.saturating_sub(block.bytes());
            removed.push(block.seq);
        }
        removed
    }

    /// The canonical run above `above`: see [`crate::UnsafeStore::canonical_run`].
    pub(super) fn canonical_run(&self, above: BlockRef, max: usize) -> Option<BlockRef> {
        let mut tip = above;
        let mut last = None;
        for (number, hash) in self
            .canonical
            .range(above.number.saturating_add(1)..)
            .take(max)
        {
            let Some(block) = self.blocks.get(hash) else {
                break;
            };
            let continues =
                Some(*number) == tip.number.checked_add(1) && block.parent_hash == tip.hash;
            if !continues || block.encoded.receipts.is_none() {
                break;
            }
            tip = block.block_ref();
            last = Some(tip);
        }
        last
    }
}
