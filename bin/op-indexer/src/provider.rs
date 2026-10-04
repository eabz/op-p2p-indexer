//! The blocks this node serves to execution peers, behind the execution network's
//! [`BlockProvider`]: the local block archive for committed blocks, then the unsafe store for
//! the canonical blocks above the archive's tip, which are what peers syncing the tip need.
//!
//! Archived blocks are returned in the encoding they were received in. Unsafe blocks are
//! encoded by the unsafe store from the stored gossip block, which gives its original bytes:
//! its transactions are signed, so they survive the decoding.
//!
//! **One chain per answer.** The unsafe store reads a run in two round trips and checks it by
//! parent hash; this module links it to the archive's tip. A run ends at the first block that
//! does not link, so an answer never holds a block of the old branch after one of the new. A
//! block found by hash is served only while it is canonical; an unsafe block's receipts only
//! once they are attached. Headers every `step` blocks cannot be linked, so they come from the
//! archive only.
//!
//! **The range** advertised is the archive's first block up to the end of the unbroken run of
//! canonical unsafe blocks above its tip that have their receipts: devp2p eth/69 promises
//! bodies and receipts for every block of it (`BlockRangeUpdate`).

use std::sync::{Arc, Mutex, PoisonError};

use alloy_primitives::{BlockHash, BlockNumber, Bytes, keccak256};
use op_indexer_el::BlockProvider;
use op_indexer_p2p::{BlockFuture, PayloadSource};
use op_indexer_primitives::{
    BlockRead, BlockRef, BlockStart, EncodedBlock, ItemConvert, ReadLimits,
};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::unsafe_store::RedisStore;
use op_indexer_storage::{ArchiveStore, BlockPart, CanonicalItem, StorageError, UnsafeStore};

/// Most unsafe heights looked at, past the last known end, to find the end of the advertised
/// range: past this the range is advertised shorter than what is held, which promises nothing
/// that is not.
const MAX_RANGE_SCAN: usize = 16_384;

/// One item, of any size.
const ONE: ReadLimits = ReadLimits {
    items: 1,
    bytes: usize::MAX,
    lowest: 0,
};

/// Bodies or receipts read from the unsafe store at once for one request: a few blocks' worth,
/// so an answer the byte limit ends early reads and root-checks little more than it sends.
const UNSAFE_ITEMS_PER_READ: usize = 16;

/// The blocks this node holds, as the execution network reads them.
#[derive(Debug, Clone)]
pub(crate) struct NodeProvider {
    archive: FjallArchive,
    unsafe_store: RedisStore,
    /// The end of the advertised range found last, from which the next search continues: the
    /// range is read again on every new head, and the run below a block that is still
    /// canonical has not changed (a reorg would have replaced it) nor lost its receipts.
    range_end: Arc<Mutex<Option<BlockRef>>>,
}

/// The items of one answer, and where it must end.
struct Answer {
    items: Vec<Bytes>,
    bytes: usize,
    limits: ReadLimits,
    convert: Option<ItemConvert>,
}

impl NodeProvider {
    pub(crate) fn new(archive: FjallArchive, unsafe_store: RedisStore) -> Self {
        Self {
            archive,
            unsafe_store,
            range_end: Arc::default(),
        }
    }

    /// The archive's last block.
    async fn archive_tip(&self) -> Result<Option<BlockRef>, StorageError> {
        Ok(self.archive.range().await?.map(|(_, tip)| tip))
    }

    /// A run of headers: from the archive while it holds them, then, for consecutive headers,
    /// from the unsafe store, linked to the archive's tip.
    async fn headers(
        &self,
        start: BlockStart,
        step: u64,
        rising: bool,
        mut answer: Answer,
    ) -> Result<Vec<Bytes>, StorageError> {
        let archive_read = |start| BlockRead::Headers {
            start,
            step,
            rising,
        };
        if step != 1 {
            return self
                .archive
                .read(archive_read(start), answer.limits, answer.convert)
                .await;
        }
        let tip = self.archive_tip().await?;
        // With the hash asked for when the unsafe store gave its number: a reorg before the
        // headers are read can put another block there, which is then not served.
        let (start, unsafe_hash) = match start {
            BlockStart::Number(number) => (number, None),
            BlockStart::Hash(hash) => match self.archive.number_of(hash).await? {
                Some(number) => (number, None),
                None => match self.unsafe_store.canonical_number(hash).await? {
                    Some(number) => (number, Some(hash)),
                    None => return Ok(answer.items),
                },
            },
        };
        let in_archive = tip.is_some_and(|tip| start <= tip.number);
        // Promoted between the two reads: the archive's block there is not checked against it.
        if unsafe_hash.is_some() && in_archive {
            return Ok(answer.items);
        }
        let starts_at_hash = |run: &[CanonicalItem]| {
            unsafe_hash.is_none_or(|hash| run.first().is_some_and(|first| first.block.hash == hash))
        };
        if rising {
            // The archive's part, then the unsafe store's from the block after its tip.
            let mut next = start;
            if in_archive {
                let archived = self
                    .archive
                    .read(
                        archive_read(BlockStart::Number(start)),
                        answer.limits,
                        answer.convert,
                    )
                    .await?;
                let count = u64::try_from(archived.len()).unwrap_or(u64::MAX);
                next = start.saturating_add(count);
                answer.extend(archived);
                // Ended before the tip: limits, `lowest` or a refused conversion.
                if tip.is_none_or(|tip| next != tip.number.saturating_add(1)) || answer.is_full() {
                    return Ok(answer.items);
                }
            }
            let run = self
                .unsafe_store
                .canonical_headers(next, answer.remaining().items, true)
                .await?;
            if !starts_at_hash(&run) {
                return Ok(answer.items);
            }
            // Linked to the archive when it starts right above its tip.
            let parent = tip.filter(|tip| next == tip.number.saturating_add(1));
            answer.push_run(run, parent.map(|tip| tip.hash), true);
            return Ok(answer.items);
        }
        // Falling: the unsafe store's part first, down to the block above the archive's tip,
        // then the archive's, if the last one names the tip as its parent.
        if !in_archive {
            let Some(tip) = tip else {
                return Ok(answer.items);
            };
            let count = usize::try_from(start.saturating_sub(tip.number))
                .unwrap_or(usize::MAX)
                .min(answer.remaining().items);
            let run = self
                .unsafe_store
                .canonical_headers(start, count, false)
                .await?;
            if !starts_at_hash(&run) {
                return Ok(answer.items);
            }
            let last = answer.push_run(run, None, false);
            let linked = last.is_some_and(|last| {
                last.block.number == tip.number.saturating_add(1) && last.parent_hash == tip.hash
            });
            if !linked || answer.is_full() {
                return Ok(answer.items);
            }
        }
        let from = match tip {
            Some(tip) if !in_archive => tip.number,
            _ => start,
        };
        let archived = self
            .archive
            .read(
                archive_read(BlockStart::Number(from)),
                answer.remaining(),
                answer.convert,
            )
            .await?;
        answer.extend(archived);
        Ok(answer.items)
    }

    /// The bodies or receipts of the blocks with `hashes`, in order: from the archive while it
    /// holds them, then from the unsafe store, ending at the first block not served.
    async fn by_hash(
        &self,
        hashes: Vec<BlockHash>,
        part: BlockPart,
        mut answer: Answer,
    ) -> Result<Vec<Bytes>, StorageError> {
        let read = match part {
            BlockPart::Body => BlockRead::Bodies(hashes),
            BlockPart::Receipts => BlockRead::Receipts(hashes),
        };
        let archived = self
            .archive
            .read(read.clone(), answer.limits, answer.convert)
            .await?;
        let served = archived.len();
        answer.extend(archived);
        let (BlockRead::Bodies(hashes) | BlockRead::Receipts(hashes)) = read else {
            return Ok(answer.items);
        };
        let rest = hashes.get(served..).unwrap_or_default();
        if rest.is_empty() || answer.is_full() {
            return Ok(answer.items);
        }
        let take = rest.len().min(answer.remaining().items);
        // A block right above the one before it (the archive's tip first) must name it as its
        // parent.
        let mut previous = self.archive_tip().await?;
        // In small reads, so the byte limit ends the answer before more is read and checked.
        for hashes in rest
            .get(..take)
            .unwrap_or_default()
            .chunks(UNSAFE_ITEMS_PER_READ)
        {
            let run = self.unsafe_store.canonical_items(hashes, part).await?;
            let complete = run.len() == hashes.len();
            for item in run {
                let breaks = previous.is_some_and(|previous| {
                    item.block.number == previous.number.saturating_add(1)
                        && item.parent_hash != previous.hash
                });
                if breaks || item.block.number < answer.limits.lowest || !answer.push(item.rlp) {
                    return Ok(answer.items);
                }
                previous = Some(item.block);
            }
            if !complete || answer.is_full() {
                break;
            }
        }
        Ok(answer.items)
    }
}

impl Answer {
    const fn new(limits: ReadLimits, convert: Option<ItemConvert>) -> Self {
        Self {
            items: Vec::new(),
            bytes: 0,
            limits,
            convert,
        }
    }

    /// Whether no item may be added.
    fn is_full(&self) -> bool {
        self.items.len() >= self.limits.items || self.bytes >= self.limits.bytes
    }

    /// The limits left for a read that continues this answer.
    fn remaining(&self) -> ReadLimits {
        ReadLimits {
            items: self.limits.items.saturating_sub(self.items.len()),
            bytes: self.limits.bytes.saturating_sub(self.bytes),
            lowest: self.limits.lowest,
        }
    }

    /// Adds items a read already converted and limited.
    fn extend(&mut self, items: Vec<Bytes>) {
        self.bytes = items
            .iter()
            .fold(self.bytes, |bytes, item| bytes.saturating_add(item.len()));
        self.items.extend(items);
    }

    /// Converts `raw` if asked and adds it. Returns whether the answer may take another item:
    /// within the limits, and the conversion did not refuse.
    fn push(&mut self, raw: Bytes) -> bool {
        if self.is_full() {
            return false;
        }
        let item = match self.convert {
            Some(convert) => match convert(&raw) {
                Some(item) => item,
                None => return false,
            },
            None => raw,
        };
        self.bytes = self.bytes.saturating_add(item.len());
        self.items.push(item);
        !self.is_full()
    }

    /// Adds a run of unsafe headers (linked among themselves by the store), the first of which
    /// must name `parent` when it is given (rising only), until the limits or `lowest`. Returns
    /// the last item added.
    fn push_run(
        &mut self,
        run: Vec<CanonicalItem>,
        parent: Option<BlockHash>,
        rising: bool,
    ) -> Option<CanonicalItem> {
        let starts_linked = run
            .first()
            .is_none_or(|first| !rising || parent.is_none_or(|parent| first.parent_hash == parent));
        let mut last = None;
        if !starts_linked {
            return last;
        }
        for item in run {
            if item.block.number < self.limits.lowest {
                break;
            }
            let more = self.push(item.rlp.clone());
            last = Some(item);
            if !more {
                break;
            }
        }
        last
    }
}

impl BlockProvider for NodeProvider {
    type Error = StorageError;

    async fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> Result<Vec<Bytes>, StorageError> {
        let answer = Answer::new(limits, convert);
        match read {
            BlockRead::Headers {
                start,
                step,
                rising,
            } => self.headers(start, step, rising, answer).await,
            BlockRead::Bodies(hashes) => self.by_hash(hashes, BlockPart::Body, answer).await,
            BlockRead::Receipts(hashes) => self.by_hash(hashes, BlockPart::Receipts, answer).await,
        }
    }

    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError> {
        let Some((first, tip)) = self.archive.range().await? else {
            return Ok(None);
        };
        // Every advertised block must have its receipts (eth/69): an archived block still
        // waiting for them, which the receipts task fills within minutes, ends the range below.
        let (waiting, _) = self.archive.pending_receipts(first.number, 1).await?;
        if let Some(waiting) = waiting.first().filter(|block| block.number <= tip.number) {
            let Some(latest) = waiting.number.checked_sub(1).filter(|n| *n >= first.number) else {
                return Ok(None);
            };
            let header = self.archive.read(header_at(latest), ONE, None).await?.pop();
            return Ok(header.map(|header| {
                let hash = keccak256(&header);
                (
                    first,
                    BlockRef {
                        number: latest,
                        hash,
                    },
                )
            }));
        }
        let known = *self
            .range_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Continue from the last end while it is above the tip and still canonical.
        let from = match known.filter(|known| known.number > tip.number) {
            Some(known)
                if self.unsafe_store.canonical_number(known.hash).await? == Some(known.number) =>
            {
                known
            }
            _ => tip,
        };
        let end = self
            .unsafe_store
            .canonical_run(from, MAX_RANGE_SCAN)
            .await?
            .unwrap_or(from);
        *self
            .range_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(end);
        Ok(Some((first, end)))
    }
}

impl NodeProvider {
    /// The canonical block at `number`, header and body only (no receipts): the archive's
    /// when it holds it, else the unsafe store's canonical block there. Neither decompresses
    /// receipts nor decodes senders or receipts.
    async fn canonical_block(
        &self,
        number: BlockNumber,
    ) -> Result<Option<EncodedBlock>, StorageError> {
        if let Some(header) = self.archive.read(header_at(number), ONE, None).await?.pop() {
            // The archive checked that the header hashes to the block's hash when it stored it.
            let hash = keccak256(&header);
            let body = self
                .archive
                .read(BlockRead::Bodies(vec![hash]), ONE, None)
                .await?;
            // A header without its body: not held.
            return Ok(body.into_iter().next().map(|body| EncodedBlock {
                hash,
                header,
                body,
                receipts: None,
            }));
        }
        let Some(header) = self
            .unsafe_store
            .canonical_headers(number, 1, true)
            .await?
            .pop()
        else {
            return Ok(None);
        };
        let hash = header.block.hash;
        let body = self
            .unsafe_store
            .canonical_items(&[hash], BlockPart::Body)
            .await?
            .pop();
        Ok(body.map(|body| EncodedBlock {
            hash,
            header: header.rlp,
            body: body.rlp,
            receipts: None,
        }))
    }
}

/// The consensus layer's `payload_by_number` server reads blocks by number through this.
impl PayloadSource for NodeProvider {
    fn canonical_block(&self, number: BlockNumber) -> BlockFuture<'_> {
        Box::pin(async move {
            Self::canonical_block(self, number)
                .await
                .map_err(Into::into)
        })
    }
}

/// The archived header at `number`, alone.
const fn header_at(number: BlockNumber) -> BlockRead {
    BlockRead::Headers {
        start: BlockStart::Number(number),
        step: 1,
        rising: true,
    }
}
