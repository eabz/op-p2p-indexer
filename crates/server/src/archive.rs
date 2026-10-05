//! [`R2Archive`]: the committed store of a server (D13).
//!
//! The sealed range, from the first listed chunk to the last, is read from the
//! [`ChunkSource`]; the tail, a [`FjallArchive`] of the committed blocks above the last sealed
//! chunk, holds the rest and takes every write. Blocks leave the tail once a listed chunk
//! covers them.
//!
//! Reads:
//! - a read that starts in the tail goes to the tail as it is;
//! - otherwise blocks are read one after the other through a [`Cursor`], which keeps one
//!   chunk stream open while the numbers asked for follow each other, and opens another
//!   (from the block's segment) when they do not;
//! - a peer's read (`read`) that needs R2 takes a place in the [`PeerBudget`] first, and is
//!   answered empty without one;
//! - a consumer owns its [`ArchiveStore::read_range`] and bounded read-ahead.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::Header;
use alloy_primitives::{BlockHash, BlockNumber, Bytes};
use alloy_rlp::Decodable;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_chainspec::ChainSpec;
use op_indexer_chunks::StreamReads;
use op_indexer_primitives::{
    ArchivedBlock, BlockRead, BlockRef, BlockStart, ItemConvert, L1Heads, ReadLimits, ReadParts,
};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::{ArchiveRange, ArchiveStore, StorageError, Store};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::ServerError;
use crate::budget::PeerBudget;
use crate::feed::Feeds;
use crate::source::{ChunkRange, ChunkSource};

/// How often the manifest is read again for chunks the exporter sealed.
const MANIFEST_REFRESH: Duration = Duration::from_secs(30);

/// Headers a spaced read (a skeleton: every n-th header) answers from R2, and how many of them
/// are read at once. Each is in its own segment, often its own chunk: one index GET and one
/// segment GET apiece, so a full page of 1,024 would take minutes; a peer takes a shorter page
/// and asks again from its end.
const MAX_SPACED_HEADERS: usize = 64;
const SPACED_READS: usize = 16;
/// How a spaced header is read: its own segment and nothing more, without its receipts.
const ONE_SEGMENT: StreamReads = StreamReads {
    range_bytes: 1,
    in_flight: 1,
    lend: None,
    parts: ReadParts::WithoutReceipts,
};

/// One item, of any size.
const ONE: ReadLimits = ReadLimits {
    items: 1,
    bytes: usize::MAX,
    lowest: 0,
};

/// The committed store of a server: sealed history from a [`ChunkSource`], a local tail
/// above it.
#[derive(Debug, Clone)]
pub struct R2Archive<S> {
    chain: &'static ChainSpec,
    source: S,
    tail: FjallArchive,
    /// The first sealed block, which the manifest does not name; chunks are only appended.
    first: Option<BlockRef>,
    budget: PeerBudget,
    feeds: Feeds,
}

/// The sealed range as the source last read it.
#[derive(Debug, Clone)]
pub(crate) struct Sealed {
    pub(crate) chunks: Arc<[ChunkRange]>,
    first: Option<BlockRef>,
}

impl Sealed {
    /// The last sealed block.
    pub(crate) fn last(&self) -> Option<BlockRef> {
        self.chunks.last().map(|chunk| BlockRef {
            number: chunk.last,
            hash: chunk.last_hash,
        })
    }

    /// Whether `number` is in a sealed chunk.
    pub(crate) fn holds(&self, number: BlockNumber) -> bool {
        self.first_number()
            .zip(self.last())
            .is_some_and(|(first, last)| first <= number && number <= last.number)
    }

    /// The first sealed block's number.
    fn first_number(&self) -> Option<BlockNumber> {
        self.chunks.first().map(|chunk| chunk.first)
    }

    /// The chunk holding `number`.
    pub(crate) fn find(&self, number: BlockNumber) -> Option<&ChunkRange> {
        let at = self.chunks.partition_point(|chunk| chunk.last < number);
        self.chunks.get(at).filter(|chunk| chunk.first <= number)
    }
}

impl<S: ChunkSource> R2Archive<S> {
    /// Opens the store over `source` and `tail`: reads the first sealed block's hash, checks
    /// that the chunks link, and drops from `tail` what they already cover. Consumers' reads of
    /// sealed history hold at most `read_budget` bytes in all (see `feed`).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the first chunk cannot be read, the chunks do not link, or
    /// the tail cannot be pruned.
    pub async fn open(
        chain: &'static ChainSpec,
        source: S,
        tail: FjallArchive,
        read_budget: u64,
    ) -> Result<Self, ServerError> {
        let chunks = source.chunks();
        check_links(&chunks, None)?;
        let first = match chunks.first() {
            Some(chunk) => source
                .stream(chunk, chunk.first, crate::feed::ONE_RANGE)
                .next()
                .await
                .transpose()
                .map_err(ServerError::source("first block"))?
                .map(|block| BlockRef {
                    number: chunk.first,
                    hash: block.encoded.hash,
                }),
            None => None,
        };
        let archive = Self {
            chain,
            source,
            tail,
            first,
            budget: PeerBudget::new(),
            feeds: Feeds::new(read_budget),
        };
        let sealed = archive.sealed();
        info!(
            chunks = sealed.chunks.len(),
            first = ?sealed.first,
            last = ?sealed.last(),
            "R2 archive ready"
        );
        archive.prune_tail().await?;
        Ok(archive)
    }

    /// Follows the manifest until `cancel` fires: reads it again every
    /// 30 seconds and drops from the tail the blocks new chunks cover. A failed
    /// read is warned about and tried again next time.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if a new chunk does not continue the last one, or the tail
    /// cannot be pruned.
    pub async fn follow(self, cancel: CancellationToken) -> Result<(), ServerError> {
        let mut tick = tokio::time::interval(MANIFEST_REFRESH);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                _ = tick.tick() => {}
            }
            let before = self.sealed().chunks.last().copied();
            match self.source.refresh().await {
                Ok(added) if added.is_empty() => {}
                Ok(added) => {
                    check_links(&added, before.as_ref())?;
                    self.prune_tail().await?;
                }
                Err(err) => warn!(%err, "cannot read the R2 manifest; trying again"),
            }
        }
    }

    /// The last sealed block, as the manifest was last read.
    #[must_use]
    pub fn last_sealed(&self) -> Option<BlockNumber> {
        self.source.chunks().last().map(|chunk| chunk.last)
    }

    /// The chain this store holds.
    pub(crate) const fn chain(&self) -> &'static ChainSpec {
        self.chain
    }

    /// The chunk source.
    pub(crate) const fn source(&self) -> &S {
        &self.source
    }

    /// The local tail.
    pub(crate) const fn tail(&self) -> &FjallArchive {
        &self.tail
    }

    /// The sealed range as the source last read it.
    pub(crate) fn sealed(&self) -> Sealed {
        Sealed {
            chunks: self.source.chunks(),
            first: self.first,
        }
    }

    /// Drops from the tail every block the sealed range covers.
    pub(crate) async fn prune_tail(&self) -> Result<(), ServerError> {
        let Some(last) = self.sealed().last() else {
            return Ok(());
        };
        if let Some((first, _)) = self.tail.range().await?
            && first.number <= last.number
        {
            let first_kept = last.number.saturating_add(1);
            self.tail.prune_below(first_kept).await?;
            info!(below = first_kept, "tail pruned: sealed in R2");
        }
        Ok(())
    }

    /// The number of the block with `hash`, in the tail or the sealed range.
    async fn find(&self, hash: BlockHash) -> Result<Option<BlockNumber>, StorageError> {
        match self.tail.number_of(hash).await? {
            Some(number) => Ok(Some(number)),
            None => self
                .source
                .number_of(hash)
                .await
                .map_err(remote("hash lookup")),
        }
    }

    /// The headers of a spaced read from `start` (every `step`-th block): at most
    /// [`MAX_SPACED_HEADERS`], read [`SPACED_READS`] at once, up to the first that is not
    /// sealed.
    async fn spaced_headers(
        cursor: &mut Cursor<'_, S>,
        run: &mut Run,
        start: Option<BlockNumber>,
        (step, rising): (u64, bool),
    ) -> Result<(), StorageError> {
        let lowest = run.limits.lowest;
        let chunks: Vec<(BlockNumber, ChunkRange)> = std::iter::successors(start, |number| {
            if rising {
                number.checked_add(step)
            } else {
                number.checked_sub(step)
            }
        })
        .take_while(|number| *number >= lowest)
        .map_while(|number| Some((number, *cursor.sealed.find(number)?)))
        .take(run.limits.items.min(MAX_SPACED_HEADERS))
        .collect();
        let archive = cursor.archive;
        let mut reads = futures_util::stream::iter(chunks)
            .map(|(number, chunk)| async move {
                let block = archive
                    .source
                    .stream(&chunk, number, ONE_SEGMENT)
                    .next()
                    .await;
                block.transpose().map_err(remote("chunk read"))
            })
            .buffered(SPACED_READS);
        let mut taking = true;
        while let Some(block) = reads.next().await {
            let Some(block) = block? else { break };
            // Every read counts, those past where the run ends too.
            cursor.bytes = cursor.bytes.saturating_add(size(&block));
            taking = taking && run.push(&block.encoded.header);
        }
        Ok(())
    }

    /// Reads a run through a [`Cursor`], for a read that needs R2.
    async fn read_through(
        &self,
        sealed: Sealed,
        read: &BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> Result<Vec<Bytes>, StorageError> {
        let mut run = Run {
            items: Vec::new(),
            bytes: 0,
            limits,
            convert,
        };
        // What of each block the answer needs: rebuilding receipts' blooms is most of what
        // reading a sealed block costs. Headers and bodies need no receipts; receipts sent the
        // eth/69 way (converted, which drops the bloom) need no blooms.
        let parts = match read {
            BlockRead::Headers { .. } | BlockRead::Bodies(_) => ReadParts::WithoutReceipts,
            BlockRead::Receipts(_) if convert.is_some() => ReadParts::WithoutBlooms,
            BlockRead::Receipts(_) => ReadParts::Whole,
        };
        let mut cursor = Cursor::new(self, sealed, parts);
        match read {
            BlockRead::Headers {
                start,
                step,
                rising,
            } => {
                let start = match start {
                    BlockStart::Number(number) => Some(*number),
                    BlockStart::Hash(hash) => self.find(*hash).await?,
                };
                let start = start.filter(|start| *start >= limits.lowest);
                match (*step, *rising) {
                    (2.., _) => {
                        Self::spaced_headers(&mut cursor, &mut run, start, (*step, *rising))
                            .await?;
                    }
                    (_, false) => Self::headers_down(&mut cursor, &mut run, start).await?,
                    (_, true) => {
                        let mut next = start;
                        while let Some(number) = next {
                            let Some(block) = cursor.get(number).await? else {
                                break;
                            };
                            if !run.push(&block.encoded.header) {
                                break;
                            }
                            next = number.checked_add(1);
                        }
                    }
                }
            }
            BlockRead::Bodies(hashes) | BlockRead::Receipts(hashes) => {
                let bodies = matches!(read, BlockRead::Bodies(_));
                self.by_hash(&mut cursor, &mut run, hashes, bodies).await?;
            }
        }
        self.budget.spent(cursor.bytes);
        Ok(run.items)
    }

    /// Consecutive headers down from `top` (a peer walking the chain from a hash): read
    /// upwards, the way a chunk stream goes, and answered from the top. Read one by one
    /// downwards, each block would open a stream of its own.
    async fn headers_down(
        cursor: &mut Cursor<'_, S>,
        run: &mut Run,
        top: Option<BlockNumber>,
    ) -> Result<(), StorageError> {
        let Some(top) = top else { return Ok(()) };
        let wanted = u64::try_from(run.limits.items).unwrap_or(u64::MAX);
        let low = top
            .saturating_sub(wanted.saturating_sub(1))
            .max(run.limits.lowest)
            .max(cursor.sealed.first_number().unwrap_or(0));
        let mut headers = Vec::new();
        for number in low..=top {
            // A run starts at the top: without it nothing is answered.
            let Some(block) = cursor.get(number).await? else {
                return Ok(());
            };
            headers.push(block.encoded.header);
        }
        for header in headers.iter().rev() {
            if !run.push(header) {
                break;
            }
        }
        Ok(())
    }

    /// The bodies (or receipts) of the blocks with `hashes`, up to the first not held. A
    /// syncing peer asks for consecutive blocks, so the block after the previous one is tried
    /// before the hash index, which costs GETs of its own.
    async fn by_hash(
        &self,
        cursor: &mut Cursor<'_, S>,
        run: &mut Run,
        hashes: &[BlockHash],
        bodies: bool,
    ) -> Result<(), StorageError> {
        let mut after: Option<BlockNumber> = None;
        for hash in hashes {
            let guess = match after {
                Some(number) => cursor
                    .get(number)
                    .await?
                    .filter(|block| block.encoded.hash == *hash)
                    .map(|block| (number, block)),
                None => None,
            };
            let (number, block) = if let Some(hit) = guess {
                hit
            } else {
                let Some(number) = self
                    .find(*hash)
                    .await?
                    .filter(|number| *number >= run.limits.lowest)
                else {
                    break;
                };
                let Some(block) = cursor.get(number).await? else {
                    break;
                };
                (number, block)
            };
            after = number.checked_add(1);
            let item = if bodies {
                Some(&block.encoded.body)
            } else {
                block.encoded.receipts.as_ref()
            };
            if !item.is_some_and(|item| run.push(item)) {
                break;
            }
        }
        Ok(())
    }
}

impl<S: ChunkSource> ArchiveStore for R2Archive<S> {
    /// Blocks the sealed range holds are skipped. Into an empty tail, the first block must
    /// continue the last sealed one.
    async fn append_batch(&self, blocks: Vec<ArchivedBlock>) -> Result<(), StorageError> {
        let sealed = self.sealed();
        let mut blocks = blocks.into_iter().peekable();
        let mut first = None;
        while let Some(block) = blocks.peek() {
            let header = header_of(block)?;
            if !sealed.holds(header.number) {
                first = Some(header);
                break;
            }
            blocks.next();
        }
        let blocks: Vec<ArchivedBlock> = blocks.collect();
        if let (Some(header), Some(last)) = (first, sealed.last())
            && self.tail.range().await?.is_none()
            && (header.number != last.number.saturating_add(1) || header.parent_hash != last.hash)
        {
            return Err(StorageError::NotContiguous {
                expected: last,
                got: BlockRef {
                    number: header.number.saturating_sub(1),
                    hash: header.parent_hash,
                },
            });
        }
        self.tail.append_batch(blocks).await
    }

    async fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> Result<bool, StorageError> {
        // A sealed block has its receipts: only the tail takes them.
        self.tail.set_receipts(block, receipts).await
    }

    /// A read that starts in the tail is the tail's. Any other is a peer's read of history:
    /// it needs a place in the global R2 budget for peers, and without one it is answered
    /// empty.
    async fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> Result<Vec<Bytes>, StorageError> {
        let sealed = self.sealed();
        let from_tail = match &read {
            BlockRead::Headers {
                start: BlockStart::Number(number),
                ..
            } => !sealed.holds(*number),
            BlockRead::Headers {
                start: BlockStart::Hash(hash),
                ..
            } => self.tail.number_of(*hash).await?.is_some(),
            BlockRead::Bodies(hashes) | BlockRead::Receipts(hashes) => match hashes.first() {
                Some(hash) => self.tail.number_of(*hash).await?.is_some(),
                None => true,
            },
        };
        if from_tail || sealed.chunks.is_empty() {
            return self.tail.read(read, limits, convert).await;
        }
        let Some(_place) = self.budget.try_read() else {
            return Ok(Vec::new());
        };
        self.read_through(sealed, &read, limits, convert).await
    }

    /// Above the sealed range, the tail's; within it, the read-ahead's.
    async fn blocks(
        &self,
        from: BlockNumber,
        limits: ReadLimits,
    ) -> Result<Vec<ArchivedBlock>, StorageError> {
        let sealed = self.sealed();
        if from < limits.lowest || !sealed.holds(from) {
            return self.tail.blocks(from, limits).await;
        }
        self.feeds
            .start(&self.source, &sealed, from, ReadParts::Whole)
            .read(limits)
            .await
    }

    /// The sealed range from a feed of the `parts` asked for (sealed receipts are not rebuilt
    /// for a reader without them), above it the tail's whole blocks.
    fn read_range(&self, from: BlockNumber, limits: ReadLimits, parts: ReadParts) -> ArchiveRange {
        let archive = self.clone();
        let sealed = self.sealed();
        let feed = (from >= limits.lowest && sealed.holds(from))
            .then(|| self.feeds.start(&self.source, &sealed, from, parts));
        Box::pin(futures_util::stream::unfold(
            (archive, feed, Some(from)),
            move |(archive, mut feed, next)| async move {
                let from = next?;
                let blocks = if let Some(reader) = feed.as_mut() {
                    match reader.read(limits).await {
                        Ok(blocks) if blocks.is_empty() => {
                            feed = None;
                            archive.blocks(from, limits).await
                        }
                        result => result,
                    }
                } else {
                    archive.blocks(from, limits).await
                };
                if blocks.as_ref().is_ok_and(Vec::is_empty) {
                    return None;
                }
                let next = blocks
                    .as_ref()
                    .ok()
                    .and_then(|blocks| from.checked_add(u64::try_from(blocks.len()).ok()?));
                Some((blocks, (archive, feed, next)))
            },
        ))
    }

    /// The L1 heads the tail records, raised to the last sealed block: a chunk is sealed only
    /// once every block in it is finalized, so the sealed range is finalized whatever L1 has
    /// been seen to say (with the L1 side off, nothing at all).
    async fn heads(&self) -> Result<L1Heads, StorageError> {
        let heads = self.tail.heads().await?;
        let finalized = at_least(heads.finalized, self.sealed().last());
        Ok(L1Heads {
            safe: at_least(heads.safe, finalized),
            finalized,
        })
    }

    async fn set_heads(&self, heads: L1Heads) -> Result<(), StorageError> {
        self.tail.set_heads(heads).await
    }

    async fn pending_receipts(
        &self,
        from: BlockNumber,
        limit: usize,
    ) -> Result<(Vec<BlockRef>, u64), StorageError> {
        // Sealed blocks have their receipts.
        self.tail.pending_receipts(from, limit).await
    }

    async fn number_of(&self, hash: BlockHash) -> Result<Option<BlockNumber>, StorageError> {
        self.find(hash).await
    }

    /// From the first sealed block (the tail's first, with nothing sealed) to the tail's last
    /// (the last sealed, with an empty tail).
    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError> {
        let sealed = self.sealed();
        let tail = self.tail.range().await?;
        let first = sealed.first.or(tail.map(|(first, _)| first));
        let last = tail.map(|(_, last)| last).or(sealed.last());
        Ok(first.zip(last))
    }
}

/// Reads blocks by number, keeping one chunk stream open while the numbers asked for follow
/// each other. Blocks above the sealed range come from the tail.
struct Cursor<'a, S> {
    archive: &'a R2Archive<S>,
    sealed: Sealed,
    open: Option<(BlockNumber, BoxStream<'static, io::Result<ArchivedBlock>>)>,
    /// What of each block is read.
    parts: ReadParts,
    /// Bytes read from R2 so far.
    bytes: u64,
}

impl<'a, S: ChunkSource> Cursor<'a, S> {
    const fn new(archive: &'a R2Archive<S>, sealed: Sealed, parts: ReadParts) -> Self {
        Self {
            archive,
            sealed,
            open: None,
            parts,
            bytes: 0,
        }
    }

    /// The block numbered `number`, if it is held.
    async fn get(&mut self, number: BlockNumber) -> Result<Option<ArchivedBlock>, StorageError> {
        if !self.sealed.holds(number) {
            self.open = None;
            return Ok(self.archive.tail.blocks(number, ONE).await?.pop());
        }
        // A chunk's stream yields its blocks in order from the one asked for, and ends with the
        // chunk: the next block is then the first of another stream.
        let continued = match self.open.take() {
            Some((next, mut stream)) if next == number => {
                stream.next().await.map(|block| (block, stream))
            }
            _ => None,
        };
        let (block, stream) = if let Some(read) = continued {
            read
        } else {
            let Some(chunk) = self.sealed.find(number) else {
                return Ok(None);
            };
            let mut stream = self.archive.source.stream(
                chunk,
                number,
                StreamReads {
                    parts: self.parts,
                    ..crate::feed::PEER_READS
                },
            );
            let Some(block) = stream.next().await else {
                return Ok(None);
            };
            (block, stream)
        };
        let block = block.map_err(remote("chunk read"))?;
        self.bytes = self.bytes.saturating_add(size(&block));
        self.open = Some((number.saturating_add(1), stream));
        Ok(Some(block))
    }
}

/// The items read so far, with where the run must end.
struct Run {
    items: Vec<Bytes>,
    bytes: usize,
    limits: ReadLimits,
    convert: Option<ItemConvert>,
}

impl Run {
    /// Converts an item if asked, and adds it. Returns whether the run may take another.
    fn push(&mut self, raw: &Bytes) -> bool {
        if self.items.len() >= self.limits.items {
            return false;
        }
        let item = match self.convert {
            Some(convert) => match convert(raw) {
                Some(item) => item,
                None => return false,
            },
            None => raw.clone(),
        };
        self.bytes = self.bytes.saturating_add(item.len());
        self.items.push(item);
        self.items.len() < self.limits.items && self.bytes < self.limits.bytes
    }
}

/// The decoded header of `block`.
fn header_of(block: &ArchivedBlock) -> Result<Header, StorageError> {
    Header::decode(&mut &block.encoded.header[..]).map_err(|_err| StorageError::InvalidData {
        store: Store::R2,
        what: "header",
        block: Some(block.encoded.hash),
        source: None,
    })
}

/// The bytes of a block's header, body and receipts.
pub(crate) fn size(block: &ArchivedBlock) -> u64 {
    let encoded = &block.encoded;
    let bytes = encoded
        .header
        .len()
        .saturating_add(encoded.body.len())
        .saturating_add(
            encoded
                .receipts
                .as_ref()
                .map_or(0, |receipts| receipts.len()),
        );
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// Turns a chunk source error into the storage error the node's components handle; its kind
/// says whether trying again can help.
pub(crate) fn remote(operation: &'static str) -> impl Fn(io::Error) -> StorageError {
    move |source| StorageError::Remote { operation, source }
}

/// The later of `head` and `floor`.
fn at_least(head: Option<BlockRef>, floor: Option<BlockRef>) -> Option<BlockRef> {
    head.into_iter()
        .chain(floor)
        .max_by_key(|block| block.number)
}

/// Checks that each of `chunks` continues the one before it, the first continuing `before`.
fn check_links(chunks: &[ChunkRange], before: Option<&ChunkRange>) -> Result<(), ServerError> {
    let mut previous = before;
    for chunk in chunks {
        if let Some(previous) = previous
            && (chunk.first != previous.last.saturating_add(1)
                || chunk.first_parent != previous.last_hash)
        {
            return Err(ServerError::Unlinked {
                first: chunk.first,
                last: chunk.last,
            });
        }
        previous = Some(chunk);
    }
    Ok(())
}
