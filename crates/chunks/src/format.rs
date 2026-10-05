//! The chunk object: sealed blocks in segments, an index, a footer (`docs/serving.md` §1).
//!
//! ```text
//! chunk    = segment* | index | footer
//! segment  = zstd frame (level 1, with its checksum) of up to ~1 MiB of records
//! record   = hash (32) | sender count u32 | senders (20 each)
//!          | header length u32 | header RLP | body length u32 | body RLP
//!          | receipts length u32 | receipts RLP without blooms (eth/69's form)
//! index    = zstd frame of: version u16 | chain id u64 | first u64 | last u64
//!          | first parent (32) | last hash (32) | level i32
//!          | segment count u32 | per segment: offset u64, length u32, first u64, blocks u32,
//!            sha256 (32)
//!          | per block, in block order: hash (32)
//! footer   = index offset u64 | index length u32 | magic "OPXC"
//! ```
//!
//! Integers are little-endian. The chunk's root is the sha256 of its index frame: the
//! manifest records it, and the index records each segment's sha256, so a ranged read of any
//! part is checked before it is decompressed. Blocks keep the verified encodings the archive
//! holds, except for the receipts' blooms, rebuilt from the logs when read: a block read back
//! is byte for byte the [`ArchivedBlock`] that was written.

use alloy_consensus::{Header, ReceiptWithBloom, TxReceipt as _};
use alloy_primitives::{Address, B256, Bloom, Bytes, keccak256, logs_bloom};
use alloy_rlp::Decodable;
use op_alloy_consensus::{OpReceipt, OpReceiptEnvelope};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::{ArchivedBlock, EncodedBlock, ReadParts, encode_receipts};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::io::Write as _;

use zstd::zstd_safe::{DCtx, DParameter, InBuffer, OutBuffer, ResetDirective};

use crate::ChunksError;
use crate::manifest::ChunkEntry;

/// The footer's magic.
const MAGIC: [u8; 4] = *b"OPXC";
/// The index's format version.
const VERSION: u16 = 1;
/// Bytes of the footer.
const FOOTER_LEN: usize = 16;
/// zstd level of segments and index (`docs/serving.md` D3: light compression, CPU first).
const LEVEL: i32 = 1;
/// Uncompressed bytes of records after which a segment is closed.
const SEGMENT_BYTES: usize = 1 << 20;
/// Uncompressed bytes of records at which a chunk ends (D4; settled in the bench).
const CHUNK_BYTES: usize = 256 << 20;
/// Most blocks in a chunk (D4): legacy blocks are small.
const CHUNK_BLOCKS: u64 = 100_000;
/// Bytes of one segment's entry in the index.
const SEGMENT_ENTRY: usize = 8 + 4 + 8 + 4 + 32;

/// A block prepared for a chunk: its record, encoded off the writer so that several can be
/// prepared at once.
#[derive(Debug, Clone)]
pub struct ChunkRecord {
    number: u64,
    hash: B256,
    parent: B256,
    bytes: Vec<u8>,
}

impl ChunkRecord {
    /// The block's number.
    #[must_use]
    pub const fn number(&self) -> u64 {
        self.number
    }

    /// The block's hash.
    #[must_use]
    pub const fn hash(&self) -> B256 {
        self.hash
    }

    /// Prepares `block`, which must have its receipts. Its header is decoded for its number
    /// and parent and must hash to its hash; each receipt's bloom must be the one its logs
    /// give, so that the bloom rebuilt on read is the stored one.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::NotSealable`] if the block has no receipts, its header does not
    /// decode or hash to its hash, a receipt does not decode or carries another bloom, or a
    /// value is longer than `u32::MAX` bytes.
    pub fn new(block: &ArchivedBlock) -> Result<Self, ChunksError> {
        let encoded = &block.encoded;
        let unsealable = |reason| ChunksError::NotSealable {
            hash: encoded.hash,
            reason,
        };
        let receipts = encoded
            .receipts
            .as_ref()
            .ok_or_else(|| unsealable("it has no receipts"))?;
        if keccak256(&encoded.header) != encoded.hash {
            return Err(unsealable("its header does not hash to its hash"));
        }
        let header = Header::decode(&mut &encoded.header[..])
            .map_err(|_rlp| unsealable("its header does not decode"))?;
        let receipts = Vec::<OpReceiptEnvelope>::decode(&mut &receipts[..])
            .map_err(|_rlp| unsealable("its receipts do not decode"))?;
        if receipts
            .iter()
            .any(|receipt| *receipt.logs_bloom() != logs_bloom(receipt.logs()))
        {
            return Err(unsealable("a receipt's bloom is not the one of its logs"));
        }
        let receipts: Vec<OpReceipt> = receipts.into_iter().map(OpReceipt::from).collect();
        let mut without_blooms = Vec::new();
        alloy_rlp::encode_list(&receipts, &mut without_blooms);

        let mut bytes = Vec::with_capacity(
            encoded.header.len() + encoded.body.len() + without_blooms.len() + 64,
        );
        bytes.extend_from_slice(encoded.hash.as_slice());
        bytes.extend_from_slice(&length(block.senders.len(), encoded.hash)?);
        for sender in &block.senders {
            bytes.extend_from_slice(sender.as_slice());
        }
        for value in [&encoded.header[..], &encoded.body[..], &without_blooms[..]] {
            bytes.extend_from_slice(&length(value.len(), encoded.hash)?);
            bytes.extend_from_slice(value);
        }
        Ok(Self {
            number: header.number,
            hash: encoded.hash,
            parent: header.parent_hash,
            bytes,
        })
    }
}

/// A finished chunk: its bytes and its manifest entry.
#[derive(Debug, Clone)]
pub struct SealedChunk {
    /// The whole object.
    pub bytes: bytes::Bytes,
    /// What the manifest records about it.
    pub entry: ChunkEntry,
}

/// One segment, as the index records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Segment {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) first: u64,
    pub(crate) blocks: u32,
    pub(crate) sha256: B256,
}

impl Segment {
    /// The byte range of the segment in its chunk.
    pub(crate) const fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset.saturating_add(self.length as u64)
    }

    /// Whether the segment holds block `number`.
    pub(crate) const fn holds(&self, number: u64) -> bool {
        number >= self.first && number < self.first.saturating_add(self.blocks as u64)
    }
}

/// Builds one chunk from consecutive blocks, in memory (a chunk is a few tens of MB).
#[derive(Debug)]
pub struct ChunkWriter {
    chain: &'static ChainSpec,
    first: u64,
    next: u64,
    first_parent: Option<B256>,
    last_hash: B256,
    body: Vec<u8>,
    segment: Vec<u8>,
    segment_first: u64,
    segment_blocks: u32,
    segments: Vec<Segment>,
    hashes: Vec<B256>,
    uncompressed: usize,
}

impl ChunkWriter {
    /// A writer for the chunk of `chain` that starts at block `first`.
    #[must_use]
    pub const fn new(chain: &'static ChainSpec, first: u64) -> Self {
        Self {
            chain,
            first,
            next: first,
            first_parent: None,
            last_hash: B256::ZERO,
            body: Vec::new(),
            segment: Vec::new(),
            segment_first: first,
            segment_blocks: 0,
            segments: Vec::new(),
            hashes: Vec::new(),
            uncompressed: 0,
        }
    }

    /// Adds the next block. Returns whether the chunk ends with it (D4): its records reach
    /// 256 MiB, it holds 100,000 blocks, or the next block is the chain's Bedrock block.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::NotSealable`] if `record` is not the next block or does not name
    /// the previous one as its parent, and [`ChunksError::Io`] if compressing fails.
    pub fn push(&mut self, mut record: ChunkRecord) -> Result<bool, ChunksError> {
        if record.number != self.next {
            return Err(ChunksError::NotSealable {
                hash: record.hash,
                reason: "it is not the chunk's next block",
            });
        }
        if self.first_parent.is_some() && record.parent != self.last_hash {
            return Err(ChunksError::NotSealable {
                hash: record.hash,
                reason: "it does not name the previous block as its parent",
            });
        }
        self.first_parent.get_or_insert(record.parent);
        self.last_hash = record.hash;
        self.hashes.push(record.hash);
        self.uncompressed = self.uncompressed.saturating_add(record.bytes.len());
        self.segment.append(&mut record.bytes);
        self.segment_blocks = self.segment_blocks.saturating_add(1);
        self.next = record.number.saturating_add(1);
        if self.segment.len() >= SEGMENT_BYTES {
            self.close_segment()?;
        }
        let blocks = self.next.saturating_sub(self.first);
        let before_bedrock = self.chain.bedrock_block > 0 && self.next == self.chain.bedrock_block;
        Ok(self.uncompressed >= CHUNK_BYTES || blocks >= CHUNK_BLOCKS || before_bedrock)
    }

    /// Whether no block was added yet.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.next == self.first
    }

    fn close_segment(&mut self) -> Result<(), ChunksError> {
        if self.segment_blocks == 0 {
            return Ok(());
        }
        let frame = compress(&self.segment)?;
        let offset = self.body.len() as u64;
        self.segments.push(Segment {
            offset,
            length: u32::try_from(frame.len()).map_err(|_long| ChunksError::NotSealable {
                hash: self.last_hash,
                reason: "a segment is longer than u32::MAX bytes",
            })?,
            first: self.segment_first,
            blocks: self.segment_blocks,
            sha256: sha256(&frame),
        });
        self.body.extend_from_slice(&frame);
        self.segment.clear();
        self.segment_first = self.next;
        self.segment_blocks = 0;
        Ok(())
    }

    /// Seals the chunk: its last segment, its index and its footer.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::NotSealable`] if no block was added, and [`ChunksError::Io`] if
    /// compressing fails.
    pub fn finish(mut self) -> Result<SealedChunk, ChunksError> {
        self.close_segment()?;
        let Some(first_parent) = self.first_parent else {
            return Err(ChunksError::NotSealable {
                hash: B256::ZERO,
                reason: "a chunk needs at least one block",
            });
        };
        let last = self.next.saturating_sub(1);
        let mut index =
            Vec::with_capacity(128 + self.segments.len() * SEGMENT_ENTRY + self.hashes.len() * 32);
        index.extend_from_slice(&VERSION.to_le_bytes());
        index.extend_from_slice(&self.chain.chain_id.to_le_bytes());
        index.extend_from_slice(&self.first.to_le_bytes());
        index.extend_from_slice(&last.to_le_bytes());
        index.extend_from_slice(first_parent.as_slice());
        index.extend_from_slice(self.last_hash.as_slice());
        index.extend_from_slice(&LEVEL.to_le_bytes());
        let segments =
            u32::try_from(self.segments.len()).map_err(|_many| ChunksError::NotSealable {
                hash: self.last_hash,
                reason: "a chunk has more than u32::MAX segments",
            })?;
        index.extend_from_slice(&segments.to_le_bytes());
        for segment in &self.segments {
            index.extend_from_slice(&segment.offset.to_le_bytes());
            index.extend_from_slice(&segment.length.to_le_bytes());
            index.extend_from_slice(&segment.first.to_le_bytes());
            index.extend_from_slice(&segment.blocks.to_le_bytes());
            index.extend_from_slice(segment.sha256.as_slice());
        }
        for hash in &self.hashes {
            index.extend_from_slice(hash.as_slice());
        }
        let frame = compress(&index)?;
        let index_offset = self.body.len() as u64;
        let index_length =
            u32::try_from(frame.len()).map_err(|_long| ChunksError::NotSealable {
                hash: self.last_hash,
                reason: "the index is longer than u32::MAX bytes",
            })?;
        let root = sha256(&frame);
        let mut bytes = self.body;
        bytes.extend_from_slice(&frame);
        bytes.extend_from_slice(&index_offset.to_le_bytes());
        bytes.extend_from_slice(&index_length.to_le_bytes());
        bytes.extend_from_slice(&MAGIC);
        let entry = ChunkEntry {
            first: self.first,
            last,
            first_parent,
            last_hash: self.last_hash,
            sha256: root,
            size: bytes.len() as u64,
            footer_offset: index_offset,
            level: LEVEL,
        };
        Ok(SealedChunk {
            bytes: bytes.into(),
            entry,
        })
    }
}

/// A chunk's index: its segments and its blocks' hashes, read from one ranged GET.
#[derive(Debug, Clone)]
pub struct ChunkIndex {
    first: u64,
    pub(crate) segments: Vec<Segment>,
    /// The blocks' hashes in block order.
    hashes: Vec<B256>,
}

impl ChunkIndex {
    /// Reads the index from the chunk's tail (index frame and footer), checking it against
    /// `entry`: the footer, the frame's sha256 (the chunk's root) and the range it states.
    pub(crate) fn parse(entry: &ChunkEntry, tail: &[u8]) -> Result<Self, ChunksError> {
        let malformed = |reason| ChunksError::Malformed {
            key: entry.key(),
            reason,
        };
        let footer_at = tail
            .len()
            .checked_sub(FOOTER_LEN)
            .ok_or_else(|| malformed("the object is shorter than its footer"))?;
        let (frame, footer) = tail.split_at(footer_at);
        let mut footer = Reader::new(footer);
        let (offset, length) = (footer.u64(), footer.u32());
        if footer.take(4) != Some(&MAGIC[..])
            || offset != Some(entry.footer_offset)
            || length.map(|length| length as usize) != Some(frame.len())
        {
            return Err(malformed("its footer is not the manifest's"));
        }
        if sha256(frame) != entry.sha256 {
            return Err(ChunksError::Integrity {
                key: entry.key(),
                check: "its index does not hash to the manifest's root",
            });
        }
        let index = decompress(frame).ok_or_else(|| malformed("its index does not decompress"))?;
        let mut reader = Reader::new(&index);
        let header = (|| {
            Some((
                reader.u16()?,
                reader.u64()?,
                reader.u64()?,
                reader.u64()?,
                reader.hash()?,
                reader.hash()?,
            ))
        })();
        let Some((VERSION, _chain_id, first, last, first_parent, last_hash)) = header else {
            return Err(malformed("its index header is not of this format"));
        };
        if (first, last, first_parent, last_hash)
            != (entry.first, entry.last, entry.first_parent, entry.last_hash)
        {
            return Err(ChunksError::Integrity {
                key: entry.key(),
                check: "its index states another range than the manifest",
            });
        }
        let segments = (|| {
            let _level = reader.u32()?;
            let count = reader.u32()?;
            (0..count)
                .map(|_| {
                    Some(Segment {
                        offset: reader.u64()?,
                        length: reader.u32()?,
                        first: reader.u64()?,
                        blocks: reader.u32()?,
                        sha256: reader.hash()?,
                    })
                })
                .collect::<Option<Vec<_>>>()
        })()
        .ok_or_else(|| malformed("its segment table is cut short"))?;
        let blocks = last.saturating_sub(first).saturating_add(1);
        let hashes = (0..blocks)
            .map(|_| reader.hash())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| malformed("its hash table is cut short"))?;
        let covered: u64 = segments
            .iter()
            .map(|segment| u64::from(segment.blocks))
            .sum();
        if covered != blocks || !reader.is_empty() {
            return Err(malformed("its segments do not cover its blocks"));
        }
        Ok(Self {
            first,
            segments,
            hashes,
        })
    }

    /// The number of the block with `hash`, if the chunk holds it.
    #[must_use]
    pub fn number_of(&self, hash: B256) -> Option<u64> {
        // A scan: an index serves one or two lookups, cheaper than sorting it for them.
        let position = self.hashes.iter().position(|held| *held == hash)?;
        Some(self.first.saturating_add(u64::try_from(position).ok()?))
    }

    /// The chunk's blocks, `(hash, number)`, in block order.
    pub fn blocks(&self) -> impl Iterator<Item = (B256, u64)> + '_ {
        (self.first..)
            .zip(self.hashes.iter().copied())
            .map(|(number, hash)| (hash, number))
    }

    /// The segment holding block `number`.
    pub(crate) fn segment_of(&self, number: u64) -> Option<usize> {
        self.segments
            .iter()
            .position(|segment| segment.holds(number))
    }
}

/// Decodes one segment, checking its bytes against the index's sha256 first. With `links`
/// (the last block's hash before the segment, or the chunk's first parent) it also checks
/// each block's header against its hash and its parent link to the block before: what a reader
/// that does not trust the index needs (`import fetch`). Without, the records are taken as
/// sealed: the index is the manifest's (its sha256, [`ChunkIndex::parse`]), the segment the
/// index's, and every block was verified before it was sealed, so a server reading its own
/// chunks does not hash every header again. Only blocks numbered in `wanted` are built, with
/// the `parts` asked for; the rest are skipped.
pub(crate) fn decode_segment(
    entry: &ChunkEntry,
    segment: &Segment,
    bytes: &[u8],
    mut links: Option<B256>,
    wanted: std::ops::Range<u64>,
    parts: ReadParts,
) -> Result<Vec<ArchivedBlock>, ChunksError> {
    let integrity = |check| ChunksError::Integrity {
        key: entry.key(),
        check,
    };
    let malformed = |reason| ChunksError::Malformed {
        key: entry.key(),
        reason,
    };
    if sha256(bytes) != segment.sha256 {
        return Err(integrity("a segment does not hash to the index's sha256"));
    }
    // One buffer for the segment: the blocks built from it are slices of it, not copies.
    let records = bytes::Bytes::from(
        decompress(bytes).ok_or_else(|| malformed("a segment does not decompress"))?,
    );
    let mut reader = Reader::new(&records);
    let mut blocks = Vec::new();
    let numbers = segment.first..segment.first.saturating_add(u64::from(segment.blocks));
    for number in numbers {
        let record = Record::read(&mut reader).ok_or_else(|| malformed("a record is cut short"))?;
        if let Some(parent) = &mut links {
            if keccak256(record.header) != record.hash {
                return Err(integrity("a header does not hash to its hash"));
            }
            if parent_hash(record.header) != Some(*parent) {
                return Err(integrity(
                    "a block does not name the previous block as its parent",
                ));
            }
            *parent = record.hash;
        }
        if wanted.contains(&number) {
            blocks.push(
                record
                    .build(&records, parts)
                    .ok_or_else(|| malformed("a record's receipts do not decode"))?,
            );
        }
    }
    if !reader.is_empty() {
        return Err(malformed("a segment holds more than its blocks"));
    }
    Ok(blocks)
}

/// The parent hash in a header's RLP: its first field, read without decoding the rest.
fn parent_hash(header: &[u8]) -> Option<B256> {
    let mut rest = header;
    let list = alloy_rlp::Header::decode(&mut rest).ok()?;
    let (&0xa0, rest) = rest.split_first().filter(|_| list.list)? else {
        return None;
    };
    rest.first_chunk::<32>().map(|parent| B256::from(*parent))
}

/// A record as it lies in its segment.
struct Record<'a> {
    hash: B256,
    senders: &'a [u8],
    header: &'a [u8],
    body: &'a [u8],
    receipts: &'a [u8],
}

impl<'a> Record<'a> {
    fn read(reader: &mut Reader<'a>) -> Option<Self> {
        let hash = reader.hash()?;
        let count = reader.u32()?;
        let senders = reader.take(usize::try_from(count).ok()?.checked_mul(20)?)?;
        Some(Self {
            hash,
            senders,
            header: reader.value()?,
            body: reader.value()?,
            receipts: reader.value()?,
        })
    }

    /// The block, its header and body sliced out of `records` (the segment they lie in), its
    /// receipts' blooms rebuilt from their logs (zero for [`ReadParts::WithoutBlooms`]), unless
    /// `parts` leaves the receipts out: then they are an empty value, known and not loaded
    /// ([`ReadParts::WithoutReceipts`]).
    fn build(&self, records: &bytes::Bytes, parts: ReadParts) -> Option<ArchivedBlock> {
        let receipts = match parts {
            ReadParts::Whole | ReadParts::WithoutBlooms => {
                let blooms = parts == ReadParts::Whole;
                let receipts = Vec::<OpReceipt>::decode(&mut &self.receipts[..]).ok()?;
                let receipts: Vec<OpReceiptEnvelope> = receipts
                    .into_iter()
                    .map(|receipt| {
                        let bloom = if blooms { receipt.bloom() } else { Bloom::ZERO };
                        ReceiptWithBloom::new(receipt, bloom).into()
                    })
                    .collect();
                encode_receipts(&receipts)
            }
            ReadParts::WithoutReceipts => Bytes::new(),
        };
        let (senders, _rest) = self.senders.as_chunks::<20>();
        Some(ArchivedBlock {
            encoded: EncodedBlock {
                hash: self.hash,
                header: Bytes::from(records.slice_ref(self.header)),
                body: Bytes::from(records.slice_ref(self.body)),
                receipts: Some(receipts),
            },
            senders: senders
                .iter()
                .map(|sender| Address::from(*sender))
                .collect(),
        })
    }
}

/// Decompressed bytes reserved per compressed byte before a frame is decompressed: blocks
/// compress up to about sevenfold, so most frames decompress without the buffer growing.
const EXPANSION: usize = 8;

/// Decompresses one frame of a chunk (a segment or the index) whose sha256 the caller has
/// checked. `None` if it is not one whole zstd frame.
///
/// The frame's own checksum (XXH64 of what it decompresses to) is not checked: the sha256
/// over its compressed bytes already is, and is the stronger check. Decompresses straight
/// into the buffer it returns, with one decompression context per thread, where
/// `zstd::decode_all` copies through a small buffer and grows its output from empty: the
/// two were a third of a Flight read's CPU.
fn decompress(frame: &[u8]) -> Option<Vec<u8>> {
    thread_local! {
        static CONTEXT: RefCell<DCtx<'static>> = RefCell::new({
            let mut context = DCtx::create();
            // Without it the frame's checksum is checked too: slower, not wrong.
            let _ignored = context.set_parameter(DParameter::ForceIgnoreChecksum(true));
            context
        });
    }
    CONTEXT.with_borrow_mut(|context| {
        context.reset(ResetDirective::SessionOnly).ok()?;
        let mut out = Vec::with_capacity(frame.len().saturating_mul(EXPANSION));
        let mut input = InBuffer::around(frame);
        loop {
            if out.len() == out.capacity() {
                out.reserve(out.capacity().max(1 << 16));
            }
            let len = out.len();
            let left = {
                let mut output = OutBuffer::around_pos(&mut out, len);
                context.decompress_stream(&mut output, &mut input).ok()?
            };
            if left == 0 {
                // The frame ended: nothing may follow it.
                return (input.pos() == frame.len()).then_some(out);
            }
            // Every byte read and room left over: the frame is cut short.
            if input.pos() == frame.len() && out.len() < out.capacity() {
                return None;
            }
        }
    })
}

/// The sha256 of `bytes`.
pub(crate) fn sha256(bytes: &[u8]) -> B256 {
    B256::from_slice(&Sha256::digest(bytes))
}

fn compress(bytes: &[u8]) -> Result<Vec<u8>, ChunksError> {
    let mut encoder = zstd::stream::Encoder::new(Vec::with_capacity(bytes.len() / 4), LEVEL)?;
    encoder.include_checksum(true)?;
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

fn length(len: usize, hash: B256) -> Result<[u8; 4], ChunksError> {
    u32::try_from(len)
        .map(u32::to_le_bytes)
        .map_err(|_long| ChunksError::NotSealable {
            hash,
            reason: "a value is longer than u32::MAX bytes",
        })
}

/// Reads little-endian fields from a byte slice; `None` when it is cut short.
struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.rest.split_at_checked(len)?;
        self.rest = rest;
        Some(taken)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    fn hash(&mut self) -> Option<B256> {
        self.array().map(B256::from)
    }

    fn value(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}
