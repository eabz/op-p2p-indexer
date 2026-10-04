//! The archive's fjall keyspaces and the synchronous operations on them.
//!
//! Every function here does blocking disk I/O and runs on a blocking thread (see
//! [`super::FjallArchive`]). Every write is one fjall batch, synced to disk before it returns, so
//! the held range is contiguous after any crash.
//!
//! Does not validate or encode blocks (the caller passes RLP), spawn threads, or name the
//! operation in its errors: [`Failure::into_storage_error`] attaches it.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_consensus::BlockBody;
use alloy_primitives::{BlockHash, BlockNumber, Bytes, keccak256};
use alloy_rlp::Decodable;
use fjall::{
    CompressionType, Database, Guard, Keyspace, KeyspaceCreateOptions, KvSeparationOptions,
    PersistMode, Readable,
};
use op_alloy_consensus::OpTxEnvelope;
use op_indexer_primitives::{ArchivedBlock, BlockRef};
use tokio::sync::{Mutex, MutexGuard};
use tracing::debug;

use crate::{BlockPart, InvalidBlockReason, ParseError, StorageError, Store, metrics};

/// Most blocks written in one batch by an append of many blocks: with [`MAX_APPEND_BATCH_BYTES`]
/// it bounds one journal record and how long other writers wait for the lock.
const MAX_APPEND_BATCH_BLOCKS: usize = 1024;
/// Most encoded bytes in one such batch (a single larger block still goes alone): an eighth of
/// the journal limit, and one memtable.
const MAX_APPEND_BATCH_BYTES: usize = 16 * 1024 * 1024;
/// Block cache shared by the keyspaces. It holds the index and filter blocks of the trees and
/// recently read data blocks; serving peers is not latency-critical, so it stays small and fixed
/// (fjall's default is 32 MiB).
const CACHE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
/// Most journal kept on disk before memtables are flushed to make room. It also bounds how much
/// is replayed on open after a crash (fjall's default is 512 MiB, its minimum 64 MiB).
const MAX_JOURNAL_BYTES: u64 = 128 * 1024 * 1024;
/// Memtable of one keyspace before it is flushed. With five keyspaces this bounds the active
/// memtables to 80 MiB; fjall 3.1's database-wide cap (`max_write_buffer_size`) is deprecated
/// and not enforced, so this and [`MAX_JOURNAL_BYTES`] are the bounds. Live appends add about
/// 20 KB a block, so a memtable fills in under an hour; a smaller one only means more, smaller
/// flushes (fjall's default is 64 MiB per keyspace).
const MAX_MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;
/// Background threads for flushes and compaction. The archive is a side job: it must not compete
/// with ingestion for cores (fjall's default is up to 4).
const WORKER_THREADS: usize = 2;
/// Name of the schema version entry in `meta`.
const SCHEMA_VERSION_KEY: &str = "schema_version";
/// Layout version of the keyspaces. An archive written with another version is emptied on open.
const SCHEMA_VERSION: u64 = 1;
/// Most blocks removed in one batch by [`truncate_above`] and [`trim`], so one batch stays small
/// and the writer lock is held briefly: appends go in between batches.
const DELETE_BATCH_BLOCKS: u64 = 1024;
/// Longest one [`truncate_above`] or [`trim`] call keeps removing, checked between batches. A
/// call that has more to remove then fails with a timeout and is finished by calling it again.
/// The blocking call cannot be cancelled, so this also bounds how long it outlives a dropped
/// future or delays shutdown. The same limit as the unsafe store's multi-step operations.
const REMOVE_DEADLINE: Duration = Duration::from_secs(60);

/// The open database and its keyspaces. Cheap to clone: fjall handles are reference-counted.
#[derive(Clone)]
pub(super) struct Tables {
    db: Database,
    /// Block number -> snappy-compressed RLP of the header.
    headers: Keyspace,
    /// Block number -> snappy-compressed RLP of the body; key-value separated.
    bodies: Keyspace,
    /// Block number -> snappy-compressed RLP list of the receipts, absent until set;
    /// key-value separated.
    receipts: Keyspace,
    /// Block hash -> block number.
    numbers: Keyspace,
    /// Name -> value; holds [`SCHEMA_VERSION_KEY`].
    meta: Keyspace,
    /// Held by every write: a batch has no conflict detection, so two appends must not both
    /// read the same tip. tokio's mutex because it is granted in arrival order: with std's, a
    /// removal taking the lock again for its next batch kept waiting appends out until the
    /// whole removal was done. Not fjall's single-writer transaction database, which would
    /// change every keyspace type.
    writer: Arc<Mutex<()>>,
}

/// A block encoded for the archive: RLP, not yet compressed.
#[derive(Debug)]
pub(super) struct Entry {
    pub(super) block: BlockRef,
    pub(super) parent_hash: BlockHash,
    pub(super) header: Bytes,
    pub(super) body: Bytes,
    pub(super) receipts: Option<Bytes>,
}

/// Why a keyspace operation failed, before the operation's name is attached
/// ([`Failure::into_storage_error`]).
#[derive(Debug)]
pub(super) enum Failure {
    /// fjall failed.
    Fjall(fjall::Error),
    /// A removal had more to do when [`REMOVE_DEADLINE`] passed.
    Deadline,
    /// The data or the block did not allow the operation.
    Storage(StorageError),
}

/// Which end of the range a batch removes from.
#[derive(Clone, Copy)]
enum End {
    Oldest,
    Newest,
}

/// What one [`remove_batch`] did.
struct Batch {
    removed: u64,
    /// More blocks were to be removed than one batch takes.
    unfinished: bool,
}

impl Tables {
    /// A batch whose commit returns only once the journal is synced to disk.
    fn durable_batch(&self) -> fjall::OwnedWriteBatch {
        self.db.batch().durability(Some(PersistMode::SyncAll))
    }

    /// Waits for this writer's turn. Blocks the thread, so it must not run on a runtime worker:
    /// every caller is on a blocking thread.
    fn lock(&self) -> MutexGuard<'_, ()> {
        self.writer.blocking_lock()
    }
}

impl Entry {
    /// Checks that this block is the child of `parent`.
    fn extends(&self, parent: BlockRef) -> Result<(), StorageError> {
        // The parent the block claims. Block 0 has none, so it never extends anything; its
        // `got` is reported at number 0.
        let number = self.block.number.checked_sub(1);
        let got = BlockRef {
            number: number.unwrap_or(0),
            hash: self.parent_hash,
        };
        if number.is_none() || got != parent {
            return Err(StorageError::NotContiguous {
                expected: parent,
                got,
            });
        }
        Ok(())
    }

    /// The size of the RLP this block adds to a batch.
    fn encoded_len(&self) -> usize {
        let receipts = self.receipts.as_ref().map_or(0, |receipts| receipts.len());
        self.header.len() + self.body.len() + receipts
    }
}

impl Failure {
    /// The [`StorageError`] for `operation`.
    pub(super) fn into_storage_error(self, operation: &'static str) -> StorageError {
        match self {
            Self::Fjall(source) => StorageError::Fjall { operation, source },
            Self::Deadline => StorageError::Timeout {
                store: Store::Archive,
                operation,
            },
            Self::Storage(error) => error,
        }
    }
}

impl From<fjall::Error> for Failure {
    fn from(error: fjall::Error) -> Self {
        Self::Fjall(error)
    }
}

impl From<StorageError> for Failure {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// Opens the database in the directory `path`, creates the keyspaces and checks the schema
/// version, emptying the archive if it differs.
///
/// Returns the stored version entry when the archive was emptied because of it (another version,
/// or bytes that are not a version at all), `None` otherwise (also for a new archive).
pub(super) fn open(path: &Path) -> Result<(Tables, Option<Bytes>), Failure> {
    let db = Database::builder(path)
        .cache_size(CACHE_SIZE_BYTES)
        .max_journaling_size(MAX_JOURNAL_BYTES)
        .worker_threads(WORKER_THREADS)
        .open()?;
    let inline = || KeyspaceCreateOptions::default().max_memtable_size(MAX_MEMTABLE_BYTES);
    // Values are already snappy-compressed: no blob compression on top.
    let separated = || {
        inline().with_kv_separation(Some(
            KvSeparationOptions::default().compression(CompressionType::None),
        ))
    };
    let tables = Tables {
        headers: db.keyspace("headers", inline)?,
        bodies: db.keyspace("bodies", separated)?,
        receipts: db.keyspace("receipts", separated)?,
        numbers: db.keyspace("numbers", inline)?,
        meta: db.keyspace("meta", inline)?,
        db,
        writer: Arc::default(),
    };
    let current = SCHEMA_VERSION.to_be_bytes();
    let stored = tables.meta.get(SCHEMA_VERSION_KEY)?;
    if stored.as_deref() == Some(current.as_slice()) {
        return Ok((tables, None));
    }
    // Cleared before the version is written: a crash in between clears again on next open.
    for keyspace in [
        &tables.headers,
        &tables.bodies,
        &tables.receipts,
        &tables.numbers,
    ] {
        keyspace.clear()?;
    }
    let mut batch = tables.durable_batch();
    batch.insert(&tables.meta, SCHEMA_VERSION_KEY, current);
    batch.commit()?;
    let emptied = stored.map(|version| Bytes::copy_from_slice(&version));
    Ok((tables, emptied))
}

/// Appends `blocks`, which must be consecutive, oldest first, and extend the held range (or the
/// archive is empty). Leading blocks the archive holds, up to its tip, are skipped.
///
/// Everything is checked before the first write. The blocks are then written in chunks, one
/// durable batch and one turn at the writer lock each, so a long list neither builds one huge
/// journal record nor keeps other writers out for its whole length.
pub(super) fn append_batch(tables: &Tables, blocks: &[Entry]) -> Result<(), Failure> {
    for pair in blocks.windows(2) {
        if let [parent, child] = pair {
            child.extends(parent.block)?;
        }
    }
    // Read without the lock: a writer getting in between shows as `NotContiguous` below.
    let tip = end_ref(tables.headers.last_key_value())?;
    let mut rest = match tip {
        Some(tip) => above(blocks, tip)?,
        None => blocks,
    };
    let appended = rest.len();
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(chunk_len(rest));
        append_chunk(tables, chunk)?;
        rest = tail;
    }
    if appended > 0 {
        metrics::blocks_inserted(Store::Archive, appended);
        record_usage(tables);
    }
    Ok(())
}

/// The blocks of `blocks` (consecutive) above `tip`: all of them if the first extends it, those
/// after the tip if the tip is among them.
fn above(blocks: &[Entry], tip: BlockRef) -> Result<&[Entry], StorageError> {
    let Some(first) = blocks.first() else {
        return Ok(blocks);
    };
    let held = tip
        .number
        .checked_sub(first.block.number)
        .and_then(|offset| usize::try_from(offset).ok())
        .and_then(|offset| Some((blocks.get(offset)?, blocks.get(offset.checked_add(1)?..)?)));
    match held {
        Some((at_tip, rest)) if at_tip.block == tip => Ok(rest),
        Some((at_tip, _)) => Err(StorageError::NotContiguous {
            expected: tip,
            got: at_tip.block,
        }),
        None => first.extends(tip).map(|()| blocks),
    }
}

/// How many of `blocks` go into one batch: up to [`MAX_APPEND_BATCH_BLOCKS`], fewer when their
/// encoded size passes [`MAX_APPEND_BATCH_BYTES`], and always at least one.
fn chunk_len(blocks: &[Entry]) -> usize {
    let mut bytes = 0_usize;
    blocks
        .iter()
        .take(MAX_APPEND_BATCH_BLOCKS)
        .take_while(|block| {
            bytes = bytes.saturating_add(block.encoded_len());
            bytes <= MAX_APPEND_BATCH_BYTES
        })
        .count()
        .max(1)
}

/// Writes `chunk` (consecutive, not empty) in one durable batch if it extends the tip.
fn append_chunk(tables: &Tables, chunk: &[Entry]) -> Result<(), Failure> {
    // Compressed before the lock is taken.
    let mut values = Vec::with_capacity(chunk.len());
    for block in chunk {
        let number = block.block.number;
        let receipts = block.receipts.as_deref();
        values.push((
            compress(&block.header, number)?,
            compress(&block.body, number)?,
            receipts
                .map(|receipts| compress(receipts, number))
                .transpose()?,
        ));
    }

    let _writer = tables.lock();
    if let (Some(tip), Some(first)) = (end_ref(tables.headers.last_key_value())?, chunk.first()) {
        first.extends(tip)?;
    }
    let mut batch = tables.durable_batch();
    for (block, (header, body, receipts)) in chunk.iter().zip(values) {
        let key = block.block.number.to_be_bytes();
        batch.insert(&tables.headers, key, header);
        batch.insert(&tables.bodies, key, body);
        if let Some(receipts) = receipts {
            batch.insert(&tables.receipts, key, receipts);
        }
        batch.insert(&tables.numbers, block.block.hash.0, key);
    }
    batch.commit()?;
    Ok(())
}

/// Stores `receipts` (RLP, `count` of them) for `block`. `Ok(false)` if it is not archived.
pub(super) fn set_receipts(
    tables: &Tables,
    block: BlockRef,
    receipts: &[u8],
    count: usize,
) -> Result<bool, Failure> {
    let invalid = |reason| StorageError::InvalidBlock {
        number: block.number,
        reason,
    };
    let receipts = compress(receipts, block.number)?;
    let _writer = tables.lock();
    let Some(stored) = tables.numbers.get(block.hash.0)? else {
        return Ok(false);
    };
    let stored = decode_number(&stored)?;
    if stored != block.number {
        return Err(invalid(InvalidBlockReason::StoredNumber).into());
    }
    let key = stored.to_be_bytes();
    let body = tables
        .bodies
        .get(key)?
        .ok_or_else(|| missing("body", block.hash))?;
    let body = decompress(&body, "body", Some(block.hash))?;
    let body = BlockBody::<OpTxEnvelope>::decode(&mut body.as_slice())
        .map_err(|source| invalid_data("body", Some(block.hash), source.into()))?;
    if body.transactions.len() != count {
        return Err(invalid(InvalidBlockReason::ReceiptCount).into());
    }
    let mut batch = tables.durable_batch();
    batch.insert(&tables.receipts, key, receipts);
    batch.commit()?;
    Ok(true)
}

/// The archived block at `number`, decompressed, read from one snapshot.
pub(super) fn block(
    tables: &Tables,
    number: BlockNumber,
) -> Result<Option<ArchivedBlock>, Failure> {
    let snapshot = tables.db.snapshot();
    let key = number.to_be_bytes();
    let Some(header) = snapshot.get(&tables.headers, key)? else {
        return Ok(None);
    };
    let header = decompress(&header, "header", None)?;
    let hash = keccak256(&header);
    let body = snapshot
        .get(&tables.bodies, key)?
        .ok_or_else(|| missing("body", hash))?;
    let receipts = snapshot
        .get(&tables.receipts, key)?
        .map(|receipts| decompress(&receipts, "receipts", Some(hash)))
        .transpose()?;
    Ok(Some(ArchivedBlock {
        header: Bytes::from(header),
        body: Bytes::from(decompress(&body, "body", Some(hash))?),
        receipts: receipts.map(Bytes::from),
    }))
}

/// One part of the archived block at `number`, decompressed; only its keyspace is read.
pub(super) fn part(
    tables: &Tables,
    number: BlockNumber,
    part: BlockPart,
) -> Result<Option<Bytes>, Failure> {
    let (keyspace, what) = match part {
        BlockPart::Header => (&tables.headers, "header"),
        BlockPart::Body => (&tables.bodies, "body"),
        BlockPart::Receipts => (&tables.receipts, "receipts"),
    };
    let Some(value) = keyspace.get(number.to_be_bytes())? else {
        return Ok(None);
    };
    Ok(Some(decompress(&value, what, None)?.into()))
}

/// The number of the archived block with `hash`.
pub(super) fn number_of(tables: &Tables, hash: BlockHash) -> Result<Option<BlockNumber>, Failure> {
    Ok(tables
        .numbers
        .get(hash.0)?
        .map(|number| decode_number(&number))
        .transpose()?)
}

/// The first and last archived block, from the first and last keys of `headers` in one
/// snapshot.
pub(super) fn range(tables: &Tables) -> Result<Option<(BlockRef, BlockRef)>, Failure> {
    let snapshot = tables.db.snapshot();
    let first = end_ref(snapshot.first_key_value(&tables.headers))?;
    let last = end_ref(snapshot.last_key_value(&tables.headers))?;
    Ok(first.zip(last))
}

/// Removes every block above `number`, newest first, in batches of [`DELETE_BATCH_BLOCKS`],
/// for at most [`REMOVE_DEADLINE`].
pub(super) fn truncate_above(tables: &Tables, number: BlockNumber) -> Result<(), Failure> {
    let excess = |(_, last): (BlockNumber, BlockNumber)| last.saturating_sub(number);
    remove(tables, End::Newest, excess).map(drop)
}

/// Removes the oldest blocks so at most `retain` remain, in batches of [`DELETE_BATCH_BLOCKS`],
/// for at most [`REMOVE_DEADLINE`]. Returns how many were removed.
pub(super) fn trim(tables: &Tables, retain: u64) -> Result<u64, Failure> {
    // The range is contiguous, so its length follows from its ends.
    let excess = |(first, last): (BlockNumber, BlockNumber)| {
        last.saturating_sub(first)
            .saturating_add(1)
            .saturating_sub(retain)
    };
    remove(tables, End::Oldest, excess)
}

/// Removes blocks from `end`, batch by batch, until `excess(first, last)` is zero or
/// [`REMOVE_DEADLINE`] has passed with more to remove ([`Failure::Deadline`]). Returns how many
/// were removed; the count is also recorded as a metric, so it is not lost with an error.
///
/// The writer lock is taken per batch, so appends go in between batches; each batch measures
/// the range again.
fn remove(
    tables: &Tables,
    end: End,
    excess: impl Fn((BlockNumber, BlockNumber)) -> u64,
) -> Result<u64, Failure> {
    let started = Instant::now();
    let mut removed: u64 = 0;
    let outcome = loop {
        let batch = match remove_batch(tables, end, &excess) {
            Ok(batch) => batch,
            Err(failure) => break Err(failure),
        };
        removed = removed.saturating_add(batch.removed);
        if !batch.unfinished {
            break Ok(removed);
        }
        if started.elapsed() >= REMOVE_DEADLINE {
            break Err(Failure::Deadline);
        }
    };
    metrics::archive_blocks_removed(removed);
    record_usage(tables);
    outcome
}

/// Removes `excess(first, last)` blocks from `end`, at most [`DELETE_BATCH_BLOCKS`], in one
/// durable batch under the writer lock. Removing from an end keeps the range contiguous.
fn remove_batch(
    tables: &Tables,
    end: End,
    excess: impl Fn((BlockNumber, BlockNumber)) -> u64,
) -> Result<Batch, Failure> {
    let _writer = tables.lock();
    let first = end_key(tables.headers.first_key_value())?;
    let last = end_key(tables.headers.last_key_value())?;
    let Some(ends) = first.zip(last) else {
        return Ok(Batch {
            removed: 0,
            unfinished: false,
        });
    };
    let excess = excess(ends);
    let take = usize::try_from(excess.min(DELETE_BATCH_BLOCKS)).unwrap_or(usize::MAX);
    let iter = tables.headers.iter();
    let doomed: Box<dyn Iterator<Item = Guard>> = match end {
        End::Oldest => Box::new(iter.take(take)),
        End::Newest => Box::new(iter.rev().take(take)),
    };
    let mut batch = tables.durable_batch();
    let mut removed: u64 = 0;
    for guard in doomed {
        let (key, header) = guard.into_inner()?;
        let hash = keccak256(decompress(&header, "header", None)?);
        batch.remove(&tables.headers, key.clone());
        batch.remove(&tables.bodies, key.clone());
        batch.remove(&tables.receipts, key);
        batch.remove(&tables.numbers, hash.0);
        removed = removed.saturating_add(1);
    }
    batch.commit()?;
    Ok(Batch {
        removed,
        unfinished: excess > removed,
    })
}

/// Samples fjall's own counters into the archive gauges: disk use (journal, trees and blob
/// files), blob bytes no longer referenced, and running compactions. All are cheap reads of
/// fjall's metadata.
fn record_usage(tables: &Tables) {
    let fragmented = tables
        .bodies
        .fragmented_blob_bytes()
        .saturating_add(tables.receipts.fragmented_blob_bytes());
    match tables.db.disk_space() {
        Ok(disk) => metrics::archive_usage(disk, fragmented, tables.db.active_compactions()),
        // A gauge left stale is better than failing a write that already succeeded.
        Err(err) => debug!(%err, "could not read the archive's disk use; gauges not updated"),
    }
}

/// The block whose header entry is `guard` (first or last of `headers`), identified by its hash.
fn end_ref(guard: Option<Guard>) -> Result<Option<BlockRef>, Failure> {
    let Some(guard) = guard else {
        return Ok(None);
    };
    let (key, header) = guard.into_inner()?;
    Ok(Some(BlockRef {
        number: decode_number(&key)?,
        hash: keccak256(decompress(&header, "header", None)?),
    }))
}

/// The block number of the header entry `guard`, without reading its value.
fn end_key(guard: Option<Guard>) -> Result<Option<BlockNumber>, Failure> {
    let Some(guard) = guard else {
        return Ok(None);
    };
    Ok(Some(decode_number(&guard.key()?)?))
}

/// A block number stored as a key or value: a big-endian `u64`.
fn decode_number(bytes: &[u8]) -> Result<BlockNumber, StorageError> {
    <[u8; 8]>::try_from(bytes)
        .map(u64::from_be_bytes)
        .map_err(|_wrong_length| StorageError::InvalidData {
            store: Store::Archive,
            what: "block number",
            block: None,
            source: None,
        })
}

fn compress(rlp: &[u8], number: BlockNumber) -> Result<Vec<u8>, StorageError> {
    snap::raw::Encoder::new()
        .compress_vec(rlp)
        .map_err(|source| StorageError::Oversized { number, source })
}

fn decompress(
    compressed: &[u8],
    what: &'static str,
    block: Option<BlockHash>,
) -> Result<Vec<u8>, StorageError> {
    snap::raw::Decoder::new()
        .decompress_vec(compressed)
        .map_err(|source| invalid_data(what, block, source.into()))
}

const fn invalid_data(
    what: &'static str,
    block: Option<BlockHash>,
    source: ParseError,
) -> StorageError {
    StorageError::InvalidData {
        store: Store::Archive,
        what,
        block,
        source: Some(source),
    }
}

/// A part of a block whose header is archived is absent: the keyspaces disagree.
const fn missing(field: &'static str, block: BlockHash) -> StorageError {
    StorageError::MissingField {
        store: Store::Archive,
        field,
        block: Some(block),
    }
}
