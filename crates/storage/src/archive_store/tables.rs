//! The archive's fjall keyspaces and the synchronous operations on them.
//!
//! Every function here does blocking disk I/O and runs on a blocking thread (see
//! [`super::FjallArchive`]). Every write is one fjall batch, synced to disk before it returns, so
//! the held range is contiguous after any crash.
//!
//! Does not validate or encode blocks (the caller passes RLP), spawn threads, or name the
//! operation in its errors: [`Failure::into_storage_error`] attaches it.

mod append;
mod bulk;

use std::path::Path;
use std::sync::Arc;

use alloy_primitives::{Address, BlockHash, BlockNumber, Bytes, keccak256};
use fjall::{
    CompressionType, Database, Guard, Keyspace, KeyspaceCreateOptions, KvSeparationOptions,
    PersistMode, Readable,
};
use op_indexer_primitives::{
    ArchivedBlock, BlockRead, BlockRef, BlockStart, ChainIdentity, EncodedBlock, ItemConvert,
    L1Heads, ReadLimits, split_body,
};
use tokio::sync::{Mutex, MutexGuard};
use tracing::debug;

pub(super) use self::append::{Entry, append_batch};
pub(super) use self::bulk::{Prepared, bulk_append};
use crate::{InvalidBlockReason, ParseError, StorageError, Store, metrics};

/// Block cache shared by the keyspaces. It holds the index and filter blocks of the trees and
/// recently read data blocks; serving peers is not latency-critical, so it stays small and fixed
/// (fjall's default is 32 MiB).
const CACHE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
/// Most journal kept on disk before memtables are flushed to make room. It also bounds how much
/// is replayed on open after a crash (fjall's default is 512 MiB, its minimum 64 MiB).
const MAX_JOURNAL_BYTES: u64 = 128 * 1024 * 1024;
/// Memtable of one keyspace before it is flushed. With seven keyspaces this bounds the active
/// memtables to 112 MiB; fjall 3.1's database-wide cap (`max_write_buffer_size`) is deprecated
/// and not enforced, so this and [`MAX_JOURNAL_BYTES`] are the bounds. Live appends add about
/// 20 KB a block, so a memtable fills in under an hour; a smaller one only means more, smaller
/// flushes (fjall's default is 64 MiB per keyspace).
const MAX_MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;
/// Background threads for flushes and compaction. The archive is a side job: it must not compete
/// with ingestion for cores (fjall's default is up to 4).
const WORKER_THREADS: usize = 2;
/// Name of the schema version entry in `meta`.
const SCHEMA_VERSION_KEY: &str = "schema_version";
/// Layout version of the keyspaces. An archive written with another version is refused on open.
/// Version 2 added `senders` and the heads in `meta`. `pending_receipts` came later without a
/// new version: an archive of version 2 gains it empty on open, which is right for an import
/// (every imported block has its receipts).
const SCHEMA_VERSION: u64 = 2;
/// Name of the entry in `meta` recording the archive's chain ([`ChainIdentity::to_bytes`]).
const CHAIN_KEY: &str = "chain";
/// Names of the entries in `meta` recording the committed safe and finalized heads: the
/// block's number (big-endian) and hash, 40 bytes. Absent until promotion records one.
const SAFE_HEAD_KEY: &str = "safe_head";
const FINALIZED_HEAD_KEY: &str = "finalized_head";
/// Length of an address in `senders`.
const ADDRESS_LEN: usize = Address::len_bytes();

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
    /// Block number -> the sender of each transaction, 20 bytes each, in block order,
    /// uncompressed (addresses do not compress).
    senders: Keyspace,
    /// Block number -> block hash, for every archived block without receipts: written with
    /// the block, removed when its receipts are set, so the few blocks promoted before their
    /// receipts arrived can be found and filled.
    pending: Keyspace,
    /// Name -> value; holds [`SCHEMA_VERSION_KEY`], [`CHAIN_KEY`], [`SAFE_HEAD_KEY`] and
    /// [`FINALIZED_HEAD_KEY`].
    meta: Keyspace,
    /// Held by every write: a batch has no conflict detection, so two appends must not both
    /// read the same tip. tokio's mutex because it is granted in arrival order, so the batches
    /// of a long append from one writer (promotion, range sync) and another's take turns. Not
    /// fjall's single-writer transaction database, which would change every keyspace type.
    writer: Arc<Mutex<()>>,
}

/// Why a keyspace operation failed, before the operation's name is attached
/// ([`Failure::into_storage_error`]).
#[derive(Debug)]
pub(super) enum Failure {
    /// fjall failed.
    Fjall(fjall::Error),
    /// The data or the block did not allow the operation.
    Storage(StorageError),
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

impl Failure {
    /// The [`StorageError`] for `operation`.
    pub(super) fn into_storage_error(self, operation: &'static str) -> StorageError {
        match self {
            Self::Fjall(source) => StorageError::Fjall { operation, source },
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
/// version and the chain. A new archive gets this build's version; one of another version is
/// refused and left as it is. Then see [`check_chain`].
pub(super) fn open(path: &Path, chain: ChainIdentity) -> Result<Tables, Failure> {
    let tables = open_schema(path)?;
    check_chain(&tables, path, chain)?;
    Ok(tables)
}

/// Checks that the archive holds `chain`. An archive with no chain recorded holds, if it holds
/// blocks, [`ChainIdentity::BEFORE_RECORD`]'s (it was written by a build before the record),
/// and if it is empty, `chain`'s; that is recorded when it is `chain`. One holding another
/// chain is refused and left as it is, unrecorded too.
fn check_chain(tables: &Tables, path: &Path, chain: ChainIdentity) -> Result<(), Failure> {
    let (found, recorded) = if let Some(record) = tables.meta.get(CHAIN_KEY)? {
        let found = ChainIdentity::from_bytes(&record).ok_or_else(|| {
            StorageError::ArchiveChainUnreadable {
                path: path.to_owned(),
            }
        })?;
        (found, true)
    } else if tables.headers.first_key_value().is_some() {
        (ChainIdentity::BEFORE_RECORD, false)
    } else {
        (chain, false)
    };
    if found != chain {
        return Err(StorageError::ArchiveChain {
            path: path.to_owned(),
            found: Box::new(found),
            expected: Box::new(chain),
        }
        .into());
    }
    if !recorded {
        let mut batch = tables.durable_batch();
        batch.insert(&tables.meta, CHAIN_KEY, found.to_bytes());
        batch.commit()?;
    }
    Ok(())
}

/// Opens the database in the directory `path`, creates the keyspaces and checks the schema
/// version.
fn open_schema(path: &Path) -> Result<Tables, Failure> {
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
        senders: db.keyspace("senders", inline)?,
        pending: db.keyspace("pending_receipts", inline)?,
        meta: db.keyspace("meta", inline)?,
        db,
        writer: Arc::default(),
    };
    let current = SCHEMA_VERSION.to_be_bytes();
    let stored = tables.meta.get(SCHEMA_VERSION_KEY)?;
    let found = match stored.as_deref() {
        Some(version) if version == current.as_slice() => return Ok(tables),
        Some(version) => <[u8; 8]>::try_from(version).map_or_else(
            |_length| format!("0x{}", alloy_primitives::hex::encode(version)),
            |version| u64::from_be_bytes(version).to_string(),
        ),
        // No version and no blocks: a new archive.
        None if tables.headers.first_key_value().is_none() => {
            let mut batch = tables.durable_batch();
            batch.insert(&tables.meta, SCHEMA_VERSION_KEY, current);
            batch.commit()?;
            return Ok(tables);
        }
        None => "none".to_owned(),
    };
    Err(StorageError::ArchiveSchema {
        path: path.to_owned(),
        found,
        expected: SCHEMA_VERSION,
    }
    .into())
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
    // Cut, not decoded: a body may hold a transaction the typed decoder refuses.
    let body = split_body(&body).ok_or(StorageError::InvalidData {
        store: Store::Archive,
        what: "body",
        block: Some(block.hash),
        source: None,
    })?;
    if body.transactions.len() != count {
        return Err(invalid(InvalidBlockReason::ReceiptCount).into());
    }
    let mut batch = tables.durable_batch();
    batch.insert(&tables.receipts, key, receipts);
    batch.remove(&tables.pending, key);
    batch.commit()?;
    Ok(true)
}

/// Reads a run of headers, bodies or receipts from one snapshot, decompressed, up to `limits`.
/// The run ends at the first block not held.
pub(super) fn read(
    tables: &Tables,
    read: &BlockRead,
    limits: ReadLimits,
    convert: Option<ItemConvert>,
) -> Result<Vec<Bytes>, Failure> {
    let snapshot = tables.db.snapshot();
    let mut run = Run {
        items: Vec::new(),
        bytes: 0,
        limits,
        convert,
    };
    let number_of = |hash: &BlockHash| -> Result<Option<BlockNumber>, Failure> {
        let number = snapshot.get(&tables.numbers, hash.0)?;
        Ok(number.as_deref().map(decode_number).transpose()?)
    };
    match read {
        BlockRead::Headers {
            start,
            step,
            rising,
        } => {
            let start = match start {
                BlockStart::Number(number) => Some(*number),
                BlockStart::Hash(hash) => number_of(hash)?,
            };
            let Some(start) = start else {
                return Ok(run.items);
            };
            if *step == 1 {
                // Consecutive headers are neighbours in the keyspace: one scan.
                let key = start.to_be_bytes();
                let scan: Box<dyn Iterator<Item = Guard>> = if *rising {
                    Box::new(snapshot.range(&tables.headers, key..))
                } else {
                    Box::new(snapshot.range(&tables.headers, ..=key).rev())
                };
                let mut expected = Some(start);
                for guard in scan {
                    let (key, header) = guard.into_inner()?;
                    let number = decode_number(&key)?;
                    // The first key at or past `start` is another block if `start` is not held.
                    if Some(number) != expected
                        || number < limits.lowest
                        || !run.push(&header, "header")?
                    {
                        break;
                    }
                    expected = expected.and_then(|number| step_from(number, 1, *rising));
                }
            } else {
                let mut next = Some(start).filter(|start| *start >= limits.lowest);
                while let Some(number) = next {
                    let Some(header) = snapshot.get(&tables.headers, number.to_be_bytes())? else {
                        break;
                    };
                    if !run.push(&header, "header")? {
                        break;
                    }
                    next =
                        step_from(number, *step, *rising).filter(|number| *number >= limits.lowest);
                }
            }
        }
        BlockRead::Bodies(hashes) | BlockRead::Receipts(hashes) => {
            let (keyspace, what) = if matches!(read, BlockRead::Bodies(_)) {
                (&tables.bodies, "body")
            } else {
                (&tables.receipts, "receipts")
            };
            for hash in hashes {
                let Some(number) = number_of(hash)?.filter(|number| *number >= limits.lowest)
                else {
                    break;
                };
                let Some(value) = snapshot.get(keyspace, number.to_be_bytes())? else {
                    break;
                };
                if !run.push(&value, what)? {
                    break;
                }
            }
        }
    }
    Ok(run.items)
}

/// The block `step` blocks after `number` in a run, if there is one.
const fn step_from(number: BlockNumber, step: u64, rising: bool) -> Option<BlockNumber> {
    if rising {
        number.checked_add(step)
    } else {
        number.checked_sub(step)
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
    /// Decompresses a stored value, converts it if asked, and adds it. Returns whether the
    /// run may take another item: within the limits, and the conversion did not refuse.
    fn push(&mut self, compressed: &[u8], what: &'static str) -> Result<bool, StorageError> {
        if self.items.len() >= self.limits.items {
            return Ok(false);
        }
        let raw = decompress(compressed, what, None)?;
        let item = match self.convert {
            Some(convert) => match convert(&raw) {
                Some(item) => item,
                None => return Ok(false),
            },
            None => raw.into(),
        };
        self.bytes = self.bytes.saturating_add(item.len());
        self.items.push(item);
        Ok(self.items.len() < self.limits.items && self.bytes < self.limits.bytes)
    }
}

/// The number of the archived block with `hash`, on one snapshot.
pub(super) fn number_of(tables: &Tables, hash: BlockHash) -> Result<Option<BlockNumber>, Failure> {
    let snapshot = tables.db.snapshot();
    let Some(number) = snapshot.get(&tables.numbers, hash.0)? else {
        return Ok(None);
    };
    let number = decode_number(&number)?;
    // Only a block whose header is held: a bulk load's leftovers above the tip are not.
    let held = snapshot
        .get(&tables.headers, number.to_be_bytes())?
        .is_some();
    Ok(held.then_some(number))
}

/// Reads whole blocks from `from` upwards on one snapshot, decompressed: header, body,
/// receipts if set, and senders. The run ends at the first block not held, below
/// `limits.lowest`, or at `limits` (the bytes of header, body and receipts count).
pub(super) fn blocks(
    tables: &Tables,
    from: BlockNumber,
    limits: ReadLimits,
) -> Result<Vec<ArchivedBlock>, Failure> {
    let snapshot = tables.db.snapshot();
    let mut blocks = Vec::new();
    let mut bytes: usize = 0;
    if from < limits.lowest {
        return Ok(blocks);
    }
    for guard in snapshot.range(&tables.headers, from.to_be_bytes()..) {
        if blocks.len() >= limits.items || bytes >= limits.bytes {
            break;
        }
        let (key, header) = guard.into_inner()?;
        let number = decode_number(&key)?;
        let expected = from.checked_add(u64::try_from(blocks.len()).unwrap_or(u64::MAX));
        if Some(number) != expected {
            break;
        }
        let header = decompress(&header, "header", None)?;
        let hash = keccak256(&header);
        let body = snapshot
            .get(&tables.bodies, key.clone())?
            .ok_or_else(|| missing("body", hash))?;
        let body = decompress(&body, "body", Some(hash))?;
        let receipts = snapshot
            .get(&tables.receipts, key.clone())?
            .map(|receipts| decompress(&receipts, "receipts", Some(hash)))
            .transpose()?;
        let senders = snapshot
            .get(&tables.senders, key)?
            .ok_or_else(|| missing("senders", hash))?;
        let senders = decode_senders(&senders, hash)?;
        bytes = bytes
            .saturating_add(header.len())
            .saturating_add(body.len())
            .saturating_add(receipts.as_ref().map_or(0, Vec::len));
        blocks.push(ArchivedBlock {
            encoded: EncodedBlock {
                hash,
                header: header.into(),
                body: body.into(),
                receipts: receipts.map(Into::into),
            },
            senders,
        });
    }
    Ok(blocks)
}

/// The archived blocks without receipts from `from` up, then from the lowest, at most `limit`,
/// and how many there are in all (counted over the index, which is small: imported blocks
/// always have receipts).
pub(super) fn pending_receipts(
    tables: &Tables,
    from: BlockNumber,
    limit: usize,
) -> Result<(Vec<BlockRef>, u64), Failure> {
    let snapshot = tables.db.snapshot();
    let mut blocks = Vec::new();
    let mut total: u64 = 0;
    let from = from.to_be_bytes();
    let above = snapshot.range(&tables.pending, from..);
    let below = snapshot.range(&tables.pending, ..from);
    for guard in above.chain(below) {
        total = total.saturating_add(1);
        if blocks.len() < limit {
            let (key, hash) = guard.into_inner()?;
            blocks.push(BlockRef {
                number: decode_number(&key)?,
                hash: BlockHash::try_from(&*hash)
                    .map_err(|_wrong_length| invalid("pending receipts hash", None))?,
            });
        }
    }
    Ok((blocks, total))
}

/// The committed heads recorded by [`set_heads`].
pub(super) fn heads(tables: &Tables) -> Result<L1Heads, Failure> {
    let snapshot = tables.db.snapshot();
    let head = |key: &str| -> Result<Option<BlockRef>, Failure> {
        let Some(value) = snapshot.get(&tables.meta, key)? else {
            return Ok(None);
        };
        let head = BlockRef::from_bytes(&value).ok_or(invalid("head", None))?;
        Ok(Some(head))
    };
    Ok(L1Heads {
        safe: head(SAFE_HEAD_KEY)?,
        finalized: head(FINALIZED_HEAD_KEY)?,
    })
}

/// Records `heads` in one durable batch; a `None` head leaves the recorded one in place.
pub(super) fn set_heads(tables: &Tables, heads: L1Heads) -> Result<(), Failure> {
    let mut batch = tables.durable_batch();
    for (key, head) in [
        (SAFE_HEAD_KEY, heads.safe),
        (FINALIZED_HEAD_KEY, heads.finalized),
    ] {
        if let Some(head) = head {
            batch.insert(&tables.meta, key, head.to_bytes());
        }
    }
    batch.commit()?;
    Ok(())
}

/// The senders as stored: 20 bytes each, concatenated.
fn encode_senders(senders: &[Address]) -> Vec<u8> {
    senders.iter().flat_map(|sender| sender.0.0).collect()
}

fn decode_senders(bytes: &[u8], block: BlockHash) -> Result<Vec<Address>, StorageError> {
    let (addresses, rest) = bytes.as_chunks::<ADDRESS_LEN>();
    if !rest.is_empty() {
        return Err(invalid("senders", Some(block)));
    }
    Ok(addresses.iter().map(Address::from).collect())
}

/// The first and last archived block, from the first and last keys of `headers` in one
/// snapshot.
pub(super) fn range(tables: &Tables) -> Result<Option<(BlockRef, BlockRef)>, Failure> {
    let snapshot = tables.db.snapshot();
    let first = end_ref(snapshot.first_key_value(&tables.headers))?;
    let last = end_ref(snapshot.last_key_value(&tables.headers))?;
    Ok(first.zip(last))
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

/// A block number stored as a key or value: a big-endian `u64`.
fn decode_number(bytes: &[u8]) -> Result<BlockNumber, StorageError> {
    <[u8; 8]>::try_from(bytes)
        .map(u64::from_be_bytes)
        .map_err(|_wrong_length| invalid("block number", None))
}

/// Checks that `block`, whose header names `parent_hash`, is the child of `parent`.
fn extends(block: BlockRef, parent_hash: BlockHash, parent: BlockRef) -> Result<(), StorageError> {
    // The parent the block claims. Block 0 has none, so it never extends anything; its `got`
    // is reported at number 0.
    let number = block.number.checked_sub(1);
    let got = BlockRef {
        number: number.unwrap_or(0),
        hash: parent_hash,
    };
    if number.is_none() || got != parent {
        return Err(StorageError::NotContiguous {
            expected: parent,
            got,
        });
    }
    Ok(())
}

/// A block's header, body and receipts as stored: compressed.
type Compressed = (Vec<u8>, Vec<u8>, Option<Vec<u8>>);

/// The stored values of block `number`: its header, body and receipts (RLP), compressed.
fn compress_values(
    number: BlockNumber,
    header: &[u8],
    body: &[u8],
    receipts: Option<&[u8]>,
) -> Result<Compressed, StorageError> {
    Ok((
        compress(header, number)?,
        compress(body, number)?,
        receipts
            .map(|receipts| compress(receipts, number))
            .transpose()?,
    ))
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

/// Stored data that is present but not what the layout promises, with nothing to say why.
const fn invalid(what: &'static str, block: Option<BlockHash>) -> StorageError {
    StorageError::InvalidData {
        store: Store::Archive,
        what,
        block,
        source: None,
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
