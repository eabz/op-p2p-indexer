//! Local block archive on fjall: a contiguous range of committed blocks, kept in the encoding
//! execution-network peers ask for, so serving them never touches ClickHouse.
//!
//! - [`FjallArchive`]: the [`ArchiveStore`] implementation, cheap to clone.
//! - `tables`: the fjall keyspaces and the synchronous operations on them.
//!
//! Values are snappy-compressed RLP: the header as `alloy_consensus::Header` encodes it (its
//! keccak is the block hash), the body as `alloy_consensus::BlockBody<OpTxEnvelope>` encodes it
//! (transactions in network encoding, ommers, optional withdrawals: one `BlockBodies` entry), and
//! the receipts as an RLP list of `OpReceiptEnvelope` network encodings (with bloom: one
//! `Receipts` entry up to eth/68). Senders are not stored. The message shapes are those of the
//! devp2p eth protocol: <https://github.com/ethereum/devp2p/blob/master/caps/eth.md>.
//!
//! Does not decide which blocks are archived or how many are kept: the caller appends, truncates
//! and trims.
//!
//! **Space.** fjall is log-structured: [`ArchiveStore::trim`] and
//! [`ArchiveStore::truncate_above`] write tombstones, and the
//! space comes back later, when background compaction rewrites the trees and drops blob files
//! nothing references. A trim with no writes after it frees nothing; the space returns as
//! further appends trigger flushes and compactions. Appends always follow in this indexer, so
//! there is no forced compaction. Disk use therefore runs above the live data by roughly a few
//! blob files (64 MiB each) plus the journal (at most 128 MiB): a constant, not a multiple of
//! the window. With a tiny window that constant dominates; at the windows this archive is for,
//! it is a few percent. The `archive_*` gauges show disk use, stale blob bytes and running
//! compactions after each write.

mod tables;

use std::fmt;
use std::path::Path;

use alloy_consensus::Header;
use alloy_primitives::{BlockHash, BlockNumber, Bytes, keccak256};
use alloy_rlp::Decodable;
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{
    BlockRead, BlockRef, EncodedBlock, ItemConvert, ReadLimits, encode_receipts,
};

use self::tables::{Entry, Failure, Prepared, Tables};
use crate::metrics::{self, Operation};
use crate::validate::validate_receipts;
use crate::{ArchiveStore, InvalidBlockReason, StorageError, Store};

/// The block archive in one fjall database directory. Cheap to clone: clones share the open
/// database.
///
/// Every call runs on a blocking thread, so the futures never block the runtime. Dropping a
/// future does not cancel its blocking call: an append or `set_receipts` still completes, and a
/// `trim` or `truncate_above` keeps removing until it is done or its deadline (60 s) passes.
/// Single reads and writes have no timeout: a local disk either answers or the process has
/// bigger problems.
///
/// Shutdown needs no call: every write is synced to disk before it returns, and dropping the last
/// clone stops fjall's background threads (it also syncs its journal once more). A process killed
/// without dropping it loses nothing that was acknowledged; fjall replays its journal on the
/// next open.
#[derive(Clone)]
pub struct FjallArchive {
    tables: Tables,
}

impl fmt::Debug for FjallArchive {
    // fjall's handles are not `Debug`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FjallArchive").finish_non_exhaustive()
    }
}

impl FjallArchive {
    /// Opens the archive in the directory `path`, creating it and its keyspaces if needed.
    /// fjall locks the directory, so one process opens it at a time.
    ///
    /// Does blocking disk I/O, including replaying the journal after a crash: call it at startup
    /// or from a blocking thread.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::ArchiveSchema`] if the directory holds an archive of another
    /// schema version (it is left as it is), and [`StorageError::Fjall`] if the directory
    /// cannot be created, opened, locked or written.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let tables = tables::open(path).map_err(|failure| failure.into_storage_error("open"))?;
        Ok(Self { tables })
    }

    /// Runs `call` on a blocking thread with the keyspaces, timed as `operation`.
    async fn blocking<T, F>(
        &self,
        operation: Operation,
        name: &'static str,
        call: F,
    ) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&Tables) -> Result<T, Failure> + Send + 'static,
    {
        let tables = self.tables.clone();
        metrics::timed(Store::Archive, operation, async move {
            tokio::task::spawn_blocking(move || call(&tables))
                .await
                .map_err(|source| StorageError::BlockingTask {
                    operation: name,
                    source,
                })?
                .map_err(|failure| failure.into_storage_error(name))
        })
        .await
    }
}

impl FjallArchive {
    /// Appends blocks the importer prepared, for its bulk load only: written straight into
    /// new table and blob files, all keyspaces at once, without the journal. Several times
    /// faster than [`ArchiveStore::append_batch`] for long lists, and as durable when it
    /// returns; each call writes new files, so it is for lists of hundreds of megabytes, not
    /// a few blocks. A crash during a call leaves the archive as it was before it (see
    /// `tables::bulk`).
    ///
    /// `blocks` must be consecutive, oldest first, and extend the archive's last block (or
    /// the archive is empty). No other write may run at the same time; the writer lock
    /// ensures it in this process.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::NotContiguous`] if `blocks` do not extend the archive, and
    /// [`StorageError::Fjall`] if writing fails.
    pub async fn bulk_append(&self, blocks: Vec<PreparedBlock>) -> Result<(), StorageError> {
        self.blocking(Operation::AppendBatch, "bulk_append", move |tables| {
            let blocks: Vec<Prepared> = blocks.into_iter().map(|block| block.0).collect();
            tables::bulk_append(tables, &blocks)
        })
        .await
    }
}

/// A block checked and compressed for [`FjallArchive::bulk_append`], off the writer: its
/// header hashes to its hash, and its number and parent are read from it.
#[derive(Debug)]
pub struct PreparedBlock(Prepared);

impl PreparedBlock {
    /// Prepares `block`. CPU work only (decoding the header, hashing it, compressing the
    /// values): call it on a blocking thread, as many in parallel as there are cores.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidData`] if the header does not decode,
    /// [`StorageError::InvalidBlock`] if it does not hash to the block's hash, and
    /// [`StorageError::Oversized`] if a value is too large to compress.
    pub fn new(block: &EncodedBlock) -> Result<Self, StorageError> {
        let checked = checked_header(block.hash, &block.header)?;
        let prepared = Prepared::new(
            BlockRef {
                number: checked.number,
                hash: block.hash,
            },
            checked.parent_hash,
            &block.header,
            &block.body,
            block.receipts.as_ref().map(|receipts| &receipts[..]),
        )?;
        Ok(Self(prepared))
    }

    /// The block's number.
    #[must_use]
    pub const fn number(&self) -> BlockNumber {
        self.0.block.number
    }

    /// The compressed bytes the block adds to the archive.
    #[must_use]
    pub fn stored_len(&self) -> usize {
        self.0.stored_len()
    }
}

impl ArchiveStore for FjallArchive {
    /// Hashes, compresses and writes on a blocking thread. One batch holds at most 16 MiB of
    /// RLP, and the writer lock is taken once per batch.
    async fn append_batch(&self, blocks: Vec<EncodedBlock>) -> Result<(), StorageError> {
        self.blocking(Operation::AppendBatch, "append_batch", move |tables| {
            let entries: Vec<Entry> = blocks.into_iter().map(entry).collect::<Result<_, _>>()?;
            tables::append_batch(tables, &entries)
        })
        .await
    }

    async fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> Result<bool, StorageError> {
        // The error goes into the timed call on purpose, so invalid receipts are counted.
        let checked = validate_receipts(block.number, receipts);
        let count = receipts.len();
        let receipts = encode_receipts(receipts);
        self.blocking(Operation::SetReceipts, "set_receipts", move |tables| {
            checked?;
            tables::set_receipts(tables, block, &receipts, count)
        })
        .await
    }

    async fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> Result<Vec<Bytes>, StorageError> {
        self.blocking(Operation::Read, "read", move |tables| {
            tables::read(tables, &read, limits, convert)
        })
        .await
    }

    async fn number_of(&self, hash: BlockHash) -> Result<Option<BlockNumber>, StorageError> {
        self.blocking(Operation::NumberOf, "number_of", move |tables| {
            tables::number_of(tables, hash)
        })
        .await
    }

    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError> {
        self.blocking(Operation::Range, "range", tables::range)
            .await
    }

    async fn truncate_above(&self, number: BlockNumber) -> Result<(), StorageError> {
        self.blocking(Operation::TruncateAbove, "truncate_above", move |tables| {
            tables::truncate_above(tables, number)
        })
        .await
    }

    async fn trim(&self, retain: u64) -> Result<u64, StorageError> {
        self.blocking(Operation::Trim, "trim", move |tables| {
            tables::trim(tables, retain)
        })
        .await
    }
}

/// The archive entry of a block handed over in its original encoding: its bytes unchanged, with
/// the number and parent hash read from the header, which must hash to the block's hash.
fn entry(block: EncodedBlock) -> Result<Entry, StorageError> {
    let EncodedBlock {
        hash,
        header,
        body,
        receipts,
    } = block;
    let checked = checked_header(hash, &header)?;
    Ok(Entry {
        block: BlockRef {
            number: checked.number,
            hash,
        },
        parent_hash: checked.parent_hash,
        header,
        body,
        receipts,
    })
}

/// The decoded `header`, which must hash to `hash`.
fn checked_header(hash: BlockHash, header: &[u8]) -> Result<Header, StorageError> {
    let decoded = Header::decode(&mut &header[..]).map_err(|source| StorageError::InvalidData {
        store: Store::Archive,
        what: "header to append",
        block: Some(hash),
        source: Some(source.into()),
    })?;
    if keccak256(header) != hash {
        return Err(StorageError::InvalidBlock {
            number: decoded.number,
            reason: InvalidBlockReason::HeaderHash,
        });
    }
    Ok(decoded)
}
