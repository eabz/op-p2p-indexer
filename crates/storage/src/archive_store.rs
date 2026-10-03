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

use alloy_primitives::{BlockHash, BlockNumber, keccak256};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{ArchivedBlock, BlockRef, DecodedBlock};
use tracing::warn;

use self::tables::{EncodedBlock, Failure, Tables};
use crate::metrics::{self, Operation};
use crate::validate::{validate_block, validate_receipts};
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
    /// Opens the archive in the directory `path`, creating it and its keyspaces if needed. An
    /// archive written with another schema version is emptied, with a warning: it can be rebuilt
    /// from the committed store. fjall locks the directory, so one process opens it at a time.
    ///
    /// Does blocking disk I/O, including replaying the journal after a crash: call it at startup
    /// or from a blocking thread.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Fjall`] if the directory cannot be created, opened, locked or
    /// written.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let (tables, emptied) =
            tables::open(path).map_err(|failure| failure.into_storage_error("open"))?;
        if let Some(version) = emptied {
            warn!(
                path = %path.display(),
                %version,
                "block archive had another schema version; emptied it"
            );
        }
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

impl ArchiveStore for FjallArchive {
    /// Validates and encodes the block on the calling task, then compresses and writes it in
    /// one durable batch on a blocking thread.
    async fn append(&self, block: &DecodedBlock) -> Result<(), StorageError> {
        // The error goes into the timed call on purpose, so an invalid block is counted.
        let encoded = encode(block);
        self.blocking(Operation::Append, "append", move |tables| {
            tables::append(tables, &encoded?)
        })
        .await
    }

    async fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> Result<bool, StorageError> {
        // As in `append`: the error goes into the timed call, so invalid receipts are counted.
        let checked = validate_receipts(block.number, receipts);
        let count = receipts.len();
        let receipts = encode_receipts(receipts);
        self.blocking(Operation::SetReceipts, "set_receipts", move |tables| {
            checked?;
            tables::set_receipts(tables, block, &receipts, count)
        })
        .await
    }

    async fn block(&self, number: BlockNumber) -> Result<Option<ArchivedBlock>, StorageError> {
        self.blocking(Operation::Block, "block", move |tables| {
            tables::block(tables, number)
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

/// Validates `block` and encodes it as RLP, checking that the header hashes to its hash.
fn encode(block: &DecodedBlock) -> Result<EncodedBlock, StorageError> {
    validate_block(block)?;
    let header = &block.block.header;
    let header_rlp = alloy_rlp::encode(header);
    if keccak256(&header_rlp) != block.hash {
        return Err(StorageError::InvalidBlock {
            number: header.number,
            reason: InvalidBlockReason::HeaderHash,
        });
    }
    Ok(EncodedBlock {
        number: header.number,
        hash: block.hash,
        parent_hash: header.parent_hash,
        header: header_rlp,
        body: alloy_rlp::encode(&block.block.body),
        receipts: block.receipts.as_deref().map(encode_receipts),
    })
}

/// The RLP list of `receipts` in network encoding.
fn encode_receipts(receipts: &[OpReceiptEnvelope]) -> Vec<u8> {
    let mut out = Vec::new();
    alloy_rlp::encode_list(receipts, &mut out);
    out
}
