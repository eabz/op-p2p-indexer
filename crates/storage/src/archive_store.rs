//! Local block archive on fjall: the committed store. A contiguous range of committed blocks,
//! kept in the encoding execution-network peers ask for, with each transaction's sender and
//! the committed L1 heads.
//!
//! - [`FjallArchive`]: the [`ArchiveStore`] implementation, cheap to clone.
//! - `tables`: the fjall keyspaces and the synchronous operations on them.
//!
//! Values are snappy-compressed RLP: the header as `alloy_consensus::Header` encodes it (its
//! keccak is the block hash), the body as `alloy_consensus::BlockBody<OpTxEnvelope>` encodes it
//! (transactions in network encoding, ommers, optional withdrawals: one `BlockBodies` entry), and
//! the receipts as an RLP list of `OpReceiptEnvelope` network encodings (with bloom: one
//! `Receipts` entry up to eth/68). Senders are 20-byte addresses, one per transaction,
//! uncompressed. The message shapes are those of the devp2p eth protocol:
//! <https://github.com/ethereum/devp2p/blob/master/caps/eth.md>.
//!
//! Does not decide which blocks are archived: the caller appends. Nothing removes blocks; the
//! archive keeps every block it is given.
//!
//! **Space.** fjall is log-structured: a value written again (receipts set, a block appended
//! twice) leaves a stale copy, and the space comes back later, when background compaction
//! rewrites the trees and drops blob files nothing references. Disk use therefore runs above
//! the live data by roughly a few blob files (64 MiB each) plus the journal (at most 128 MiB):
//! a constant. In a small archive that constant dominates; in a full history it is a few
//! percent. The `archive_*` gauges show disk use, stale blob bytes and running compactions
//! after each write.

mod tables;

use std::fmt;
use std::path::Path;

use alloy_consensus::Header;
use alloy_primitives::{Address, BlockHash, BlockNumber, Bytes, keccak256};
use alloy_rlp::Decodable;
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{
    ArchivedBlock, BlockRead, BlockRef, ChainIdentity, EncodedBlock, ItemConvert, L1Heads,
    ReadLimits, encode_receipts, split_body,
};

use tracing::debug;

use self::tables::{Entry, Failure, Tables};
use crate::metrics::{self, Operation};
use crate::{ArchiveStore, InvalidBlockReason, StorageError, Store};

/// The block archive in one fjall database directory. Cheap to clone: clones share the open
/// database.
///
/// Every call runs on a blocking thread, so the futures never block the runtime. Dropping a
/// future does not cancel its blocking call: an append or `set_receipts` still completes.
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
    /// Opens the archive of `chain` in the directory `path`, creating it and its keyspaces if
    /// needed. fjall locks the directory, so one process opens it at a time.
    ///
    /// The archive records its chain when it is created. One with no record (made by a build
    /// before the record) is taken to hold OP Mainnet's ([`ChainIdentity::BEFORE_RECORD`]) if
    /// it holds blocks, and `chain`'s if it is empty; that chain is recorded then.
    ///
    /// Does blocking disk I/O, including replaying the journal after a crash: call it at startup
    /// or from a blocking thread.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::ArchiveSchema`] if the directory holds an archive of another
    /// schema version, [`StorageError::ArchiveChain`] if it holds another chain's,
    /// [`StorageError::ArchiveChainUnreadable`] if its chain record does not decode (each is
    /// left as it is), and [`StorageError::Fjall`] if the directory cannot be created, opened,
    /// locked or written.
    pub fn open(path: &Path, chain: ChainIdentity) -> Result<Self, StorageError> {
        let tables =
            tables::open(path, chain).map_err(|failure| failure.into_storage_error("open"))?;
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
    /// Removes every block below `first_kept` (header, body, receipts, senders, its hash entry
    /// and its pending-receipts entry), for an archive that keeps only a tail of the chain.
    /// The recorded heads stay. Afterwards [`ArchiveStore::range`] starts at `first_kept`, or
    /// is `None` if nothing is left, and appends continue above the tip as before; an emptied
    /// archive takes any block next. Durable, in batches that take turns with appends; a call
    /// cut short is finished by the next.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Fjall`] if the archive cannot be written, and
    /// [`StorageError::InvalidData`] if a header to remove does not decompress.
    pub async fn prune_below(&self, first_kept: BlockNumber) -> Result<(), StorageError> {
        let removed = self
            .blocking(Operation::Prune, "prune_below", move |tables| {
                tables::prune_below(tables, first_kept)
            })
            .await?;
        debug!(first_kept, removed, "archive pruned below a block");
        Ok(())
    }
}

impl ArchiveStore for FjallArchive {
    /// Hashes, compresses and writes on a blocking thread. One batch holds at most 16 MiB of
    /// RLP, and the writer lock is taken once per batch.
    async fn append_batch(&self, blocks: Vec<ArchivedBlock>) -> Result<(), StorageError> {
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
        let count = receipts.len();
        let receipts = encode_receipts(receipts);
        self.blocking(Operation::SetReceipts, "set_receipts", move |tables| {
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

    async fn blocks(
        &self,
        from: BlockNumber,
        limits: ReadLimits,
    ) -> Result<Vec<ArchivedBlock>, StorageError> {
        self.blocking(Operation::Blocks, "blocks", move |tables| {
            tables::blocks(tables, from, limits)
        })
        .await
    }

    async fn pending_receipts(
        &self,
        from: BlockNumber,
        limit: usize,
    ) -> Result<(Vec<BlockRef>, u64), StorageError> {
        self.blocking(
            Operation::PendingReceipts,
            "pending_receipts",
            move |tables| tables::pending_receipts(tables, from, limit),
        )
        .await
    }

    async fn heads(&self) -> Result<L1Heads, StorageError> {
        self.blocking(Operation::Heads, "heads", tables::heads)
            .await
    }

    async fn set_heads(&self, heads: L1Heads) -> Result<(), StorageError> {
        self.blocking(Operation::SetL1Heads, "set_heads", move |tables| {
            tables::set_heads(tables, heads)
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
}

/// The archive entry of a block handed over in its original encoding: its bytes unchanged, with
/// the number and parent hash read from the header, which must hash to the block's hash.
fn entry(block: ArchivedBlock) -> Result<Entry, StorageError> {
    let checked = checked(&block.encoded, &block.senders)?;
    let ArchivedBlock {
        encoded:
            EncodedBlock {
                hash,
                header,
                body,
                receipts,
            },
        senders,
    } = block;
    Ok(Entry {
        block: BlockRef {
            number: checked.number,
            hash,
        },
        parent_hash: checked.parent_hash,
        header,
        body,
        receipts,
        senders,
    })
}

/// The decoded header of `block`, which must hash to the block's hash, after checking that
/// `senders` are one per transaction of its body (cut, not decoded).
fn checked(block: &EncodedBlock, senders: &[Address]) -> Result<Header, StorageError> {
    let header = checked_header(block.hash, &block.header)?;
    let body = split_body(&block.body).ok_or(StorageError::InvalidData {
        store: Store::Archive,
        what: "body to append",
        block: Some(block.hash),
        source: None,
    })?;
    if body.transactions.len() != senders.len() {
        return Err(StorageError::InvalidBlock {
            number: header.number,
            reason: InvalidBlockReason::SenderCount,
        });
    }
    Ok(header)
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
