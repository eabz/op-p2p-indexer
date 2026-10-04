//! The importer's bulk load: blocks prepared off the writer, written straight into new tables
//! and blob files with fjall's ingestion, bypassing the journal and the memtables.
//!
//! Each keyspace takes its part of a list in one ingestion, all four at once on their own
//! threads. An ingestion is registered atomically, and durably, by its `finish`: its tables and
//! blob files are synced before the keyspace's version that lists them is. `headers` is
//! finished last, after the other three, and its last key is the archive's tip, so a crash
//! leaves the archive holding a contiguous prefix; the other keyspaces may then hold the
//! blocks of the unfinished list above the tip, which nothing reads (every read goes through
//! a header or ends at one) and which the next load of the same blocks writes again with the
//! same values.
//!
//! Does not check what a block contains: [`PreparedBlock::new`] checks the header's hash, the
//! writer checks the chain.

use std::panic::resume_unwind;
use std::thread;

use alloy_primitives::BlockHash;
use fjall::{Keyspace, UserKey, UserValue};
use op_indexer_primitives::BlockRef;

use super::{Failure, Tables, compress, end_ref, record_usage};
use crate::{StorageError, Store, metrics};

/// A block ready for [`super::FjallArchive::bulk_append`](crate::archive_store::FjallArchive::bulk_append):
/// its header's hash checked, its number and parent read, its values compressed.
#[derive(Debug)]
pub(in crate::archive_store) struct Prepared {
    pub(in crate::archive_store) block: BlockRef,
    pub(in crate::archive_store) parent_hash: BlockHash,
    header: Vec<u8>,
    body: Vec<u8>,
    receipts: Option<Vec<u8>>,
}

impl Prepared {
    /// Compresses the values of `block`, whose number, hash and parent are already known.
    pub(in crate::archive_store) fn new(
        block: BlockRef,
        parent_hash: BlockHash,
        header: &[u8],
        body: &[u8],
        receipts: Option<&[u8]>,
    ) -> Result<Self, StorageError> {
        let number = block.number;
        Ok(Self {
            block,
            parent_hash,
            header: compress(header, number)?,
            body: compress(body, number)?,
            receipts: receipts
                .map(|receipts| compress(receipts, number))
                .transpose()?,
        })
    }

    /// The bytes this block adds to the archive's keyspaces, before their overhead.
    pub(in crate::archive_store) fn stored_len(&self) -> usize {
        let receipts = self.receipts.as_ref().map_or(0, Vec::len);
        self.header.len() + self.body.len() + receipts
    }
}

/// Appends `blocks`, which must be consecutive, oldest first, and extend the archive's tip (or
/// the archive is empty), in one ingestion per keyspace.
///
/// Holds the writer lock throughout: ingestion is only correct without concurrent writes to
/// the same keyspace.
pub(in crate::archive_store) fn bulk_append(
    tables: &Tables,
    blocks: &[Prepared],
) -> Result<(), Failure> {
    if blocks.is_empty() {
        return Ok(());
    }
    let _writer = tables.lock();
    let mut parent = end_ref(tables.headers.last_key_value())?;
    for block in blocks {
        // An empty archive takes any first block.
        if let Some(expected) = parent {
            let number = block.block.number.checked_sub(1);
            let got = BlockRef {
                number: number.unwrap_or(0),
                hash: block.parent_hash,
            };
            if number.is_none() || got != expected {
                return Err(StorageError::NotContiguous { expected, got }.into());
            }
        }
        parent = Some(block.block);
    }

    // Hash order for `numbers`. Hashes are distinct (keccak), but a duplicate must not reach
    // the ingestion, which requires strictly ascending keys.
    let mut numbers: Vec<([u8; 32], [u8; 8])> = blocks
        .iter()
        .map(|block| (block.block.hash.0, block.block.number.to_be_bytes()))
        .collect();
    numbers.sort_unstable();
    numbers.dedup_by_key(|(hash, _)| *hash);

    let by_number = |keyspace: &Keyspace, value: fn(&Prepared) -> Option<&[u8]>| {
        let entries = blocks
            .iter()
            .filter_map(|block| Some((block.block.number.to_be_bytes(), value(block)?)));
        ingest(keyspace, entries)
    };
    thread::scope(|scope| {
        let ingestions = [
            scope.spawn(|| by_number(&tables.bodies, |block| Some(&block.body))),
            scope.spawn(|| by_number(&tables.receipts, |block| block.receipts.as_deref())),
            scope.spawn(|| ingest(&tables.numbers, numbers.iter().copied())),
        ];
        for ingestion in ingestions {
            ingestion
                .join()
                .unwrap_or_else(|panic| resume_unwind(panic))?;
        }
        Ok::<_, Failure>(())
    })?;
    // Last: once it is registered, the blocks are in the archive.
    by_number(&tables.headers, |block| Some(&block.header))?;

    metrics::blocks_inserted(Store::Archive, blocks.len());
    record_usage(tables);
    Ok(())
}

/// Writes `entries`, in ascending key order, into `keyspace` in one ingestion.
fn ingest<K: Into<UserKey>, V: Into<UserValue>>(
    keyspace: &Keyspace,
    entries: impl Iterator<Item = (K, V)>,
) -> Result<(), Failure> {
    let mut ingestion = keyspace.start_ingestion()?;
    for (key, value) in entries {
        ingestion.write(key, value)?;
    }
    ingestion.finish()?;
    Ok(())
}
