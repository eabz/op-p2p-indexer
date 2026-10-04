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
//! Does not check what a block contains: `PreparedBlock::new` checks the header's hash, the
//! writer checks the chain.

use std::panic::resume_unwind;
use std::sync::mpsc;
use std::thread;

use alloy_primitives::{BlockHash, Bytes};
use fjall::{Keyspace, UserKey, UserValue};
use op_indexer_primitives::BlockRef;

use super::{Failure, Tables, compress, end_ref, record_usage};
use crate::{StorageError, Store, metrics};

/// A block ready for [`FjallArchive::bulk_append`](crate::archive_store::FjallArchive::bulk_append):
/// its header's hash checked, its number and parent read, its values compressed.
#[derive(Debug, Clone)]
pub(in crate::archive_store) struct Prepared {
    pub(in crate::archive_store) block: BlockRef,
    pub(in crate::archive_store) parent_hash: BlockHash,
    header: Bytes,
    body: Bytes,
    receipts: Option<Bytes>,
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
            header: compress(header, number)?.into(),
            body: compress(body, number)?.into(),
            receipts: receipts
                .map(|receipts| compress(receipts, number).map(Bytes::from))
                .transpose()?,
        })
    }

    /// The bytes this block adds to the archive's keyspaces, before their overhead.
    pub(in crate::archive_store) fn stored_len(&self) -> usize {
        let receipts = self.receipts.as_ref().map_or(0, |receipts| receipts.len());
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

    let by_number = |value: fn(&Prepared) -> Option<&[u8]>| {
        blocks
            .iter()
            .filter_map(move |block| Some((block.block.number.to_be_bytes(), value(block)?)))
    };
    // `headers` is written alongside the others but registered only once they are: it waits
    // for word that they all were, and is dropped unregistered otherwise.
    let (others_done, all_done) = mpsc::sync_channel::<()>(1);
    thread::scope(|scope| {
        let headers = scope.spawn(move || {
            let mut ingestion = tables.headers.start_ingestion()?;
            for (key, value) in by_number(|block| Some(&block.header)) {
                ingestion.write(key, value)?;
            }
            if all_done.recv().is_ok() {
                ingestion.finish()?;
            }
            Ok::<_, Failure>(())
        });
        let others = [
            scope.spawn(|| ingest(&tables.bodies, by_number(|block| Some(&block.body)))),
            scope.spawn(|| {
                let receipts =
                    by_number(|block| block.receipts.as_ref().map(|receipts| &receipts[..]));
                ingest(&tables.receipts, receipts)
            }),
            scope.spawn(|| ingest(&tables.numbers, numbers.iter().copied())),
        ];
        let mut result = Ok(());
        for ingestion in others {
            let joined = ingestion
                .join()
                .unwrap_or_else(|panic| resume_unwind(panic));
            result = result.and(joined);
        }
        if result.is_ok() {
            // The headers thread is waiting for it: the send cannot fail.
            let _sent = others_done.send(());
        }
        drop(others_done);
        let headers = headers.join().unwrap_or_else(|panic| resume_unwind(panic));
        result.and(headers)
    })?;

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
