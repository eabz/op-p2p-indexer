//! The importer's bulk load: blocks prepared off the writer, written straight into new tables
//! and blob files with fjall's ingestion, bypassing the journal and the memtables.
//!
//! Each keyspace takes its part of a list in one ingestion, all four at once on their own
//! threads. An ingestion is registered atomically, and durably, by its `finish`: its tables and
//! blob files are synced before the keyspace's version that lists them is. `headers` is
//! finished last, after the other three, and its last key is the archive's tip, so a crash
//! leaves the archive holding a contiguous prefix.
//!
//! The other keyspaces may then hold blocks of the unfinished list above the tip. Reads by
//! number stop at the tip, but a read of bodies or receipts by hash, `number_of` and
//! `set_receipts` find them; `trim` and `truncate_above` walk `headers` and do not remove
//! them. They are verified blocks of the chain the archive holds, and the next load writes
//! them again, with the same values, and then the header.
//!
//! Does not check what a block contains: `PreparedBlock::new` checks the header's hash, the
//! writer checks the chain.

use std::panic::resume_unwind;
use std::sync::mpsc;
use std::thread;

use alloy_primitives::BlockHash;
use fjall::{Keyspace, UserKey, UserValue};
use op_indexer_primitives::BlockRef;

use super::{Failure, Tables, compress_values, end_ref, extends, record_usage};
use crate::{StorageError, Store, metrics};

/// A block ready for [`FjallArchive::bulk_append`](crate::archive_store::FjallArchive::bulk_append):
/// its header's hash checked, its number and parent read, its values compressed into the
/// buffers the ingestion takes, so the writer copies nothing.
#[derive(Debug)]
pub(in crate::archive_store) struct Prepared {
    block: BlockRef,
    parent_hash: BlockHash,
    header: UserValue,
    body: UserValue,
    receipts: Option<UserValue>,
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
        let (header, body, receipts) = compress_values(block.number, header, body, receipts)?;
        Ok(Self {
            block,
            parent_hash,
            header: UserValue::from(&header[..]),
            body: UserValue::from(&body[..]),
            receipts: receipts.map(|receipts| UserValue::from(&receipts[..])),
        })
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
    // An empty archive takes any first block.
    let mut parent = end_ref(tables.headers.last_key_value())?;
    for block in blocks {
        if let Some(parent) = parent {
            extends(block.block, block.parent_hash, parent)?;
        }
        parent = Some(block.block);
    }

    let by_number = |value: fn(&Prepared) -> Option<&UserValue>| -> Entries<'_> {
        Box::new(blocks.iter().filter_map(move |block| {
            let key = UserKey::from(block.block.number.to_be_bytes());
            Some((key, value(block)?.clone()))
        }))
    };
    // `headers` is written alongside the others but finished only on word that they all
    // were (fjall's ingestion cannot move between threads, so it waits where it is); without
    // that word it is dropped unregistered.
    let (others_done, go) = mpsc::sync_channel::<()>(1);
    thread::scope(|scope| {
        let headers = scope.spawn(move || {
            let mut ingestion = tables.headers.start_ingestion()?;
            for (key, value) in by_number(|block| Some(&block.header)) {
                ingestion.write(key, value)?;
            }
            if go.recv().is_ok() {
                ingestion.finish()?;
            }
            Ok::<_, Failure>(())
        });
        let others = [
            scope.spawn(|| ingest(&tables.bodies, by_number(|block| Some(&block.body)))),
            scope.spawn(|| ingest(&tables.receipts, by_number(|block| block.receipts.as_ref()))),
            scope.spawn(|| {
                // Hash order. Hashes are distinct (keccak), but a duplicate must not reach the
                // ingestion, which requires strictly ascending keys.
                let mut numbers: Vec<([u8; 32], [u8; 8])> = blocks
                    .iter()
                    .map(|block| (block.block.hash.0, block.block.number.to_be_bytes()))
                    .collect();
                numbers.sort_unstable();
                numbers.dedup_by_key(|(hash, _)| *hash);
                let entries = numbers
                    .into_iter()
                    .map(|(hash, number)| (UserKey::from(hash), UserValue::from(number)));
                ingest(&tables.numbers, Box::new(entries))
            }),
        ];
        let mut result = Ok(());
        for ingestion in others {
            let joined = ingestion
                .join()
                .unwrap_or_else(|panic| resume_unwind(panic));
            result = result.and(joined);
        }
        if result.is_ok() {
            // Fails only if the headers thread already failed, which its result reports.
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

/// Keys and values for one ingestion, in ascending key order.
type Entries<'a> = Box<dyn Iterator<Item = (UserKey, UserValue)> + Send + 'a>;

/// Writes `entries` into `keyspace` in one ingestion and registers it.
fn ingest(keyspace: &Keyspace, entries: Entries<'_>) -> Result<(), Failure> {
    let mut ingestion = keyspace.start_ingestion()?;
    for (key, value) in entries {
        ingestion.write(key, value)?;
    }
    ingestion.finish()?;
    Ok(())
}
