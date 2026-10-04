//! Appending blocks to the archive: the contiguity checks and the batched, durable writes.
//!
//! Does not validate what a block contains: the caller hands in bytes it has verified.

use alloy_primitives::{BlockHash, Bytes};
use op_indexer_primitives::BlockRef;

use super::{Failure, Tables, compress, decode_number, end_ref, record_usage};
use crate::{StorageError, Store, metrics};

/// Most encoded bytes written in one batch by an append of many blocks (a single larger block
/// still goes alone): an eighth of the journal limit, and one memtable. It bounds one journal
/// record and how long other writers wait for the lock. There is no limit on the number of
/// blocks: each batch is one synced commit, and small blocks (the legacy chain's are about
/// 2 KB) would otherwise cost several commits where one will do.
const MAX_APPEND_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// A block encoded for the archive: RLP, not yet compressed.
#[derive(Debug)]
pub(in crate::archive_store) struct Entry {
    pub(in crate::archive_store) block: BlockRef,
    pub(in crate::archive_store) parent_hash: BlockHash,
    pub(in crate::archive_store) header: Bytes,
    pub(in crate::archive_store) body: Bytes,
    pub(in crate::archive_store) receipts: Option<Bytes>,
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

/// Appends `blocks`, which must be consecutive, oldest first, and extend the held range (or the
/// archive is empty). Leading blocks the archive holds, up to its tip, are skipped.
///
/// Everything is checked before the first write. The blocks are then written in chunks, one
/// durable batch and one turn at the writer lock each, so a long list neither builds one huge
/// journal record nor keeps other writers out for its whole length.
pub(in crate::archive_store) fn append_batch(
    tables: &Tables,
    blocks: &[Entry],
) -> Result<(), Failure> {
    for pair in blocks.windows(2) {
        if let [parent, child] = pair {
            child.extends(parent.block)?;
        }
    }
    // Read without the lock: a writer getting in between shows as `NotContiguous` below.
    let tip = end_ref(tables.headers.last_key_value())?;
    let mut rest = match tip {
        Some(tip) => above(tables, blocks, tip)?,
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

/// The blocks of `blocks` (consecutive) above `tip`: all of them if the first extends it,
/// those after the tip if the tip is among them, and none if the list ends at or below the tip
/// and its last block is held (the archive is one chain, so the blocks before it are too).
fn above<'a>(tables: &Tables, blocks: &'a [Entry], tip: BlockRef) -> Result<&'a [Entry], Failure> {
    let (Some(first), Some(last)) = (blocks.first(), blocks.last()) else {
        return Ok(blocks);
    };
    if first.block.number > tip.number {
        first.extends(tip)?;
        return Ok(blocks);
    }
    // The last block the archive should already hold: the one at the tip's height, or the
    // last of a list that ends below it.
    let overlap = tip.number.min(last.block.number);
    let held = overlap
        .checked_sub(first.block.number)
        .and_then(|offset| usize::try_from(offset).ok())
        .and_then(|offset| Some((blocks.get(offset)?, blocks.get(offset.checked_add(1)?..)?)));
    let Some((at_overlap, rest)) = held else {
        return Ok(blocks);
    };
    let stored = tables.numbers.get(at_overlap.block.hash.0)?;
    let stored = stored.as_deref().map(decode_number).transpose()?;
    if stored != Some(at_overlap.block.number) {
        return Err(StorageError::NotContiguous {
            expected: tip,
            got: at_overlap.block,
        }
        .into());
    }
    Ok(rest)
}

/// How many of `blocks` go into one batch: as many as fit [`MAX_APPEND_BATCH_BYTES`], and
/// always at least one.
fn chunk_len(blocks: &[Entry]) -> usize {
    let mut bytes = 0_usize;
    blocks
        .iter()
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
