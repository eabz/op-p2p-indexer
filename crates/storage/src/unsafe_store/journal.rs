//! The unsafe chain's journal: a small fjall database of its own (`unsafe/` in the data
//! directory), so a restart rebuilds the chain without the network.
//!
//! It holds every stored block, keyed by the order it was inserted in, and the L1 heads. A
//! block is written when it is stored, written again when its receipts arrive, and deleted when
//! a prune or retention removes it, so the journal never holds more than the chain does. A
//! restart replays the blocks in insertion order through fork choice, which rebuilds the same
//! chain. Writes are not synced one by one: a crash loses at most the last moments, which
//! gossip brings again.
//!
//! The database records its chain, as the archive does, and refuses another; one written with
//! another layout version is emptied, since unsafe blocks are disposable.

use std::path::Path;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use op_indexer_primitives::{BlockRef, BlockSource, ChainIdentity, EncodedBlock, L1Heads};

use super::chain::Stored;
use crate::{StorageError, Store};

/// Layout version of the journal. One written with another version is emptied on open.
const SCHEMA_VERSION: u64 = 1;
const SCHEMA_VERSION_KEY: &str = "schema_version";
const CHAIN_KEY: &str = "chain";
const SAFE_HEAD_KEY: &str = "safe_head";
const FINALIZED_HEAD_KEY: &str = "finalized_head";
/// Small: the chain is in memory, the journal is only read on open. Its memtable and cache
/// would otherwise hold a second copy of the blocks.
const CACHE_SIZE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_MEMTABLE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_JOURNAL_BYTES: u64 = 64 * 1024 * 1024;

/// The journal of one chain.
pub(super) struct Journal {
    db: Database,
    /// Insertion order (big-endian `u64`) → the block, as [`encode`] writes it.
    blocks: Keyspace,
    meta: Keyspace,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal").finish_non_exhaustive()
    }
}

/// What a write changes, in one batch.
#[derive(Debug, Default)]
pub(super) struct Changes<'a> {
    /// A block stored or changed.
    pub(super) put: Option<&'a Stored>,
    /// Blocks removed, by insertion order.
    pub(super) delete: Vec<u64>,
    /// The L1 heads, when they changed.
    pub(super) heads: Option<L1Heads>,
}

impl Journal {
    /// Opens the journal at `path`, creating it if needed. Blocking.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::UnsafeChain`] if it records another chain, and
    /// [`StorageError::Fjall`] if it cannot be opened or written.
    pub(super) fn open(path: &Path, chain: ChainIdentity) -> Result<Self, StorageError> {
        let db = Database::builder(path)
            .cache_size(CACHE_SIZE_BYTES)
            .max_journaling_size(MAX_JOURNAL_BYTES)
            .open()
            .map_err(fjall("open"))?;
        let options = || KeyspaceCreateOptions::default().max_memtable_size(MAX_MEMTABLE_BYTES);
        let journal = Self {
            blocks: db.keyspace("blocks", options).map_err(fjall("open"))?,
            meta: db.keyspace("meta", options).map_err(fjall("open"))?,
            db,
        };
        let version = journal
            .meta
            .get(SCHEMA_VERSION_KEY)
            .map_err(fjall("open"))?;
        if version.as_deref() != Some(&SCHEMA_VERSION.to_be_bytes()[..]) {
            journal.clear()?;
        }
        match journal.meta.get(CHAIN_KEY).map_err(fjall("open"))? {
            Some(found) if found.as_ref() != chain.to_bytes() => {
                let found = ChainIdentity::from_bytes(&found);
                return Err(StorageError::UnsafeChain {
                    path: path.to_owned(),
                    found: found.map(Box::new),
                    expected: Box::new(chain),
                });
            }
            Some(_) => {}
            None => {
                let mut batch = journal.db.batch();
                batch.insert(&journal.meta, CHAIN_KEY, chain.to_bytes());
                batch.commit().map_err(fjall("open"))?;
            }
        }
        Ok(journal)
    }

    /// Empties the journal and records this layout version.
    fn clear(&self) -> Result<(), StorageError> {
        let mut batch = self.db.batch();
        for guard in self.db.snapshot().iter(&self.blocks) {
            let key = guard.key().map_err(fjall("clear"))?;
            batch.remove(&self.blocks, key);
        }
        for key in [CHAIN_KEY, SAFE_HEAD_KEY, FINALIZED_HEAD_KEY] {
            batch.remove(&self.meta, key);
        }
        batch.insert(&self.meta, SCHEMA_VERSION_KEY, SCHEMA_VERSION.to_be_bytes());
        batch.commit().map_err(fjall("clear"))
    }

    /// The L1 heads and every block, in insertion order. Blocking.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the journal cannot be read or holds a record that does not
    /// decode.
    pub(super) fn replay(&self) -> Result<(L1Heads, Vec<Stored>), StorageError> {
        let snapshot = self.db.snapshot();
        let head = |key: &str| -> Result<Option<BlockRef>, StorageError> {
            let value = snapshot.get(&self.meta, key).map_err(fjall("replay"))?;
            value
                .map(|value| BlockRef::from_bytes(&value).ok_or_else(|| invalid("head", None)))
                .transpose()
        };
        let heads = L1Heads {
            safe: head(SAFE_HEAD_KEY)?,
            finalized: head(FINALIZED_HEAD_KEY)?,
        };
        let mut blocks = Vec::new();
        for guard in snapshot.iter(&self.blocks) {
            let (key, value) = guard.into_inner().map_err(fjall("replay"))?;
            let seq = <[u8; 8]>::try_from(&*key)
                .map(u64::from_be_bytes)
                .map_err(|_length| invalid("block key", None))?;
            blocks.push(decode(seq, &value)?);
        }
        Ok((heads, blocks))
    }

    /// Applies `changes` in one batch, not synced to disk. Blocking.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Fjall`] if the batch cannot be written.
    pub(super) fn write(&self, changes: &Changes<'_>) -> Result<(), StorageError> {
        let mut batch = self.db.batch().durability(Some(PersistMode::Buffer));
        if let Some(block) = changes.put {
            batch.insert(&self.blocks, block.seq.to_be_bytes(), encode(block));
        }
        for seq in &changes.delete {
            batch.remove(&self.blocks, seq.to_be_bytes());
        }
        if let Some(heads) = changes.heads {
            for (key, head) in [
                (SAFE_HEAD_KEY, heads.safe),
                (FINALIZED_HEAD_KEY, heads.finalized),
            ] {
                if let Some(head) = head {
                    batch.insert(&self.meta, key, head.to_bytes());
                }
            }
        }
        batch.commit().map_err(fjall("write"))
    }
}

/// A block's record: hash, source, the senders, then the header, the body and the receipts
/// (if any), each with its length.
fn encode(block: &Stored) -> Vec<u8> {
    let encoded = &block.encoded;
    let mut out = Vec::with_capacity(block.bytes());
    out.extend_from_slice(encoded.hash.as_slice());
    out.push(match block.source {
        BlockSource::Gossip => 0,
        BlockSource::Sync => 1,
    });
    push_len(&mut out, block.senders.len());
    for sender in &block.senders {
        out.extend_from_slice(sender.as_slice());
    }
    for value in [&encoded.header, &encoded.body] {
        push_len(&mut out, value.len());
        out.extend_from_slice(value);
    }
    if let Some(receipts) = &encoded.receipts {
        push_len(&mut out, receipts.len());
        out.extend_from_slice(receipts);
    }
    out
}

fn push_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&u32::try_from(len).unwrap_or(u32::MAX).to_be_bytes());
}

/// The inverse of [`encode`], for the block inserted `seq`-th.
fn decode(seq: u64, record: &[u8]) -> Result<Stored, StorageError> {
    let mut rest = record;
    let hash = B256::from_slice(take(&mut rest, 32).ok_or_else(|| invalid("hash", None))?);
    let damaged = || invalid("block record", Some(hash));
    let source = match take(&mut rest, 1).ok_or_else(damaged)? {
        [0] => BlockSource::Gossip,
        [1] => BlockSource::Sync,
        _ => return Err(damaged()),
    };
    let count = take_len(&mut rest).ok_or_else(damaged)?;
    let senders = take(&mut rest, count.saturating_mul(Address::len_bytes()))
        .ok_or_else(damaged)?
        .as_chunks::<20>()
        .0
        .iter()
        .map(Address::from)
        .collect();
    let mut value = || -> Option<Bytes> {
        let len = take_len(&mut rest)?;
        take(&mut rest, len).map(Bytes::copy_from_slice)
    };
    let (header, body) = (value().ok_or_else(damaged)?, value().ok_or_else(damaged)?);
    let receipts = value();
    let parsed: Header = alloy_rlp::decode_exact(&header).map_err(|_err| damaged())?;
    Ok(Stored {
        number: parsed.number,
        parent_hash: parsed.parent_hash,
        timestamp: parsed.timestamp,
        encoded: EncodedBlock {
            hash,
            header,
            body,
            receipts,
        },
        senders,
        source,
        seq,
    })
}

fn take<'a>(rest: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    let (taken, left) = rest.split_at_checked(len)?;
    *rest = left;
    Some(taken)
}

fn take_len(rest: &mut &[u8]) -> Option<usize> {
    let bytes: [u8; 4] = take(rest, 4)?.try_into().ok()?;
    usize::try_from(u32::from_be_bytes(bytes)).ok()
}

fn fjall(operation: &'static str) -> impl Fn(fjall::Error) -> StorageError {
    move |source| StorageError::Fjall { operation, source }
}

const fn invalid(what: &'static str, block: Option<B256>) -> StorageError {
    StorageError::InvalidData {
        store: Store::Unsafe,
        what,
        block,
        source: None,
    }
}
