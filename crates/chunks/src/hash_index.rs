//! The global hash → block number index (`docs/serving.md` D9): immutable shards in
//! generations, read with two ranged GETs.
//!
//! ```text
//! index/<generation:06>/<shard:03x>.idx, shard = the hash's first 12 bits, 4,096 per generation
//! shard  = fan-out: 257 × u32, the entry where each value of the hash's third byte starts
//!          (the last is the entry count) | entries, sorted: hash bytes 2..10 (8) | number u64
//! ```
//!
//! Integers are little-endian. A lookup reads the fan-out (1 KB), then its bucket (about 150
//! entries on OP Mainnet). Eight bytes after a 12-bit prefix leave a false match below one in
//! 10^9 over a whole chain; a caller confirms a match with the block's own hash anyway.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use alloy_primitives::B256;
use bytes::Bytes;
use futures_util::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};

use crate::ChunksError;
use crate::manifest::Manifest;
use crate::store::ChunkStore;

/// Shards per generation: the hash's first 12 bits.
const SHARDS: usize = 4096;
/// Buckets of a shard's fan-out: the hash's third byte.
const FANOUT: usize = 256;
/// Bytes of a shard's fan-out table.
const FANOUT_LEN: usize = (FANOUT + 1) * 4;
const FANOUT_BYTES: u64 = FANOUT_LEN as u64;
/// Bytes of one entry.
const ENTRY: usize = 16;
/// Entries buffered in memory before they are spilled to the builder's directory.
const SPILL_BYTES: usize = 64 << 20;
/// Shards written (or read) at once.
const SHARDS_IN_FLIGHT: usize = 16;

/// One generation of the index, as the manifest records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexGeneration {
    /// Its number; its shards are `index/<generation>/`.
    pub generation: u64,
    /// The last block it covers; the chunks sealed after it are found by their own indexes.
    pub through_block: u64,
    /// Its entries.
    pub entries: u64,
}

/// The shard a hash is in.
fn shard_of(hash: &B256) -> usize {
    let bytes = hash.0;
    (usize::from(bytes[0]) << 4) | usize::from(bytes[1] >> 4)
}

/// The part of a hash an entry keeps: bytes 2 to 10.
fn key_of(hash: &B256) -> [u8; 8] {
    let b = hash.0;
    [b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9]]
}

fn shard_key(generation: u64, shard: usize) -> String {
    format!("index/{generation:06}/{shard:03x}.idx")
}

/// Builds a generation of the index from `(hash, number)` pairs, spilling them to a directory
/// by shard, so a whole chain (about 157 M blocks on OP Mainnet, 2.5 GB) never sits in memory.
#[derive(Debug)]
pub struct IndexBuilder {
    dir: PathBuf,
    buffers: Vec<Vec<u8>>,
    buffered: usize,
}

impl IndexBuilder {
    /// A builder spilling to `dir`, which is emptied of earlier spills.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Io`] if the directory cannot be made.
    pub fn new(dir: &Path) -> Result<Self, ChunksError> {
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_owned(),
            buffers: vec![Vec::new(); SHARDS],
            buffered: 0,
        })
    }

    /// Adds the block `number` with `hash`.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Io`] if spilling fails.
    pub fn push(&mut self, hash: B256, number: u64) -> Result<(), ChunksError> {
        if let Some(buffer) = self.buffers.get_mut(shard_of(&hash)) {
            buffer.extend_from_slice(&key_of(&hash));
            buffer.extend_from_slice(&number.to_le_bytes());
            self.buffered = self.buffered.saturating_add(ENTRY);
        }
        if self.buffered >= SPILL_BYTES {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<(), ChunksError> {
        for (shard, buffer) in self.buffers.iter_mut().enumerate() {
            if buffer.is_empty() {
                continue;
            }
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(format!("{shard:03x}.spill")))?
                .write_all(buffer)?;
            buffer.clear();
        }
        self.buffered = 0;
        Ok(())
    }

    /// Writes generation `generation`, through block `through_block`: every entry pushed, and
    /// every entry of `base` if given (the previous generation). Each shard is sorted and
    /// stored; a shard already stored (a run repeated after a crash) is kept.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Io`] if a spill cannot be read, and [`ChunksError::Store`] if the
    /// store fails.
    pub async fn finish(
        mut self,
        store: &ChunkStore,
        base: Option<&IndexGeneration>,
        generation: u64,
        through_block: u64,
    ) -> Result<IndexGeneration, ChunksError> {
        self.spill()?;
        let dir = self.dir.clone();
        let counts: Vec<u64> = futures_util::stream::iter(0..SHARDS)
            .map(|shard| {
                let dir = dir.clone();
                async move {
                    let mut entries = match base {
                        Some(base) => store
                            .get(&shard_key(base.generation, shard))
                            .await?
                            .to_vec(),
                        None => Vec::new(),
                    };
                    let base_len = entries.len();
                    let spill = dir.join(format!("{shard:03x}.spill"));
                    let shard_bytes = tokio::task::spawn_blocking(move || {
                        if spill.exists() {
                            entries.extend_from_slice(&fs::read(&spill)?);
                        }
                        let skip = FANOUT_LEN.min(base_len);
                        Ok::<_, ChunksError>(build_shard(entries.get(skip..).unwrap_or_default()))
                    })
                    .await??;
                    let count = shard_bytes.len().saturating_sub(FANOUT_LEN) / ENTRY;
                    store
                        .put_once(&shard_key(generation, shard), Bytes::from(shard_bytes))
                        .await?;
                    Ok::<_, ChunksError>(count as u64)
                }
            })
            .buffer_unordered(SHARDS_IN_FLIGHT)
            .try_collect()
            .await?;
        fs::remove_dir_all(&self.dir)?;
        Ok(IndexGeneration {
            generation,
            through_block,
            entries: counts.iter().sum(),
        })
    }
}

/// A shard from unsorted entries: sorted, duplicates dropped, its fan-out in front.
fn build_shard(entries: &[u8]) -> Vec<u8> {
    let (entries, _rest) = entries.as_chunks::<ENTRY>();
    let mut entries = entries.to_vec();
    entries.sort_unstable();
    entries.dedup();
    let mut counts = [0_u32; FANOUT];
    for entry in &entries {
        if let Some(count) = counts.get_mut(usize::from(entry[0])) {
            *count = count.saturating_add(1);
        }
    }
    // Where each bucket starts, then the count.
    let mut fanout = [0_u32; FANOUT + 1];
    let mut start = 0_u32;
    for (slot, count) in fanout.iter_mut().zip(counts.iter().chain([&0])) {
        *slot = start;
        start = start.saturating_add(*count);
    }
    let mut shard = Vec::with_capacity(FANOUT_LEN + entries.len() * ENTRY);
    for start in fanout {
        shard.extend_from_slice(&start.to_le_bytes());
    }
    for entry in &entries {
        shard.extend_from_slice(entry);
    }
    shard
}

impl ChunkStore {
    /// The number of the block with `hash` among the sealed chunks: the index's newest
    /// generation (two ranged GETs), then the chunks sealed since it, newest first (one GET
    /// each). Without a generation every chunk's index is read, so lookups need one.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the store fails, and [`ChunksError::Malformed`] if a
    /// shard is not of this format.
    pub async fn lookup_hash(
        &self,
        manifest: &Manifest,
        hash: B256,
    ) -> Result<Option<u64>, ChunksError> {
        let generation = manifest.index_generation();
        if let Some(generation) = generation
            && let Some(number) = self.lookup_in(generation, &hash).await?
        {
            return Ok(Some(number));
        }
        let after = generation.map(|generation| generation.through_block);
        for entry in manifest
            .entries()
            .iter()
            .rev()
            .take_while(|entry| after.is_none_or(|after| entry.last > after))
        {
            if let Some(number) = self.index(entry).await?.number_of(hash) {
                return Ok(Some(number));
            }
        }
        Ok(None)
    }

    async fn lookup_in(
        &self,
        generation: &IndexGeneration,
        hash: &B256,
    ) -> Result<Option<u64>, ChunksError> {
        let key = shard_key(generation.generation, shard_of(hash));
        let malformed = || ChunksError::Malformed {
            key: key.clone(),
            reason: "a hash index shard is cut short",
        };
        let fanout = self.get_range(&key, 0..FANOUT_BYTES).await?;
        let bucket = usize::from(hash.0[2]);
        let at = |position: usize| {
            fanout
                .get(position * 4..position * 4 + 4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(|bytes| u64::from(u32::from_le_bytes(bytes)))
        };
        let (start, end) = at(bucket).zip(at(bucket + 1)).ok_or_else(malformed)?;
        if end <= start {
            return Ok(None);
        }
        let entries = self
            .get_range(
                &key,
                FANOUT_BYTES + start * ENTRY as u64..FANOUT_BYTES + end * ENTRY as u64,
            )
            .await?;
        let (entries, _rest) = entries.as_chunks::<ENTRY>();
        let wanted = key_of(hash);
        Ok(entries
            .binary_search_by(|entry| entry[..8].cmp(&wanted[..]))
            .ok()
            .and_then(|found| entries.get(found))
            .and_then(|entry| entry[8..].try_into().ok())
            .map(u64::from_le_bytes))
    }
}
