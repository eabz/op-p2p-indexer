//! The manifest: the chain's sealed chunks, as an append-only sequence of immutable segments
//! (`docs/serving.md` §2.2).
//!
//! Segment `n` is `manifest/<n:010>.json`. It lists the chunks it adds, in order, and may
//! record a new generation of the hash index; it names the sha256 of segment `n - 1`, so the
//! segments form a chain. One writer per chain appends (D8): a chunk's object first, the
//! segment that lists it second, the segment with a create-only PUT as a guard.

use alloy_primitives::B256;
use futures_util::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};

use crate::format::sha256;
use crate::hash_index::IndexGeneration;
use crate::store::ChunkStore;
use crate::{ChunksError, MANIFEST_DIR};

/// Segments fetched at once when the manifest is read.
const SEGMENT_READS: usize = 16;

/// What the manifest records about one sealed chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkEntry {
    /// Its first block.
    pub first: u64,
    /// Its last block.
    pub last: u64,
    /// The parent hash in its first block's header.
    pub first_parent: B256,
    /// Its last block's hash.
    pub last_hash: B256,
    /// Its root: the sha256 of its index frame, which holds every segment's sha256.
    pub sha256: B256,
    /// Its object's size in bytes.
    pub size: u64,
    /// Where its index frame starts; the index and the 16-byte footer run to the end.
    pub footer_offset: u64,
    /// Its zstd level.
    pub level: i32,
}

impl ChunkEntry {
    /// The object's key under the chain's prefix: `chunks/<first>-<last>-<root, 16 hex>.opxc`.
    /// The root in the name means an object is never overwritten with other bytes.
    #[must_use]
    pub fn key(&self) -> String {
        let root = self.sha256;
        format!(
            "chunks/{:012}-{:012}-{}.opxc",
            self.first,
            self.last,
            alloy_primitives::hex::encode(root.get(..8).unwrap_or_default())
        )
    }

    /// Whether the chunk holds block `number`.
    #[must_use]
    pub const fn holds(&self, number: u64) -> bool {
        number >= self.first && number <= self.last
    }
}

/// One manifest segment, as stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Segment {
    version: u32,
    /// The chain the chunks are of: a reader of another chain refuses the manifest.
    chain_id: u64,
    genesis_hash: B256,
    sequence: u64,
    /// The sha256 of the previous segment's bytes; `None` for segment 0.
    previous: Option<B256>,
    exporter: String,
    written_at: u64,
    chunks: Vec<ChunkEntry>,
    index_generation: Option<IndexGeneration>,
}

/// The manifest as read so far, checked: every segment's chain, and every chunk's first block
/// right after the previous chunk's last, naming its last hash as the parent.
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    entries: Vec<ChunkEntry>,
    generation: Option<IndexGeneration>,
    /// The next segment's sequence number and the sha256 of the last one read.
    next: u64,
    previous: Option<B256>,
}

impl Manifest {
    /// Reads every segment of the chain's manifest.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the store fails, and [`ChunksError::Manifest`] if a
    /// segment does not parse or breaks the chain.
    pub async fn load(store: &ChunkStore) -> Result<Self, ChunksError> {
        let mut manifest = Self::default();
        manifest.refresh(store).await?;
        Ok(manifest)
    }

    /// Reads the segments appended since the last read; returns the chunks they add.
    ///
    /// # Errors
    ///
    /// As [`Self::load`].
    pub async fn refresh(&mut self, store: &ChunkStore) -> Result<Vec<ChunkEntry>, ChunksError> {
        let after = (self.next > 0).then(|| segment_key(self.next - 1));
        let mut keys = store.list(MANIFEST_DIR, after.as_deref()).await?;
        keys.sort_unstable();
        let segments: Vec<(bytes::Bytes, String)> = futures_util::stream::iter(keys)
            .map(|key| async move { store.get(&key).await.map(|bytes| (bytes, key)) })
            .buffered(SEGMENT_READS)
            .try_collect()
            .await?;
        let mut added = Vec::new();
        for (bytes, key) in segments {
            let segment: Segment = serde_json::from_slice(&bytes)
                .map_err(|err| ChunksError::Manifest(format!("{key} does not parse: {err}")))?;
            if (segment.chain_id, segment.genesis_hash) != store.chain() {
                return Err(ChunksError::Manifest(format!(
                    "{key} is of chain {} (genesis {}), not this one",
                    segment.chain_id, segment.genesis_hash
                )));
            }
            if key != segment_key(segment.sequence) || segment.sequence != self.next {
                return Err(ChunksError::Manifest(format!(
                    "{key} is not segment {} of the chain",
                    self.next
                )));
            }
            if segment.previous != self.previous {
                return Err(ChunksError::Manifest(format!(
                    "{key} does not name the previous segment's hash"
                )));
            }
            self.check_continues(&segment.chunks)?;
            added.extend_from_slice(&segment.chunks);
            self.apply(segment, &bytes);
        }
        Ok(added)
    }

    /// Every sealed chunk, in block order.
    #[must_use]
    pub fn entries(&self) -> &[ChunkEntry] {
        &self.entries
    }

    /// The last sealed chunk.
    #[must_use]
    pub fn last(&self) -> Option<&ChunkEntry> {
        self.entries.last()
    }

    /// The chunk holding block `number`.
    #[must_use]
    pub fn find(&self, number: u64) -> Option<&ChunkEntry> {
        let at = self.entries.partition_point(|entry| entry.last < number);
        self.entries.get(at).filter(|entry| entry.holds(number))
    }

    /// The newest generation of the hash index.
    #[must_use]
    pub const fn index_generation(&self) -> Option<&IndexGeneration> {
        self.generation.as_ref()
    }

    /// Appends a segment listing `entries`, whose objects must already be stored.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Manifest`] if `entries` do not continue the chain, and
    /// [`ChunksError::Conflict`] if another writer appended that segment first.
    pub async fn append(
        &mut self,
        store: &ChunkStore,
        entries: Vec<ChunkEntry>,
        exporter: &str,
    ) -> Result<(), ChunksError> {
        self.write(store, entries, None, exporter).await
    }

    /// Appends a segment recording `generation` of the hash index, whose shards must already be
    /// stored.
    ///
    /// # Errors
    ///
    /// As [`Self::append`].
    pub async fn append_generation(
        &mut self,
        store: &ChunkStore,
        generation: IndexGeneration,
        exporter: &str,
    ) -> Result<(), ChunksError> {
        self.write(store, Vec::new(), Some(generation), exporter)
            .await
    }

    async fn write(
        &mut self,
        store: &ChunkStore,
        chunks: Vec<ChunkEntry>,
        index_generation: Option<IndexGeneration>,
        exporter: &str,
    ) -> Result<(), ChunksError> {
        self.check_continues(&chunks)?;
        let (chain_id, genesis_hash) = store.chain();
        let segment = Segment {
            version: 1,
            chain_id,
            genesis_hash,
            sequence: self.next,
            previous: self.previous,
            exporter: exporter.to_owned(),
            written_at: std::time::UNIX_EPOCH
                .elapsed()
                .map_or(0, |elapsed| elapsed.as_secs()),
            chunks,
            index_generation,
        };
        let bytes = bytes::Bytes::from(
            serde_json::to_vec_pretty(&segment)
                .map_err(|err| ChunksError::Manifest(format!("segment does not encode: {err}")))?,
        );
        if !store
            .put_once(&segment_key(segment.sequence), bytes.clone())
            .await?
        {
            return Err(ChunksError::Conflict {
                sequence: segment.sequence,
            });
        }
        self.apply(segment, &bytes);
        Ok(())
    }

    /// Takes in a checked segment, `bytes` as stored.
    fn apply(&mut self, segment: Segment, bytes: &[u8]) {
        self.entries.extend(segment.chunks);
        if let Some(generation) = segment.index_generation {
            self.generation = Some(generation);
        }
        self.next = segment.sequence.saturating_add(1);
        self.previous = Some(sha256(bytes));
    }

    /// Checks that `chunks` continue the chain: each right after the one before, naming its
    /// last hash as the parent.
    fn check_continues(&self, chunks: &[ChunkEntry]) -> Result<(), ChunksError> {
        let mut last = self.entries.last();
        for chunk in chunks {
            if chunk.last < chunk.first
                || last.is_some_and(|last| {
                    chunk.first != last.last.saturating_add(1)
                        || chunk.first_parent != last.last_hash
                })
            {
                return Err(ChunksError::Manifest(format!(
                    "chunk {}-{} does not continue the chain",
                    chunk.first, chunk.last
                )));
            }
            last = Some(chunk);
        }
        Ok(())
    }
}

/// The key of manifest segment `sequence`.
fn segment_key(sequence: u64) -> String {
    format!("{MANIFEST_DIR}/{sequence:010}.json")
}
