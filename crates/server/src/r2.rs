//! [`R2Chunks`]: the [`ChunkSource`] over the chunk store in R2 (`op-indexer-chunks`).
//!
//! It holds the manifest: a snapshot the readers use, and the copy the one refresher or
//! writer updates. Publishing a chunk uploads it, appends its manifest segment and, every
//! [`GENERATION_BLOCKS`] blocks sealed since the last hash-index generation, writes a new one
//! from the indices of the chunks sealed since (D9).

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use alloy_primitives::{BlockHash, BlockNumber};
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use op_indexer_chunks::{ChunkEntry, ChunkStore, ChunksError, IndexBuilder, Manifest, SealedChunk};
use op_indexer_primitives::ArchivedBlock;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::source::{ChunkRange, ChunkSource};

/// Blocks sealed since the last hash-index generation after which a new one is written.
const GENERATION_BLOCKS: u64 = 50_000;

/// The sealed chunks in R2.
#[derive(Debug, Clone)]
pub struct R2Chunks {
    store: ChunkStore,
    /// What readers see: the manifest as last read or written.
    snapshot: Arc<RwLock<Snapshot>>,
    /// The copy refreshes and appends work on, one at a time.
    manifest: Arc<Mutex<Manifest>>,
    /// Where the hash-index builder spills its shards (exporter only).
    index_dir: PathBuf,
    /// Whether the missing hash index was warned about.
    unindexed_warned: Arc<AtomicBool>,
}

#[derive(Debug)]
struct Snapshot {
    manifest: Arc<Manifest>,
    chunks: Arc<[ChunkRange]>,
}

impl Snapshot {
    fn of(manifest: &Manifest) -> Self {
        Self {
            chunks: manifest.entries().iter().map(range).collect(),
            manifest: Arc::new(manifest.clone()),
        }
    }
}

impl R2Chunks {
    /// Reads the manifest of `store`. A hash-index generation the exporter writes spills its
    /// shards in `index_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError`] if the manifest cannot be read or breaks its chain.
    pub async fn open(store: ChunkStore, index_dir: PathBuf) -> Result<Self, ChunksError> {
        let manifest = Manifest::load(&store).await?;
        Ok(Self {
            store,
            snapshot: Arc::new(RwLock::new(Snapshot::of(&manifest))),
            manifest: Arc::new(Mutex::new(manifest)),
            index_dir,
            unindexed_warned: Arc::default(),
        })
    }

    fn snapshot(&self) -> (Arc<Manifest>, Arc<[ChunkRange]>) {
        let snapshot = self.snapshot.read().unwrap_or_else(PoisonError::into_inner);
        (Arc::clone(&snapshot.manifest), Arc::clone(&snapshot.chunks))
    }

    fn publish_snapshot(&self, manifest: &Manifest) {
        *self
            .snapshot
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Snapshot::of(manifest);
    }

    /// Writes a new hash-index generation if [`GENERATION_BLOCKS`] blocks were sealed since
    /// the last one.
    async fn roll_generation(
        &self,
        manifest: &mut Manifest,
        exporter_id: &str,
    ) -> Result<(), ChunksError> {
        let Some(last) = manifest.last().map(|entry| entry.last) else {
            return Ok(());
        };
        let base = manifest.index_generation().copied();
        let through = base.as_ref().map(|generation| generation.through_block);
        if through.is_some_and(|through| last.saturating_sub(through) < GENERATION_BLOCKS) {
            return Ok(());
        }
        let mut hashes = Vec::new();
        for entry in manifest
            .entries()
            .iter()
            .filter(|entry| through.is_none_or(|through| entry.first > through))
        {
            hashes.extend(self.store.index(entry).await?.blocks());
        }
        // The builder spills to disk: on a blocking thread.
        let dir = self.index_dir.clone();
        let builder = tokio::task::spawn_blocking(move || {
            let mut builder = IndexBuilder::new(&dir)?;
            for (hash, number) in hashes {
                builder.push(hash, number)?;
            }
            Ok::<_, ChunksError>(builder)
        })
        .await
        .map_err(|err| ChunksError::Io(io::Error::other(err)))??;
        let number = base
            .as_ref()
            .map_or(0, |generation| generation.generation.saturating_add(1));
        let generation = builder
            .finish(&self.store, base.as_ref(), number, last)
            .await?;
        info!(
            generation = generation.generation,
            through = generation.through_block,
            "hash index generation written"
        );
        manifest
            .append_generation(&self.store, generation, exporter_id)
            .await
    }
}

impl ChunkSource for R2Chunks {
    fn chunks(&self) -> Arc<[ChunkRange]> {
        self.snapshot().1
    }

    async fn refresh(&self) -> io::Result<Vec<ChunkRange>> {
        let mut manifest = self.manifest.lock().await;
        let added = manifest.refresh(&self.store).await.map_err(to_io)?;
        if !added.is_empty() {
            self.publish_snapshot(&manifest);
        }
        Ok(added.iter().map(range).collect())
    }

    fn stream(
        &self,
        chunk: &ChunkRange,
        from: BlockNumber,
    ) -> BoxStream<'static, io::Result<ArchivedBlock>> {
        let (manifest, _) = self.snapshot();
        match manifest.find(chunk.first) {
            Some(entry) => self
                .store
                .stream(entry, from)
                .map(|block| block.map_err(to_io))
                .boxed(),
            None => stream::once(async { Err(io::Error::from(io::ErrorKind::NotFound)) }).boxed(),
        }
    }

    /// Without a hash-index generation a lookup would read every chunk's index: then a hash
    /// is not found in R2 (warned about once), so a peer asking for a block we do not hold
    /// costs no R2 read. By number, every sealed block is still served.
    async fn number_of(&self, hash: BlockHash) -> io::Result<Option<BlockNumber>> {
        let (manifest, _) = self.snapshot();
        if manifest.index_generation().is_none() {
            if !self.unindexed_warned.swap(true, Ordering::Relaxed) {
                warn!(
                    "the R2 manifest has no hash-index generation: blocks in R2 are found by \
                     number only until the exporter or the converter writes one"
                );
            }
            return Ok(None);
        }
        self.store.lookup_hash(&manifest, hash).await.map_err(to_io)
    }

    async fn publish(&self, chunk: SealedChunk, exporter_id: &str) -> io::Result<ChunkRange> {
        self.store.put_chunk(&chunk).await.map_err(to_io)?;
        let mut manifest = self.manifest.lock().await;
        let listed = range(&chunk.entry);
        // Already listed: a publish repeated after a crash.
        if manifest
            .last()
            .is_none_or(|last| last.last < chunk.entry.last)
        {
            manifest
                .append(&self.store, vec![chunk.entry], exporter_id)
                .await
                .map_err(to_io)?;
        }
        self.roll_generation(&mut manifest, exporter_id)
            .await
            .map_err(to_io)?;
        self.publish_snapshot(&manifest);
        Ok(listed)
    }
}

/// A manifest entry as the server sees it.
fn range(entry: &ChunkEntry) -> ChunkRange {
    ChunkRange {
        first: entry.first,
        last: entry.last,
        first_parent: entry.first_parent,
        last_hash: entry.last_hash,
    }
}

/// A chunk store error as an [`io::Error`] whose kind says whether trying again can help.
fn to_io(err: ChunksError) -> io::Error {
    let kind = if err.is_transient() {
        io::ErrorKind::TimedOut
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, err)
}
