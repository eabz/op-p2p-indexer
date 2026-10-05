//! [`ChunkSource`]: what the server needs of the sealed chunks in R2.
//!
//! [`R2Archive`](crate::R2Archive) and the [`Exporter`](crate::Exporter) are written against
//! this trait, not against the chunk store directly: the chunk store's reader
//! (`op-indexer-chunks`) implements it, and the server's logic does not change with the
//! store's internals (index caching, hedged segment reads, the hash-index shards).
//!
//! Errors are [`io::Error`]s whose kind says whether trying again can help: `TimedOut`,
//! `ConnectionReset` and the like for network failures, `Other` or `NotFound` for the rest
//! (`op_indexer_storage::StorageError::Remote` classifies them the same way).

use std::future::Future;
use std::io;
use std::sync::Arc;

use alloy_primitives::{B256, BlockHash, BlockNumber};
use futures_util::stream::BoxStream;
use op_indexer_chunks::{SealedChunk, StreamReads};
use op_indexer_primitives::ArchivedBlock;

/// A sealed chunk as the manifest lists it: a contiguous range of blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRange {
    /// The first block.
    pub first: BlockNumber,
    /// The last block.
    pub last: BlockNumber,
    /// The first block's parent hash.
    pub first_parent: B256,
    /// The last block's hash.
    pub last_hash: B256,
}

/// The sealed chunks of one chain: a manifest that only grows, the blocks in them, and the
/// one writer's way to add a chunk.
pub trait ChunkSource: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The chunks listed when the manifest was last read, oldest first.
    fn chunks(&self) -> Arc<[ChunkRange]>;

    /// Reads the manifest again, after checking its segment chain, and returns the chunks it
    /// added.
    fn refresh(&self) -> impl Future<Output = io::Result<Vec<ChunkRange>>> + Send;

    /// Streams the blocks of `chunk` from `from` to its end, each verified before it is
    /// yielded, reading as `reads` says. Dropping the stream stops its reads.
    fn stream(
        &self,
        chunk: &ChunkRange,
        from: BlockNumber,
        reads: StreamReads,
    ) -> BoxStream<'static, io::Result<ArchivedBlock>>;

    /// The number of the sealed block with `hash`, if one is sealed.
    fn number_of(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = io::Result<Option<BlockNumber>>> + Send;

    /// Uploads a chunk and lists it in the manifest (the exporter's write, D8), writing a new
    /// hash-index generation when one is due (D9); returns the chunk as now listed. Only the
    /// one exporter of a deployment calls it. Repeating it after a crash is harmless.
    fn publish(
        &self,
        chunk: SealedChunk,
        exporter_id: &str,
    ) -> impl Future<Output = io::Result<ChunkRange>> + Send;
}
