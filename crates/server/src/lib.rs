//! The server's committed store and its exporter (`docs/serving.md` sections 4 and 5).
//!
//! A server keeps no history (D13). Its committed store, [`R2Archive`], implements
//! [`ArchiveStore`](op_indexer_storage::ArchiveStore), so the node's promotion, `el` serving,
//! the stream and Flight run on it unchanged:
//!
//! ```text
//! reads ≤ the last sealed block ─▶ R2 (sealed chunks, through a ChunkSource)
//! reads above it               ─▶ the tail (a FjallArchive of committed, unsealed blocks)
//! writes                       ─▶ the tail only; it drops what a sealed chunk covers
//! ```
//!
//! - [`ChunkSource`] is what the server needs of the sealed chunks; [`R2Chunks`] implements it
//!   over the chunk store in R2.
//! - [`R2Archive`] routes reads, follows the manifest ([`R2Archive::follow`]) and prunes the
//!   tail; long reads stream chunk after chunk with bounded read-ahead (`feed`), never cached
//!   on disk.
//! - Peers' history reads from R2 take a place in a global budget (`budget`, D14); past it a
//!   peer gets the empty answer eth/69 allows.
//! - [`Exporter`] (`server --export`, D11/D12) seals each full chunk whose blocks are all
//!   finalized and have their receipts, uploads it, appends the manifest and rolls the
//!   hash-index generation; it resumes from the manifest and is idempotent.

mod archive;
mod budget;
mod export;
mod feed;
mod r2;
mod source;

pub use archive::R2Archive;
pub use export::Exporter;
pub use r2::R2Chunks;
pub use source::{ChunkRange, ChunkSource};

/// What stops the server's store or its exporter.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// The chunk source (R2, the manifest, the hash index) failed.
    #[error("chunk source {operation} failed")]
    Source {
        /// What was being done.
        operation: &'static str,
        /// The source's error; its kind says whether trying again can help.
        #[source]
        source: std::io::Error,
    },
    /// A chunk could not be written.
    #[error("{operation} failed")]
    Chunk {
        /// What was being done.
        operation: &'static str,
        /// The chunk writer's error.
        #[source]
        source: op_indexer_chunks::ChunksError,
    },
    /// The local tail failed.
    #[error("the local tail failed")]
    Tail(#[from] op_indexer_storage::StorageError),
    /// A listed chunk does not continue the chunk before it.
    #[error("sealed chunk {first}..={last} does not follow the chunk before it")]
    Unlinked {
        /// The chunk's first block.
        first: u64,
        /// Its last block.
        last: u64,
    },
}

impl ServerError {
    const fn source(operation: &'static str) -> impl FnOnce(std::io::Error) -> Self {
        move |source| Self::Source { operation, source }
    }

    const fn chunk(operation: &'static str) -> impl FnOnce(op_indexer_chunks::ChunksError) -> Self {
        move |source| Self::Chunk { operation, source }
    }
}
