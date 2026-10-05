//! Sealed block chunks in object storage (`docs/serving.md`): the chunk format, the manifest,
//! the global hash index and the client that reads and writes them (Cloudflare R2 through its
//! S3 API, or any `object_store` backend with the same layout).
//!
//! ```text
//! <bucket>/<prefix>/                       e.g. op-snapshot/archive/
//!     chunks/<first>-<last>-<root>.opxc    sealed chunks (format), immutable
//!     manifest/<sequence>.json             the chunks, in hash-chained segments (manifest)
//!     index/<generation>/<shard>.idx       hash → number shards (hash_index)
//! ```
//!
//! Writing ([`ChunkRecord`], [`ChunkWriter`], [`ChunkStore::put_chunk`], [`Manifest::append`],
//! [`IndexBuilder`]) is shared by the importer's conversion and the server's exporter. Reading
//! ([`ChunkStore::stream`], [`ChunkStore::block`], [`ChunkStore::lookup_hash`]) checks what it
//! reads (D5: the chunk's root and each segment's sha256, header hashes and parent links; no
//! root is recomputed) and keeps no block data: only the manifest and indexes the caller holds.
//!
//! This is the one crate that talks to an external service besides the importer (`CLAUDE.md`).

mod config;
mod format;
mod hash_index;
mod manifest;
mod store;

pub use config::R2ConfigError;
pub use format::{ChunkIndex, ChunkRecord, ChunkWriter, SealedChunk};
pub use hash_index::{IndexBuilder, IndexGeneration};
pub use manifest::{ChunkEntry, Manifest};
pub use store::{BlockStream, ChunkStore, R2Config, ReadOptions};

use alloy_primitives::B256;

/// The manifest's directory under a chain's prefix.
pub(crate) const MANIFEST_DIR: &str = "manifest";
/// The chunks' directory under a chain's prefix.
pub(crate) const CHUNKS_DIR: &str = "chunks";

/// Why a chunk operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChunksError {
    /// The object store failed.
    #[error("object store request failed")]
    Store(#[from] object_store::Error),
    /// An object read back does not check (D5).
    #[error("{key} fails its integrity check: {check}")]
    Integrity {
        /// The object.
        key: String,
        /// What did not hold.
        check: &'static str,
    },
    /// An object is not of this format.
    #[error("{key} is malformed: {reason}")]
    Malformed {
        /// The object.
        key: String,
        /// What is wrong.
        reason: &'static str,
    },
    /// The manifest does not parse or breaks its chain.
    #[error("the manifest is invalid: {0}")]
    Manifest(String),
    /// Another writer appended this manifest segment first.
    #[error("manifest segment {sequence} was appended by another writer")]
    Conflict {
        /// The segment's sequence number.
        sequence: u64,
    },
    /// A block cannot go into a chunk.
    #[error("block {hash} cannot be sealed: {reason}")]
    NotSealable {
        /// The block's hash.
        hash: B256,
        /// Why.
        reason: &'static str,
    },
    /// A local file (a hash index spill) failed, or compression failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A blocking task (decompression, a shard build) panicked.
    #[error("a blocking chunk task failed")]
    Task(#[from] tokio::task::JoinError),
}

impl ChunksError {
    /// Whether retrying may succeed: a network error, a timeout or a server error the client
    /// gave up on (`object_store` already retries those a few times). Not authentication, a
    /// missing object, a failed check or a malformed object.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Store(object_store::Error::Generic { .. }))
    }
}
