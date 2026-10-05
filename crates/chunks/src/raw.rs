//! Whole chunks as they lie in the bucket (`docs/serving.md`, raw chunk download): presigned
//! GET URLs, which let a client download a chunk straight from R2 without the bucket's key,
//! and the check and decoding of a chunk so downloaded.
//!
//! It does not download: the client does, over plain HTTPS.

use std::fmt;
use std::time::Duration;

use object_store::aws::AmazonS3;
use object_store::path::Path;
use object_store::signer::{Method, Signer as _, Url};
use op_indexer_primitives::ArchivedBlock;

use crate::format::{ChunkIndex, decode_segment};
use crate::{ChunkEntry, ChunksError, R2Config};

/// Signs GET URLs of a chain's chunks with an R2 key. The signature is computed locally: no
/// request is made, and the URL carries the key's id, never its secret.
pub struct ChunkSigner {
    s3: AmazonS3,
    prefix: String,
}

impl fmt::Debug for ChunkSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkSigner")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl ChunkSigner {
    /// A signer with `config`'s key, for the chunks under its bucket and prefix. Give it a
    /// read-only key: a URL it signs is good for anyone who holds it until it expires.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the client cannot be built from `config`.
    pub fn r2(config: &R2Config) -> Result<Self, ChunksError> {
        Ok(Self {
            s3: config.s3().build()?,
            prefix: config.prefix.trim_matches('/').to_owned(),
        })
    }

    /// A GET URL of `entry`'s object, good for `ttl`.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the URL cannot be signed.
    pub async fn url(&self, entry: &ChunkEntry, ttl: Duration) -> Result<Url, ChunksError> {
        let key = entry.key();
        let path = if self.prefix.is_empty() {
            Path::from(key)
        } else {
            Path::from(format!("{}/{key}", self.prefix))
        };
        Ok(self.s3.signed_url(Method::GET, &path, ttl).await?)
    }
}

/// Checks and decodes a whole chunk, `bytes`, against its manifest entry: its size and footer,
/// its index against the root, every segment against the index, every header against its
/// hash and the parent links from `entry`'s first parent through its last hash. Returns its
/// blocks in order. Blocking, CPU-bound.
///
/// # Errors
///
/// Returns [`ChunksError::Integrity`] or [`ChunksError::Malformed`] if the chunk does not
/// check.
pub fn decode_chunk(entry: &ChunkEntry, bytes: &[u8]) -> Result<Vec<ArchivedBlock>, ChunksError> {
    let malformed = |reason| ChunksError::Malformed {
        key: entry.key(),
        reason,
    };
    if u64::try_from(bytes.len()).ok() != Some(entry.size) {
        return Err(malformed("its size is not the manifest's"));
    }
    let (segments, tail) = usize::try_from(entry.footer_offset)
        .ok()
        .and_then(|at| bytes.split_at_checked(at))
        .ok_or_else(|| malformed("its index starts past its end"))?;
    let index = ChunkIndex::parse(entry, tail)?;
    let mut blocks = Vec::new();
    let (mut parent, mut next) = (entry.first_parent, entry.first);
    for segment in &index.segments {
        if segment.first != next {
            return Err(malformed("its segments are not consecutive"));
        }
        let part = usize::try_from(segment.range().start)
            .ok()
            .zip(usize::try_from(segment.range().end).ok())
            .and_then(|(start, end)| segments.get(start..end))
            .ok_or_else(|| malformed("a segment lies outside the chunk"))?;
        let decoded = decode_segment(entry, segment, part, parent, segment.first..u64::MAX)?;
        parent = decoded.last().map_or(parent, |block| block.encoded.hash);
        next = segment.first.saturating_add(u64::from(segment.blocks));
        blocks.extend(decoded);
    }
    if parent != entry.last_hash {
        return Err(ChunksError::Integrity {
            key: entry.key(),
            check: "its last block is not the manifest's",
        });
    }
    Ok(blocks)
}
