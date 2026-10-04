//! The state directory: which chunks the range is cut into and where each one's files are.
//!
//! ```text
//! <state>/raw/<from>-<to>.json.zst    downloaded chunk: the service's answers, compressed
//! <state>/verified/<from>-<to>.blk    verified chunk: consensus encodings, see `chunk`
//! <state>/loaded/<from>-<to>.archive     empty marker: the block archive holds the chunk
//! <state>/loaded/<from>-<to>.clickhouse  empty marker: ClickHouse holds the chunk
//! ```
//!
//! A file that exists is complete: files are written under a temporary name and renamed.
//! Holds no credentials. Does not know what the files contain.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use alloy_primitives::B256;

/// Blocks `from..to` (`to` excluded): one file per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Chunk {
    pub(crate) from: u64,
    pub(crate) to: u64,
}

impl Chunk {
    /// Number of blocks in the chunk.
    pub(crate) const fn blocks(self) -> u64 {
        self.to.saturating_sub(self.from)
    }
}

/// The range to import and how it is cut into chunks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Plan {
    /// First block.
    pub(crate) first: u64,
    /// Last block.
    pub(crate) last: u64,
    /// Trusted hash of the last block.
    pub(crate) anchor: B256,
    /// Blocks per chunk; never zero.
    pub(crate) chunk_blocks: u64,
    /// Fork activations that change how blocks are encoded.
    pub(crate) forks: Forks,
}

/// The fork activations of an OP Stack chain that change an encoding `verify` rebuilds, in
/// Unix seconds. Unused for blocks without deposits and before the fork times.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Forks {
    /// Regolith: the L1-attributes deposit stops being a system transaction, and deposit
    /// receipts record the sender's nonce.
    pub(crate) regolith_time: u64,
    /// Canyon: the deposit nonce and receipt version become part of the hashed receipt.
    pub(crate) canyon_time: u64,
    /// Isthmus: the header carries the hash of an empty requests list.
    pub(crate) isthmus_time: u64,
}

impl Plan {
    /// The chunks of the range, in block order. The last one may be shorter.
    pub(crate) fn chunks(&self) -> impl Iterator<Item = Chunk> + use<> {
        let (first, end, step) = (self.first, self.last.saturating_add(1), self.chunk_blocks);
        let mut from = first;
        std::iter::from_fn(move || {
            if from >= end {
                return None;
            }
            let chunk = Chunk {
                from,
                to: from.saturating_add(step).min(end),
            };
            from = chunk.to;
            Some(chunk)
        })
    }
}

/// Something `load` writes chunks to. Each keeps its own record of the chunks it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    /// The local block archive the node serves from.
    Archive,
    /// ClickHouse, when asked for.
    ClickHouse,
}

/// Paths inside the state directory.
#[derive(Debug, Clone)]
pub(crate) struct State {
    raw: PathBuf,
    verified: PathBuf,
    loaded: PathBuf,
}

impl State {
    /// Creates the state directory and its subdirectories if they are missing.
    ///
    /// # Errors
    ///
    /// Returns the I/O error if a directory cannot be created.
    pub(crate) fn open(root: &Path) -> io::Result<Self> {
        let state = Self {
            raw: root.join("raw"),
            verified: root.join("verified"),
            loaded: root.join("loaded"),
        };
        fs::create_dir_all(&state.raw)?;
        fs::create_dir_all(&state.verified)?;
        fs::create_dir_all(&state.loaded)?;
        Ok(state)
    }

    /// File of the downloaded chunk.
    pub(crate) fn raw_path(&self, chunk: Chunk) -> PathBuf {
        self.raw
            .join(format!("{:012}-{:012}.json.zst", chunk.from, chunk.to))
    }

    /// File of the verified chunk.
    pub(crate) fn verified_path(&self, chunk: Chunk) -> PathBuf {
        self.verified
            .join(format!("{:012}-{:012}.blk", chunk.from, chunk.to))
    }

    /// Marker of the chunk for one target of `load`: an empty file, written once that target
    /// holds the chunk.
    pub(crate) fn loaded_path(&self, chunk: Chunk, target: Target) -> PathBuf {
        let extension = match target {
            Target::Archive => "archive",
            Target::ClickHouse => "clickhouse",
        };
        self.loaded
            .join(format!("{:012}-{:012}.{extension}", chunk.from, chunk.to))
    }
}

/// Writes a file so that it exists only when complete: `write` fills a temporary file next to
/// `path`, which is synced and renamed over `path`. Blocking.
///
/// # Errors
///
/// Returns the I/O error of creating, writing, syncing or renaming the file.
pub(crate) fn write_atomic(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    write(&mut file)?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(&temporary, path)
}
