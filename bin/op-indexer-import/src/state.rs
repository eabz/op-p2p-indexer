//! The state directory: the plan of the import, and where each chunk's files are.
//!
//! ```text
//! <state>/plan.json                     the range, its anchor and the chunk size
//! <state>/raw/<from>-<to>.raw           downloaded chunk: the service's answers as they travelled
//! <state>/raw/<from>-<to>.fill.json     fields the service left out, from the chain's RPC (`fill`)
//! <state>/sealed/<first>-<last>.json    a sealed chunk `verify` uploaded: its manifest entry
//! <state>/index-build/                  the hash index being built (`verify`)
//! <state>/lock                          held by the one process working on the directory
//! ```
//!
//! `verify` deletes a downloaded chunk once sealed chunks it recorded cover it whole, so
//! `download` treats a chunk covered by the records as done. A `verified/` directory left by
//! an earlier build is not read any more and can be deleted, as can an `export-index/`.
//! `download` writes the plan on its first run; every later run of any step reads it, so the
//! range and the chunk size cannot change under files already written. A file that exists is
//! complete: files are written under a temporary name (`*.tmp`) and renamed; temporary files
//! left by a killed run are removed when the directory is opened. Holds no credentials. Does
//! not know what the chunk files contain.

use std::fmt;
use std::fs::{self, File, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy_primitives::B256;
use op_indexer_chainspec::ChainSpec;
use op_indexer_chunks::ChunkEntry;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::game::GameAnchor;

/// Version of the state directory's layout and file formats. A directory written with another
/// version is refused.
const LAYOUT_VERSION: u32 = 1;

/// The block `download`'s rebuild from L1 left off at (`fill`): what the next base fee is
/// computed from.

/// Free space below which a step warns with its progress.
pub(crate) const LOW_SPACE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Free space below which a step starts no new chunk: the chunks in flight still have to fit.
pub(crate) const MIN_SPACE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

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
    /// The chain the blocks belong to.
    pub(crate) chain: &'static ChainSpec,
    /// First block.
    pub(crate) first: u64,
    /// Last block.
    pub(crate) last: u64,
    /// What the last block is checked against.
    pub(crate) anchor: Anchor,
    /// Blocks per chunk before the chain's Bedrock block; never zero. From Bedrock on blocks
    /// hold many transactions and a chunk is a tenth of this: see [`Plan::chunks`].
    pub(crate) chunk_blocks: u64,
}

/// What proves that the last block of the range is the canonical one; every block below is
/// then proven by the chain of parent hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Anchor {
    /// A block hash the operator trusts.
    Hash(B256),
    /// The claim of a dispute game on L1 about the last block.
    Game(GameAnchor),
}

impl fmt::Display for Anchor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hash(hash) => write!(f, "hash {hash}"),
            Self::Game(game) => write!(f, "dispute game {} on L1", game.game),
        }
    }
}

impl Plan {
    /// The chunks of the range, in block order: [`Self::chunk_blocks`] blocks each before the
    /// Bedrock block, a tenth of that from it on, where one block holds as much as many legacy
    /// ones. The chunk before the Bedrock block and the last one may be shorter.
    pub(crate) fn chunks(&self) -> impl Iterator<Item = Chunk> + use<> {
        let (end, bedrock) = (self.last.saturating_add(1), self.chain.bedrock_block);
        let (legacy_step, step) = (self.chunk_blocks, (self.chunk_blocks / 10).max(1));
        let mut from = self.first;
        std::iter::from_fn(move || {
            if from >= end {
                return None;
            }
            let to = if from < bedrock {
                from.saturating_add(legacy_step).min(bedrock)
            } else {
                from.saturating_add(step)
            };
            let chunk = Chunk {
                from,
                to: to.min(end),
            };
            from = chunk.to;
            Some(chunk)
        })
    }
}

/// `plan.json`.
#[derive(Debug, Serialize, Deserialize)]
struct PlanFile {
    version: u32,
    chain_id: u64,
    first: u64,
    last: u64,
    anchor: Anchor,
    chunk_blocks: u64,
}

/// Paths inside the state directory.
#[derive(Debug, Clone)]
pub(crate) struct State {
    root: PathBuf,
    raw: PathBuf,
    sealed: PathBuf,
    /// Held so that only one process works on the directory.
    _lock: Arc<File>,
}

impl State {
    /// Creates the state directory and its subdirectories if they are missing, takes the
    /// directory's lock for the life of the process, and removes the temporary files an
    /// interrupted run left behind.
    ///
    /// # Errors
    ///
    /// Returns `ResourceBusy` if another process holds the lock, and the I/O error if a
    /// directory cannot be created or read.
    pub(crate) fn open(root: &Path) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        // An advisory lock on an open file: the system drops it when the process ends, however
        // it ends, so a killed run never leaves the directory locked.
        let lock = File::create(root.join("lock"))?;
        lock.try_lock().map_err(|err| match err {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::ResourceBusy,
                "another `import` process is using this state directory",
            ),
            TryLockError::Error(err) => err,
        })?;
        let state = Self {
            root: root.to_owned(),
            raw: root.join("raw"),
            sealed: root.join("sealed"),
            _lock: Arc::new(lock),
        };
        fs::create_dir_all(&state.raw)?;
        fs::create_dir_all(&state.sealed)?;
        // Temporary files of a run that was killed mid-write; nobody else writes here now.
        for directory in [root, &state.raw, &state.sealed] {
            for entry in fs::read_dir(directory)? {
                let path = entry?.path();
                if path.extension().is_some_and(|extension| extension == "tmp") {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(state)
    }

    /// The directory, for messages.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Reads the recorded plan; `None` if `download` has not written one yet. Blocking.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the file is damaged, was written by another version of this
    /// tool, or names a chain this build does not know; and the I/O error of reading it.
    pub(crate) fn read_plan(&self) -> io::Result<Option<Plan>> {
        let Some(file) = read_json::<PlanFile>(&self.root.join("plan.json"))? else {
            return Ok(None);
        };
        let invalid = |reason: String| io::Error::new(io::ErrorKind::InvalidData, reason);
        if file.version != LAYOUT_VERSION {
            return Err(invalid(format!(
                "the state directory has layout version {}, this build writes {LAYOUT_VERSION}: \
                 delete the directory and download again",
                file.version
            )));
        }
        let chain = ChainSpec::by_chain_id(file.chain_id)
            .ok_or_else(|| invalid(format!("plan.json names unknown chain {}", file.chain_id)))?;
        if file.chunk_blocks == 0 {
            return Err(invalid("plan.json has a chunk size of zero".to_owned()));
        }
        Ok(Some(Plan {
            chain,
            first: file.first,
            last: file.last,
            anchor: file.anchor,
            chunk_blocks: file.chunk_blocks,
        }))
    }

    /// Records `plan`. Blocking.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of writing the file.
    pub(crate) fn write_plan(&self, plan: &Plan) -> io::Result<()> {
        let file = PlanFile {
            version: LAYOUT_VERSION,
            chain_id: plan.chain.chain_id,
            first: plan.first,
            last: plan.last,
            anchor: plan.anchor,
            chunk_blocks: plan.chunk_blocks,
        };
        write_json(&self.root.join("plan.json"), &file)
    }

    /// Whether the directory holds any downloaded chunk. Blocking.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of listing the directory.
    pub(crate) fn has_chunks(&self) -> io::Result<bool> {
        Ok(fs::read_dir(&self.raw)?.next().is_some())
    }

    /// The sealed chunks `verify` recorded as uploaded, in block order. Blocking.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if a record is damaged, and the I/O error of reading them.
    pub(crate) fn read_sealed(&self) -> io::Result<Vec<ChunkEntry>> {
        let mut entries = Vec::new();
        for file in fs::read_dir(&self.sealed)? {
            let path = file?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
                && let Some(entry) = read_json::<ChunkEntry>(&path)?
            {
                entries.push(entry);
            }
        }
        entries.sort_unstable_by_key(|entry| entry.first);
        Ok(entries)
    }

    /// The last block the recorded sealed chunks cover; `None` if there is none. Blocking.
    ///
    /// # Errors
    ///
    /// As [`Self::read_sealed`].
    pub(crate) fn sealed_through(&self) -> io::Result<Option<u64>> {
        Ok(self.read_sealed()?.last().map(|entry| entry.last))
    }

    /// Records that the sealed chunk `entry` is uploaded, durably. Blocking.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of writing the record.
    pub(crate) fn write_sealed(&self, entry: &ChunkEntry) -> io::Result<()> {
        let name = format!("{:012}-{:012}.json", entry.first, entry.last);
        write_json(&self.sealed.join(name), entry)
    }

    /// Removes a downloaded chunk and its fill, which a recorded sealed chunk covers. Blocking.
    ///
    /// # Errors
    ///
    /// Returns the I/O error, unless a file was not there.
    pub(crate) fn remove_raw(&self, chunk: Chunk) -> io::Result<()> {
        remove_if_exists(&self.raw_path(chunk))?;
        remove_if_exists(&self.fill_path(chunk))
    }

    /// Free space on the directory's filesystem, in bytes; `None` where the system has no call
    /// for it. Blocking.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of the system call.
    pub(crate) fn free_bytes(&self) -> io::Result<Option<u64>> {
        free_bytes(&self.root)
    }

    /// File of the downloaded chunk.
    pub(crate) fn raw_path(&self, chunk: Chunk) -> PathBuf {
        self.raw
            .join(format!("{:012}-{:012}.raw", chunk.from, chunk.to))
    }

    /// File of what `download` fetched from the chain's RPC for a downloaded chunk: the
    /// fields the service left out (see `fill`).
    pub(crate) fn fill_path(&self, chunk: Chunk) -> PathBuf {
        self.raw
            .join(format!("{:012}-{:012}.fill.json", chunk.from, chunk.to))
    }
}

/// Whether sealed chunks recorded through block `sealed` cover `chunk` whole.
pub(crate) fn covered(sealed: Option<u64>, chunk: Chunk) -> bool {
    sealed.is_some_and(|sealed| chunk.to <= sealed.saturating_add(1))
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
    let written = File::create(&temporary).and_then(|mut file| {
        write(&mut file)?;
        file.flush()?;
        file.sync_all()
    });
    if let Err(err) = written {
        // Best effort: a leftover is removed when the directory is next opened.
        let _removed = fs::remove_file(&temporary);
        return Err(err);
    }
    // The contents are synced first, so a crash never leaves a short file under the final
    // name; the rename itself becomes durable when the directory is synced ([`sync_dir`]).
    fs::rename(&temporary, path)
}

/// Free space on the disk of `path`, where the system tells. Blocking.
///
/// # Errors
///
/// Returns the I/O error of asking.
pub(crate) fn free_bytes(path: &Path) -> io::Result<Option<u64>> {
    #[cfg(unix)]
    {
        let stat = rustix::fs::statvfs(path)?;
        Ok(Some(stat.f_bavail.saturating_mul(stat.f_frsize)))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

/// Removes the file at `path`, if there is one. Blocking.
///
/// # Errors
///
/// Returns the I/O error, unless the file was not there.
pub(crate) fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

/// Syncs the directory `path`, so the files renamed into it so far survive a power loss.
/// Blocking.
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Reads the JSON file at `path`, if there is one. Blocking.
///
/// # Errors
///
/// Returns the I/O error, or `InvalidData` if the file does not parse.
pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let content = match fs::read(path) {
        Ok(content) => content,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    serde_json::from_slice(&content).map(Some).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is damaged or from another version: {err}",
                path.display()
            ),
        )
    })
}

/// Writes `value` as JSON to `path`, atomically and durably. Blocking.
///
/// # Errors
///
/// Returns the I/O error.
pub(crate) fn write_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let content = serde_json::to_vec_pretty(value)?;
    write_atomic(path, |file| file.write_all(&content))?;
    path.parent().map_or(Ok(()), sync_dir)
}
