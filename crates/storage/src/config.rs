//! Storage configuration, as plain data.
//!
//! Does not read the environment; the binary fills these in.

use std::path::PathBuf;

use op_indexer_primitives::ChainIdentity;

/// Configuration of the two stores.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// The unsafe chain, in memory with its journal.
    pub unsafe_chain: UnsafeConfig,
    /// Local block archive: the committed store.
    pub archive: ArchiveConfig,
    /// Chain whose blocks are stored: the archive and the journal record it and refuse
    /// another chain's directory.
    pub chain: ChainIdentity,
}

/// Where the local block archive is. It keeps every block it is given.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// The archive's directory.
    pub path: PathBuf,
}

/// The unsafe chain: its journal and its limits.
#[derive(Debug, Clone)]
pub struct UnsafeConfig {
    /// The journal's directory.
    pub path: PathBuf,
    /// The chain's Canyon time, in Unix seconds: from it a deposit receipt's nonce and version
    /// are part of the receipts root, which is checked when receipts are stored.
    pub canyon_time: u64,
    /// The memory the stored blocks may take, in bytes: past it the lowest heights leave.
    pub max_bytes: u64,
}
