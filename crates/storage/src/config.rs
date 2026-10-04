//! Storage configuration, as plain data.
//!
//! Does not read the environment; the binary fills these in.

use std::fmt;
use std::path::PathBuf;

use op_indexer_primitives::ChainIdentity;

/// Shown by `Debug` in place of credentials.
const REDACTED: &str = "<redacted>";

/// Configuration of the two stores.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Unsafe store.
    pub redis: RedisConfig,
    /// Local block archive: the committed store.
    pub archive: ArchiveConfig,
    /// Chain whose blocks are stored. Its id is part of every Redis key; the archive records
    /// all of it and refuses another chain's directory.
    pub chain: ChainIdentity,
}

/// Where the local block archive is, and how much it keeps.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// The archive's directory.
    pub path: PathBuf,
    /// How much the archive keeps.
    pub retention: ArchiveRetention,
}

/// How much of the chain the local block archive keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveRetention {
    /// The newest blocks, this many; the caller trims the archive to it after appending.
    Blocks(u64),
    /// Every block; the archive is never trimmed.
    All,
}

/// Where the unsafe store is.
#[derive(Clone)]
pub struct RedisConfig {
    /// `redis://[user:password@]host:port[/db]`.
    pub url: String,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &redact_url(&self.url))
            .finish()
    }
}

/// Returns `url` without credentials, so they never reach the logs: the `user:password@` part
/// is replaced, and the query and fragment are dropped (`?user=..&password=..`).
fn redact_url(url: &str) -> String {
    let url = url.split(['?', '#']).next().unwrap_or(url);
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest.split_at(authority_end).0.rsplit_once('@') {
        Some((userinfo, _host)) => {
            let after_userinfo = rest.get(userinfo.len()..).unwrap_or_default();
            format!("{scheme}://{REDACTED}{after_userinfo}")
        }
        None => url.to_owned(),
    }
}
