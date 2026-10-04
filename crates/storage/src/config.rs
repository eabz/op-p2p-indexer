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

/// Where the local block archive is. It keeps every block it is given.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// The archive's directory.
    pub path: PathBuf,
}

/// Where the unsafe store is.
#[derive(Clone)]
pub struct RedisConfig {
    /// `redis://[user:password@]host:port[/db]`.
    pub url: String,
    /// The chain's Canyon time, in Unix seconds: from it a deposit receipt's nonce and version
    /// are part of the receipts root, which reads check before serving receipts.
    pub canyon_time: u64,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &redact_url(&self.url))
            .field("canyon_time", &self.canyon_time)
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
