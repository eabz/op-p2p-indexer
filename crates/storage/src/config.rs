//! Storage configuration, as plain data.
//!
//! Does not read the environment; the binary fills these in.

use std::fmt;
use std::path::PathBuf;

use op_indexer_primitives::ChainIdentity;

/// Shown by `Debug` in place of credentials.
const REDACTED: &str = "<redacted>";

/// Configuration of the three stores.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Unsafe store.
    pub redis: RedisConfig,
    /// Committed store.
    pub clickhouse: ClickHouseConfig,
    /// Local block archive; `None` disables it, so nothing is opened or written.
    pub archive: Option<ArchiveConfig>,
    /// Chain whose blocks are stored. Its id is part of every Redis key and ClickHouse row; the
    /// archive records all of it and refuses another chain's directory.
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

/// Where the committed store is, and how to sign in.
#[derive(Clone)]
pub struct ClickHouseConfig {
    /// URL of the HTTP interface, e.g. `http://127.0.0.1:8123`.
    pub url: String,
    /// Database holding the tables.
    pub database: String,
    /// User to sign in as.
    pub user: String,
    /// Password of `user`, if it has one.
    pub password: Option<String>,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &redact_url(&self.url))
            .finish()
    }
}

impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("url", &redact_url(&self.url))
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| REDACTED))
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
