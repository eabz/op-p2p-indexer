//! ClickHouse schema migrations: one statement per file under `migrations/clickhouse/`,
//! embedded in the binary and applied in order by [`super::ClickHouseStore::migrate`].
//!
//! ClickHouse DDL is not transactional, so every statement is safe to run twice
//! (`IF NOT EXISTS`); a crash mid-migration is fixed by restarting. Only one indexer instance may
//! migrate at a time: there is no lock.

use alloy_primitives::B256;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::StorageError;

/// Creates the table that records applied migrations. Runs before anything else.
pub(super) const SCHEMA_MIGRATIONS: &str =
    include_str!("../../migrations/clickhouse/schema_migrations.sql");

/// Every migration, in the order it must be applied. Until the first release the initial ones
/// may be edited in place (a database that recorded them is then refused with the checksum
/// error, and must be dropped with `DROP DATABASE`); after it, never: add a new one.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "blocks",
        sql: include_str!("../../migrations/clickhouse/0001_blocks.sql"),
    },
    Migration {
        version: 2,
        name: "transactions",
        sql: include_str!("../../migrations/clickhouse/0002_transactions.sql"),
    },
    Migration {
        version: 3,
        name: "receipts",
        sql: include_str!("../../migrations/clickhouse/0003_receipts.sql"),
    },
    Migration {
        version: 4,
        name: "logs",
        sql: include_str!("../../migrations/clickhouse/0004_logs.sql"),
    },
    Migration {
        version: 5,
        name: "chain_state",
        sql: include_str!("../../migrations/clickhouse/0005_chain_state.sql"),
    },
    Migration {
        version: 6,
        name: "imported_ranges",
        sql: include_str!("../../migrations/clickhouse/0006_imported_ranges.sql"),
    },
];

/// One embedded migration.
#[derive(Debug)]
pub(super) struct Migration {
    pub(super) version: u32,
    pub(super) name: &'static str,
    pub(super) sql: &'static str,
}

/// A row of `schema_migrations`.
#[derive(Debug, Row, Serialize, Deserialize)]
pub(super) struct MigrationRow {
    version: u32,
    name: String,
    checksum: [u8; 32],
    /// Seconds since the Unix epoch.
    #[serde(rename = "applied_at")]
    applied_at_secs: u32,
}

impl Migration {
    /// SHA-256 of the SQL, recorded when the migration is applied.
    fn checksum(&self) -> B256 {
        B256::new(Sha256::digest(self.sql).into())
    }

    /// The row recording this migration as applied at `applied_at_secs` (Unix seconds).
    pub(super) fn applied(&self, applied_at_secs: u32) -> MigrationRow {
        MigrationRow {
            version: self.version,
            name: self.name.to_owned(),
            checksum: self.checksum().0,
            applied_at_secs,
        }
    }
}

/// Checks the applied migrations against the embedded ones and returns those still to apply, in
/// order.
///
/// # Errors
///
/// Returns [`StorageError::MigrationChecksum`] if an applied migration of `database` was
/// edited since, and
/// [`StorageError::UnknownMigration`] if one is not embedded in this binary.
pub(super) fn pending(
    applied: &[MigrationRow],
    database: &str,
) -> Result<Vec<&'static Migration>, StorageError> {
    for row in applied {
        let migration = MIGRATIONS
            .iter()
            .find(|migration| migration.version == row.version)
            .ok_or(StorageError::UnknownMigration {
                version: row.version,
            })?;
        if migration.checksum() != row.checksum {
            return Err(StorageError::MigrationChecksum {
                version: row.version,
                name: row.name.clone(),
                database: database.to_owned(),
            });
        }
    }
    Ok(MIGRATIONS
        .iter()
        .filter(|migration| applied.iter().all(|row| row.version != migration.version))
        .collect())
}
