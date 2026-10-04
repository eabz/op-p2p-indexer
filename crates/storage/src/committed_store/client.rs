//! [`ClickHouseStore`]: the ClickHouse connector and its [`CommittedStore`] implementation.
//!
//! Does not map blocks to rows (`rows`), hold the schema (`migrations`), classify errors or
//! retry: every call is attempted once, within its timeout.

use std::time::{Duration, UNIX_EPOCH};

use alloy_primitives::ChainId;
use clickhouse::sql::Identifier;
use clickhouse::{Client, RowOwned, RowWrite};
use op_indexer_primitives::{BlockRef, DecodedBlock, L1Heads};
use tokio::time::timeout;
use tracing::info;

use super::migrations::{self, MigrationRow, SCHEMA_MIGRATIONS};
use super::rows::{ChainStateRow, HeadKey, Rows, StoredHead};
use crate::metrics::{self, Operation, Table};
use crate::validate::validate_block;
use crate::{ClickHouseConfig, CommittedStore, StorageError, Store};

/// Limit for a query or DDL statement.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
/// Limit for writing one table's rows of an insert batch, which may be large.
const INSERT_TIMEOUT: Duration = Duration::from_secs(120);
/// Server-side limit, equal to the client timeout of the request, so work the client gave up on
/// also stops on the server. Queries and inserts get their own value, because whether the server
/// applies it to a streamed `INSERT` is not clearly documented: one value for both would either
/// cut inserts at the query limit or leave queries at the insert limit.
const MAX_EXECUTION_TIME: &str = "max_execution_time";
/// Most blocks written by one round of table inserts. A larger input is written in chunks, so
/// one call holds at most this many blocks' rows besides its input. Rows per insert stay in the
/// thousands to hundreds of thousands, the size ClickHouse handles best.
const MAX_INSERT_BLOCKS: usize = 256;
/// Settings of every insert: the server groups small inserts that arrive close together into
/// one part, and still acknowledges only after the data is written.
const ASYNC_INSERT: [(&str, &str); 2] = [("async_insert", "1"), ("wait_for_async_insert", "1")];

/// Limit for writing one table's rows of a bulk insert, which can be hundreds of thousands.
const BULK_INSERT_TIMEOUT: Duration = Duration::from_secs(300);
/// Settings of a bulk insert: synchronous, so the statement ends when the data is written and
/// one large insert makes one part per partition, not one per small insert.
const BULK_INSERT: [(&str, &str); 1] = [("async_insert", "0")];

/// Reads the applied migrations, oldest first, in the column order of `MigrationRow`.
const APPLIED_MIGRATIONS: &str =
    "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version";

/// The four block-data tables and the column holding each one's block number, in delete order.
/// `blocks` goes first: inserts write it last, so in both directions a `blocks` row is only
/// present while its transactions, receipts and logs are.
const BLOCK_TABLES: [(&str, &str); 4] = [
    ("blocks", "number"),
    ("transactions", "block_number"),
    ("receipts", "block_number"),
    ("logs", "block_number"),
];

/// The rows of a set of blocks, built by [`ClickHouseStore::bulk_rows`] and written by
/// [`ClickHouseStore::bulk_insert`]. Several sets can be joined into one insert.
#[derive(Debug, Default)]
pub struct BulkRows {
    rows: Rows,
    blocks: usize,
}

impl BulkRows {
    /// Adds the rows of `other`.
    pub fn append(&mut self, mut other: Self) {
        self.rows.blocks.append(&mut other.rows.blocks);
        self.rows.transactions.append(&mut other.rows.transactions);
        self.rows.receipts.append(&mut other.rows.receipts);
        self.rows.logs.append(&mut other.rows.logs);
        self.blocks = self.blocks.saturating_add(other.blocks);
    }

    /// Rows in every table together.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
            .blocks
            .len()
            .saturating_add(self.rows.transactions.len())
            .saturating_add(self.rows.receipts.len())
            .saturating_add(self.rows.logs.len())
    }

    /// Blocks the rows are of.
    #[must_use]
    pub const fn blocks(&self) -> usize {
        self.blocks
    }

    /// Transactions the rows hold.
    #[must_use]
    pub const fn transactions(&self) -> usize {
        self.rows.transactions.len()
    }
}

/// The committed store on ClickHouse. Cheap to clone: clones share the HTTP connection pool.
///
/// `Client`'s `Debug` hides the credentials, so deriving it here is safe.
#[derive(Debug, Clone)]
pub struct ClickHouseStore {
    /// The configured database's name, which [`Self::migrate`] creates if it is missing.
    database: String,
    /// Queries without a database, for creating it: a request naming a database that does
    /// not exist is refused before it runs.
    server: Client,
    /// Queries, DDL and deletes, limited to [`QUERY_TIMEOUT`] on the server.
    queries: Client,
    /// Inserts: async, acknowledged once written, limited to [`INSERT_TIMEOUT`] on the server.
    inserts: Client,
    /// Bulk inserts: synchronous, limited to [`BULK_INSERT_TIMEOUT`] on the server.
    bulk: Client,
    chain_id: ChainId,
}

impl ClickHouseStore {
    /// Creates a store for `chain_id` from `config`, with LZ4 compression. Makes no request: use
    /// [`Self::ping`] to check the server is reachable, then [`Self::migrate`].
    pub fn new(config: &ClickHouseConfig, chain_id: ChainId) -> Self {
        let mut server = Client::default()
            .with_url(&config.url)
            .with_user(&config.user)
            .with_compression(clickhouse::Compression::Lz4);
        if let Some(password) = &config.password {
            server = server.with_password(password);
        }
        let client = server.clone().with_database(&config.database);
        let server = server.with_setting(MAX_EXECUTION_TIME, QUERY_TIMEOUT.as_secs().to_string());
        let queries = client
            .clone()
            .with_setting(MAX_EXECUTION_TIME, QUERY_TIMEOUT.as_secs().to_string());
        let mut bulk = client.clone().with_setting(
            MAX_EXECUTION_TIME,
            BULK_INSERT_TIMEOUT.as_secs().to_string(),
        );
        for (name, value) in BULK_INSERT {
            bulk = bulk.with_setting(name, value);
        }
        let mut inserts =
            client.with_setting(MAX_EXECUTION_TIME, INSERT_TIMEOUT.as_secs().to_string());
        for (name, value) in ASYNC_INSERT {
            inserts = inserts.with_setting(name, value);
        }
        Self {
            database: config.database.clone(),
            server,
            queries,
            inserts,
            bulk,
            chain_id,
        }
    }

    /// Checks that the server answers and accepts the credentials.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the server cannot be reached, rejects the credentials, or does
    /// not answer within the timeout.
    pub async fn ping(&self) -> Result<(), StorageError> {
        metrics::timed(Store::Committed, Operation::Connect, async {
            // Without the database, which may not exist before the first `migrate`.
            within(
                QUERY_TIMEOUT,
                "ping",
                self.server.query("SELECT 1").execute(),
            )
            .await
        })
        .await
    }

    /// Brings the schema up to date: creates the configured database if missing (a new server,
    /// ClickHouse Cloud's among them, has only `default`), creates `schema_migrations` if
    /// missing, checks the applied migrations against the embedded ones, then applies and
    /// records the pending ones in order.
    ///
    /// Creating the database is not a migration: the database is configuration, the
    /// migrations are what goes inside it, and only they are recorded and checksummed.
    ///
    /// Run it before anything else uses the store, from one indexer instance at a time.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::MigrationChecksum`] if an applied migration was edited,
    /// [`StorageError::UnknownMigration`] if the schema is newer than this binary, and
    /// [`StorageError`] if a statement fails or times out.
    ///
    /// # Cancel safety
    ///
    /// Dropping it part-way is safe: every statement is `IF NOT EXISTS`, and a version is
    /// recorded only after it is applied. Calling it again finishes.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        metrics::timed(Store::Committed, Operation::Migrate, async {
            within(
                QUERY_TIMEOUT,
                "create database",
                self.server
                    .query("CREATE DATABASE IF NOT EXISTS ?")
                    .bind(Identifier(&self.database))
                    .execute(),
            )
            .await?;
            within(
                QUERY_TIMEOUT,
                "migrate",
                self.queries.query(SCHEMA_MIGRATIONS).execute(),
            )
            .await?;
            let applied: Vec<MigrationRow> = within(
                QUERY_TIMEOUT,
                "migrate",
                self.queries.query(APPLIED_MIGRATIONS).fetch_all(),
            )
            .await?;
            for migration in migrations::pending(&applied, &self.database)? {
                within(
                    QUERY_TIMEOUT,
                    "migrate",
                    self.queries.query(migration.sql).execute(),
                )
                .await?;
                // `DateTime` seconds run out in 2106; saturate rather than fail a migration.
                let applied_at_secs = u32::try_from(now_micros() / 1_000_000).unwrap_or(u32::MAX);
                self.insert_rows(
                    "migrate",
                    "schema_migrations",
                    &[migration.applied(applied_at_secs)],
                )
                .await?;
                info!(
                    version = migration.version,
                    name = migration.name,
                    "applied clickhouse migration"
                );
            }
            Ok(())
        })
        .await
    }

    /// Writes `rows` to `table` in one insert, as an async insert that is acknowledged only once
    /// written. Does nothing for no rows.
    async fn insert_rows<T>(
        &self,
        operation: &'static str,
        table: &str,
        rows: &[T],
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite,
    {
        if rows.is_empty() {
            return Ok(());
        }
        within(INSERT_TIMEOUT, operation, async {
            let mut insert = self.inserts.insert::<T>(table).await?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        })
        .await
    }

    /// Builds the rows of `blocks` for [`Self::bulk_insert`], stamped with the current time as
    /// their version. CPU work proportional to the blocks: call it from a blocking thread.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBlock`] or [`StorageError::UnsupportedTransaction`] for
    /// a block that does not fit the schema.
    pub fn bulk_rows(&self, blocks: &[DecodedBlock]) -> Result<BulkRows, StorageError> {
        let version_micros = now_micros();
        let mut rows = Rows::default();
        for block in blocks {
            validate_block(block)?;
            rows.push(self.chain_id, block, version_micros)?;
        }
        Ok(BulkRows {
            rows,
            blocks: blocks.len(),
        })
    }

    /// Writes `rows` in one synchronous insert per table, the three child tables at once and
    /// `blocks` after them, so a `blocks` row means its transactions, receipts and logs are stored, as
    /// [`CommittedStore::insert`] guarantees. For loading history in large batches, several at
    /// once on separate connections; the live path stays [`CommittedStore::insert`].
    ///
    /// Writing the same rows again is harmless: the tables keep one row per position. A retry
    /// sends identical rows (same version), which a replicated server also recognises as a
    /// repeated insert.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the server cannot be reached, refuses the rows or does not
    /// answer in time; [`StorageError::severity`] says whether to retry.
    ///
    /// # Cancel safety
    ///
    /// Dropping it part-way may leave some tables with the rows and `blocks` without them;
    /// writing the same rows again completes it.
    pub async fn bulk_insert(&self, rows: &BulkRows) -> Result<(), StorageError> {
        metrics::timed(Store::Committed, Operation::Insert, async {
            let rows_of = &rows.rows;
            // The child tables at once, each on its own connection; `blocks` once they hold
            // their rows.
            tokio::try_join!(
                self.bulk_table(Table::Transactions, "transactions", &rows_of.transactions),
                self.bulk_table(Table::Receipts, "receipts", &rows_of.receipts),
                self.bulk_table(Table::Logs, "logs", &rows_of.logs),
            )?;
            self.bulk_table(Table::Blocks, "blocks", &rows_of.blocks)
                .await?;
            metrics::blocks_inserted(Store::Committed, rows.blocks);
            Ok(())
        })
        .await
    }

    async fn bulk_table<T>(&self, table: Table, name: &str, rows: &[T]) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite,
    {
        if rows.is_empty() {
            return Ok(());
        }
        within(BULK_INSERT_TIMEOUT, "bulk insert", async {
            let mut insert = self.bulk.insert::<T>(name).await?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        })
        .await?;
        metrics::rows_inserted(table, rows.len());
        Ok(())
    }

    /// Writes `rows` to a block-data table and counts them.
    async fn insert_table<T>(
        &self,
        table: Table,
        name: &str,
        rows: &[T],
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite,
    {
        self.insert_rows("insert", name, rows).await?;
        metrics::rows_inserted(table, rows.len());
        Ok(())
    }

    /// Writes the given heads to `chain_state`; `None` leaves the stored head unchanged.
    async fn write_heads(
        &self,
        operation: &'static str,
        heads: L1Heads,
    ) -> Result<(), StorageError> {
        let updated_at_micros = now_micros();
        let rows: Vec<ChainStateRow> = [
            (HeadKey::SafeHead, heads.safe),
            (HeadKey::FinalizedHead, heads.finalized),
        ]
        .into_iter()
        .filter_map(|(key, head)| {
            head.map(|head| ChainStateRow::new(self.chain_id, key, head, updated_at_micros))
        })
        .collect();
        self.insert_rows(operation, "chain_state", &rows).await
    }

    /// Writes one chunk of at most [`MAX_INSERT_BLOCKS`] blocks: child tables first, `blocks`
    /// last.
    async fn insert_chunk(
        &self,
        blocks: &[DecodedBlock],
        version_micros: u64,
    ) -> Result<(), StorageError> {
        let mut rows = Rows::default();
        for block in blocks {
            rows.push(self.chain_id, block, version_micros)?;
        }
        self.insert_table(Table::Transactions, "transactions", &rows.transactions)
            .await?;
        self.insert_table(Table::Receipts, "receipts", &rows.receipts)
            .await?;
        self.insert_table(Table::Logs, "logs", &rows.logs).await?;
        self.insert_table(Table::Blocks, "blocks", &rows.blocks)
            .await?;
        metrics::blocks_inserted(Store::Committed, blocks.len());
        Ok(())
    }
}

impl CommittedStore for ClickHouseStore {
    /// Inserts the batch in chunks of at most 256 blocks (`MAX_INSERT_BLOCKS`), each one table
    /// at a time with `blocks` last: the tables are not atomic, so a `blocks` row means its
    /// transactions, receipts and logs are stored. A failure part-way leaves earlier chunks and
    /// child rows, which a retry overwrites.
    ///
    /// Rows are deduplicated by position (block number, transaction or log index), not by block
    /// hash. To replace the block at a height with a different one, call
    /// [`Self::rollback_to`] below that height first; otherwise rows of the old block at higher
    /// indexes remain.
    async fn insert(&self, blocks: &[DecodedBlock]) -> Result<(), StorageError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let version_micros = now_micros();
        // The mapping runs inside `timed`, so a block that does not fit is counted as a failure.
        metrics::timed(Store::Committed, Operation::Insert, async {
            // The whole batch is checked before anything is written.
            blocks.iter().try_for_each(validate_block)?;
            for chunk in blocks.chunks(MAX_INSERT_BLOCKS) {
                self.insert_chunk(chunk, version_micros).await?;
            }
            Ok(())
        })
        .await
    }

    /// Records `safe` as the safe head, then deletes every row above it: `blocks` first, then
    /// transactions, receipts and logs. Writing the head first means an interrupted rollback
    /// never leaves the recorded safe head on a deleted block; calling it again finishes the
    /// deletes. The finalized head is not touched.
    async fn rollback_to(&self, safe: BlockRef) -> Result<(), StorageError> {
        metrics::timed(Store::Committed, Operation::RollbackTo, async {
            let heads = L1Heads {
                safe: Some(safe),
                finalized: None,
            };
            self.write_heads("rollback_to", heads).await?;
            for (table, column) in BLOCK_TABLES {
                let delete = format!("DELETE FROM {table} WHERE chain_id = ? AND {column} > ?");
                within(
                    QUERY_TIMEOUT,
                    "rollback_to",
                    self.queries
                        .query(&delete)
                        .bind(self.chain_id)
                        .bind(safe.number)
                        .execute(),
                )
                .await?;
            }
            Ok(())
        })
        .await?;
        metrics::rollback();
        Ok(())
    }

    async fn l1_heads(&self) -> Result<L1Heads, StorageError> {
        metrics::timed(Store::Committed, Operation::L1Heads, async {
            // `chain_state` has no partition key, so `FINAL` needs no
            // `do_not_merge_across_partitions_select_final` here.
            let stored: Vec<StoredHead> = within(
                QUERY_TIMEOUT,
                "l1_heads",
                self.queries
                    .query("SELECT key, number, hash FROM chain_state FINAL WHERE chain_id = ?")
                    .bind(self.chain_id)
                    .fetch_all(),
            )
            .await?;
            let mut heads = L1Heads::default();
            for head in stored {
                let (key, head_ref) = head.into_head();
                match key {
                    HeadKey::SafeHead => heads.safe = Some(head_ref),
                    HeadKey::FinalizedHead => heads.finalized = Some(head_ref),
                }
            }
            Ok(heads)
        })
        .await
    }

    async fn set_l1_heads(&self, heads: L1Heads) -> Result<(), StorageError> {
        metrics::timed(
            Store::Committed,
            Operation::SetL1Heads,
            self.write_heads("set_l1_heads", heads),
        )
        .await
    }
}

/// Runs `call` with a time limit. A client error becomes [`StorageError::ClickHouse`] and expiry
/// [`StorageError::Timeout`], both naming `operation`.
async fn within<T>(
    limit: Duration,
    operation: &'static str,
    call: impl Future<Output = Result<T, clickhouse::error::Error>>,
) -> Result<T, StorageError> {
    timeout(limit, call)
        .await
        .map_err(|_elapsed| StorageError::Timeout {
            store: Store::Committed,
            operation,
        })?
        .map_err(|source| StorageError::ClickHouse { operation, source })
}

/// Current time in microseconds since the Unix epoch: the `version` of inserted rows, so the
/// newest insert of a block wins in `ReplacingMergeTree`.
///
/// A clock before 1970 gives 0 and one past the year 586,000 saturates. Neither happens on a
/// working host, and a wrong value only decides which of two copies of a row wins.
fn now_micros() -> u64 {
    UNIX_EPOCH.elapsed().map_or(0, |elapsed| {
        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
    })
}
