//! Committed store on ClickHouse: blocks committed to L1 and backfill, and the schema migrations.
//!
//! Does not decide which blocks are committed; the caller inserts and rolls back.
//!
//! - [`ClickHouseStore`]: the connector and the [`crate::CommittedStore`] implementation.
//! - `rows`: table rows and the mapping from an `DecodedBlock`.
//! - `migrations`: the embedded schema and its checksummed, ordered application.
//!
//! Block-data tables are `ReplacingMergeTree(version)` with `version` the insert time in
//! microseconds, so inserting a block again is harmless and the newest row wins; read with
//! `FINAL`.

mod client;
mod migrations;
mod rows;

pub use client::{BulkRows, ClickHouseStore};
