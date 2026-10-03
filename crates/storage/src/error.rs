//! The storage error and its classification by [`Severity`].
//!
//! Busy servers are recognised here, by error code, and nowhere else.

use std::fmt;
use std::num::ParseIntError;

use alloy_primitives::hex::FromHexError;
use alloy_primitives::{BlockHash, BlockNumber};
use op_indexer_primitives::BlockRef;

use crate::Store;

/// Redis error codes of a server that is busy or not ready; retrying can succeed.
const REDIS_BUSY_CODES: [&str; 6] = [
    "BUSY",
    "LOADING",
    "READONLY",
    "TRYAGAIN",
    "CLUSTERDOWN",
    "MASTERDOWN",
];
/// ClickHouse error codes of a server under load, from its `ErrorCodes.cpp`: 159
/// `TIMEOUT_EXCEEDED`, 202 `TOO_MANY_SIMULTANEOUS_QUERIES`, 241 `MEMORY_LIMIT_EXCEEDED`,
/// 252 `TOO_MANY_PARTS`.
const CLICKHOUSE_BUSY_CODES: [u32; 4] = [159, 202, 241, 252];
/// What a ClickHouse error response starts with, before the numeric code.
const CLICKHOUSE_CODE_PREFIX: &str = "Code: ";

/// How a caller should treat a [`StorageError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Severity {
    /// Retrying can succeed: the connection was lost, the call timed out, the server was busy,
    /// or a read or write of the archive's local files failed.
    Transient,
    /// Nothing is wrong with the store; the caller handles the outcome.
    Expected,
    /// Needs an operator: bad credentials, a schema or checksum mismatch, undecodable stored
    /// data, or a block that does not fit the schema.
    Fatal,
}

/// Why a block cannot be stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InvalidBlockReason {
    /// `senders` does not have one entry per transaction.
    SenderCount,
    /// `receipts` does not have one entry per transaction.
    ReceiptCount,
    /// The body has ommers, which are not stored.
    Ommers,
    /// The body has withdrawals, which are not stored.
    Withdrawals,
    /// The timestamp is beyond the range of the timestamp column.
    TimestampRange,
    /// More transactions than a transaction index can count.
    TooManyTransactions,
    /// More logs than a log index can count.
    TooManyLogs,
    /// A log has more than four topics.
    TooManyTopics,
    /// The number is not the parent's plus one.
    ParentNumber,
    /// The number is beyond what the unsafe store can hold.
    NumberRange,
    /// The block is stored under another number than the one given.
    StoredNumber,
    /// The header does not hash to the block's hash.
    HeaderHash,
}

/// Why a stored value could not be parsed, the source of [`StorageError::InvalidData`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseError {
    /// A number is not a decimal integer.
    #[error(transparent)]
    Number(#[from] ParseIntError),
    /// A hash is not hex of the right length.
    #[error(transparent)]
    Hex(#[from] FromHexError),
    /// A value is not valid snappy.
    #[error(transparent)]
    Snappy(#[from] snap::Error),
    /// A value is not the RLP it should be.
    #[error(transparent)]
    Rlp(#[from] alloy_rlp::Error),
}

/// Why a storage operation failed.
///
/// Storage does not retry. [`Self::severity`] tells the caller what to do with the error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// A Redis request failed.
    #[error("redis {operation} failed")]
    Redis {
        /// The operation that was running.
        operation: &'static str,
        /// The client's error.
        #[source]
        source: redis::RedisError,
    },
    /// A ClickHouse request failed.
    #[error("clickhouse {operation} failed")]
    ClickHouse {
        /// The operation that was running.
        operation: &'static str,
        /// The client's error.
        #[source]
        source: clickhouse::error::Error,
    },
    /// A fjall operation on the local block archive failed.
    #[error("fjall {operation} failed")]
    Fjall {
        /// The operation that was running.
        operation: &'static str,
        /// fjall's error.
        #[source]
        source: fjall::Error,
    },
    /// A blocking task did not run to completion (it panicked or the runtime is shutting down).
    #[error("blocking task for {operation} failed")]
    BlockingTask {
        /// The operation that was running.
        operation: &'static str,
        /// The join error.
        #[source]
        source: tokio::task::JoinError,
    },
    /// A call did not finish within its timeout.
    #[error("{store} {operation} timed out")]
    Timeout {
        /// The store that did not answer.
        store: Store,
        /// The operation that was running.
        operation: &'static str,
    },
    /// A value could not be encoded for storage.
    #[error("failed to encode {what}")]
    Encode {
        /// What was being encoded.
        what: &'static str,
        /// The encoder's error.
        #[source]
        source: serde_json::Error,
    },
    /// Stored data could not be decoded.
    #[error("stored {what}{} is not decodable", of_block(*.block))]
    Decode {
        /// What was being decoded.
        what: &'static str,
        /// The block it belongs to, when known.
        block: Option<BlockHash>,
        /// The decoder's error.
        #[source]
        source: serde_json::Error,
    },
    /// A field the layout promises is absent from stored data.
    #[error("stored {field}{} is missing in {store}", of_block(*.block))]
    MissingField {
        /// The store holding the data.
        store: Store,
        /// The absent field.
        field: &'static str,
        /// The block it belongs to, when known.
        block: Option<BlockHash>,
    },
    /// Stored data is present but is not what the layout promises.
    #[error("stored {what}{} is invalid in {store}", of_block(*.block))]
    InvalidData {
        /// The store holding the data.
        store: Store,
        /// What is invalid.
        what: &'static str,
        /// The block it belongs to, when known.
        block: Option<BlockHash>,
        /// The parse failure, when the value could not be parsed at all.
        #[source]
        source: Option<ParseError>,
    },
    /// A block in a requested ancestry is not stored (pruned, expired, or never received).
    #[error("ancestor {hash} at height {number} is not stored")]
    MissingAncestor {
        /// Hash of the missing block.
        hash: BlockHash,
        /// Height it was expected at.
        number: BlockNumber,
    },
    /// A requested ancestry is longer than the unsafe store returns in one call.
    #[error("ancestry of {requested} blocks exceeds the maximum {max}")]
    AncestryTooLong {
        /// Blocks requested.
        requested: u64,
        /// Most blocks one call returns.
        max: u64,
    },
    /// A block appended to the archive does not extend its range.
    #[error(
        "block does not extend the archive: its parent is {} ({}), the tip is {} ({})",
        got.number, got.hash, expected.number, expected.hash
    )]
    NotContiguous {
        /// The tip of the archive, which the block must extend.
        expected: BlockRef,
        /// The parent the block claims: its number minus one and its parent hash.
        got: BlockRef,
    },
    /// A block to store does not fit the schema.
    #[error("block {number} cannot be stored: {reason}")]
    InvalidBlock {
        /// Number of the block.
        number: BlockNumber,
        /// What does not fit.
        reason: InvalidBlockReason,
    },
    /// A block's encoding is larger than the archive's compressor takes.
    #[error("block {number} is too large to compress")]
    Oversized {
        /// Number of the block.
        number: BlockNumber,
        /// The compressor's error, with the sizes.
        #[source]
        source: snap::Error,
    },
    /// A block to store has a transaction type the schema has no columns for.
    #[error("block {number} has unsupported transaction type {tx_type:#04x}")]
    UnsupportedTransaction {
        /// Number of the block.
        number: BlockNumber,
        /// EIP-2718 type of the transaction.
        tx_type: u8,
    },
    /// An applied ClickHouse migration differs from the one embedded in this binary.
    #[error("migration {version} ({name}) was edited after it was applied")]
    MigrationChecksum {
        /// Version of the migration.
        version: u32,
        /// Name of the migration.
        name: String,
    },
    /// ClickHouse has a migration this binary does not know: the binary is older than the schema.
    #[error("applied migration {version} is unknown to this binary")]
    UnknownMigration {
        /// Version of the migration.
        version: u32,
    },
}

impl fmt::Display for InvalidBlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SenderCount => "senders are not one per transaction",
            Self::ReceiptCount => "receipts are not one per transaction",
            Self::Ommers => "it has ommers",
            Self::Withdrawals => "it has withdrawals",
            Self::TimestampRange => "its timestamp is beyond the stored range",
            Self::TooManyTransactions => "it has too many transactions",
            Self::TooManyLogs => "it has too many logs",
            Self::TooManyTopics => "a log has more than four topics",
            Self::ParentNumber => "its number is not its parent's plus one",
            Self::NumberRange => "its number is beyond what the unsafe store can hold",
            Self::StoredNumber => "it is stored with another number",
            Self::HeaderHash => "its header does not hash to its block hash",
        })
    }
}

impl StorageError {
    /// Classifies the error: retry it, handle it, or stop for an operator.
    #[must_use]
    pub fn severity(&self) -> Severity {
        let transient = match self {
            Self::Redis { source, .. } => redis_is_transient(source),
            Self::ClickHouse { source, .. } => clickhouse_is_transient(source),
            Self::Timeout { .. } => true,
            Self::MissingAncestor { .. }
            | Self::AncestryTooLong { .. }
            | Self::NotContiguous { .. } => {
                return Severity::Expected;
            }
            // Only an I/O failure of the local files can pass; the rest is corruption or misuse.
            Self::Fjall { source, .. } => matches!(
                source,
                fjall::Error::Io(_) | fjall::Error::Storage(fjall::LsmError::Io(_))
            ),
            Self::BlockingTask { .. }
            | Self::Encode { .. }
            | Self::Decode { .. }
            | Self::MissingField { .. }
            | Self::InvalidData { .. }
            | Self::InvalidBlock { .. }
            | Self::Oversized { .. }
            | Self::UnsupportedTransaction { .. }
            | Self::MigrationChecksum { .. }
            | Self::UnknownMigration { .. } => false,
        };
        if transient {
            Severity::Transient
        } else {
            Severity::Fatal
        }
    }
}

fn redis_is_transient(err: &redis::RedisError) -> bool {
    err.is_io_error()
        || err.is_timeout()
        || err.is_connection_dropped()
        || err
            .code()
            .is_some_and(|code| REDIS_BUSY_CODES.contains(&code))
}

fn clickhouse_is_transient(err: &clickhouse::error::Error) -> bool {
    use clickhouse::error::Error;
    // The server's own errors arrive as text: "Code: 202. DB::Exception: ...".
    matches!(err, Error::Network(_) | Error::TimedOut)
        || matches!(err, Error::BadResponse(response)
            if clickhouse_code(response).is_some_and(|code| CLICKHOUSE_BUSY_CODES.contains(&code)))
}

/// The numeric code of a ClickHouse error response, if it has one.
fn clickhouse_code(response: &str) -> Option<u32> {
    let (_, after) = response.split_once(CLICKHOUSE_CODE_PREFIX)?;
    let digits = after.split(|c: char| !c.is_ascii_digit()).next()?;
    digits.parse().ok()
}

/// ` of block 0x…` for an error message, or nothing when the block is not known.
fn of_block(block: Option<BlockHash>) -> String {
    block.map_or_else(String::new, |hash| format!(" of block {hash}"))
}
