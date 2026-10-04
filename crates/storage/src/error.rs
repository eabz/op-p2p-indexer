//! The storage error and its classification by [`Severity`].
//!
//! A busy Redis is recognised here, by error code, and nowhere else.

use std::fmt;
use std::num::ParseIntError;
use std::path::PathBuf;

use alloy_primitives::hex::FromHexError;
use alloy_primitives::{BlockHash, BlockNumber};
use op_indexer_primitives::{BlockRef, ChainIdentity};

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
/// How a caller should treat a [`StorageError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Severity {
    /// Retrying can succeed: the connection was lost, the call timed out, the server was busy,
    /// or a read or write of the archive's local files failed.
    Transient,
    /// Nothing is wrong with the store; the caller handles the outcome.
    Expected,
    /// Needs an operator: bad credentials, a schema mismatch, undecodable stored data, or a
    /// block that does not fit the schema.
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
        /// The parent the block claims (its number minus one and its parent hash), or the
        /// block of the list that the archive holds another block in place of.
        got: BlockRef,
    },
    /// The archive directory was written with another schema version. Nothing is deleted: the
    /// operator loads a new archive from the importer's verified chunks, or runs the build
    /// that wrote it.
    #[error(
        "the block archive in {} has schema version {found}, this build reads version \
         {expected}: the archive's layout changed. Move the directory away and load a new \
         archive with `import load` from the verified chunks (no download needed), \
         or keep running the build that wrote it",
        path.display()
    )]
    ArchiveSchema {
        /// The archive directory.
        path: PathBuf,
        /// The version found: a number, or the bytes stored where it should be.
        found: String,
        /// The version this build writes.
        expected: u64,
    },
    /// The archive directory holds another chain's blocks. Nothing is deleted: the operator
    /// points the node at that chain's data directory, or removes this one.
    #[error(
        "the block archive in {} holds {found}, but this node runs {expected}; use a data \
         directory of chain {}, or delete this one",
        path.display(),
        expected.chain_id
    )]
    ArchiveChain {
        /// The archive directory.
        path: PathBuf,
        /// The chain recorded in the archive. Boxed, as is `expected`, to keep the error small.
        found: Box<ChainIdentity>,
        /// The chain this node runs.
        expected: Box<ChainIdentity>,
    },
    /// The archive's chain record does not decode. Nothing is deleted.
    #[error(
        "the chain record of the block archive in {} is unreadable; the directory is left as \
         it is",
        path.display()
    )]
    ArchiveChainUnreadable {
        /// The archive directory.
        path: PathBuf,
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
}

impl fmt::Display for InvalidBlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SenderCount => "senders are not one per transaction",
            Self::ReceiptCount => "receipts are not one per transaction",
            Self::Ommers => "it has ommers",
            Self::Withdrawals => "it has withdrawals",
            Self::ParentNumber => "its number is not its parent's plus one",
            Self::NumberRange => "its number is beyond what the unsafe store can hold",
            Self::StoredNumber => "it is stored with another number",
            Self::HeaderHash => "its header does not hash to its block hash",
        })
    }
}

impl StorageError {
    /// Whether the error is the block archive's directory being open in another process:
    /// fjall locks it, so one process uses it at a time.
    #[must_use]
    pub const fn is_archive_locked(&self) -> bool {
        matches!(
            self,
            Self::Fjall {
                source: fjall::Error::Locked,
                ..
            }
        )
    }

    /// Classifies the error: retry it, handle it, or stop for an operator.
    #[must_use]
    pub fn severity(&self) -> Severity {
        let transient = match self {
            Self::Redis { source, .. } => redis_is_transient(source),
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
            | Self::ArchiveSchema { .. }
            | Self::ArchiveChain { .. }
            | Self::ArchiveChainUnreadable { .. }
            | Self::InvalidBlock { .. }
            | Self::Oversized { .. }
            | Self::UnsupportedTransaction { .. } => false,
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

/// ` of block 0x…` for an error message, or nothing when the block is not known.
fn of_block(block: Option<BlockHash>) -> String {
    block.map_or_else(String::new, |hash| format!(" of block {hash}"))
}
