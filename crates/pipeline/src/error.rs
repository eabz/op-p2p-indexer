//! The pipeline's error: what stops it.
//!
//! Transient store errors never reach here (they are retried), and neither do blocks that are
//! dropped or ranges that cannot be promoted (they are logged).

use op_indexer_storage::StorageError;
use tokio::task::JoinError;

/// Why the pipeline stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PipelineError {
    /// A store failed in a way retrying cannot fix.
    #[error("{operation} failed")]
    Storage {
        /// The store call that failed, e.g. `unsafe insert`.
        operation: &'static str,
        /// The store's error.
        #[source]
        source: StorageError,
    },
    /// A block of the range sync, verified against the chain, cannot be decoded or has a
    /// transaction without a recoverable sender: this build cannot store the range.
    #[error("the range sync cannot be stored: {0}")]
    RangeBlock(String),
    /// A task whose inputs close only when the node stops ended before that, e.g. because
    /// whatever sends the L1 heads is gone.
    #[error("the {0} task ended before shutdown")]
    Ended(&'static str),
    /// A pipeline task panicked or was aborted.
    #[error("pipeline task failed")]
    Task(#[source] JoinError),
}
