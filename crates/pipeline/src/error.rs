//! The pipeline's error: what stops it.
//!
//! Transient store errors never reach here (they are retried), and neither do blocks that are
//! dropped or ranges that cannot be promoted (they are logged and counted).

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
    /// A pipeline task panicked or was aborted.
    #[error("pipeline task failed")]
    Task(#[source] JoinError),
}
