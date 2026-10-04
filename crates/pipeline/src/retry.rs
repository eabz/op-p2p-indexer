//! How the pipeline retries store calls: storage's helper, and one way to end a task on what
//! it returns.
//!
//! A store that is down is waited for: the pipeline has nothing better to do than store.

pub(crate) use op_indexer_storage::{RetryError, retry};

use crate::PipelineError;

/// Ends a retried call for a task that cannot go on without it: the value, `None` if
/// cancellation ended the retry (the task then stops quietly), or the error that stops the
/// pipeline, with `operation` naming the call.
pub(crate) fn settle<T>(
    result: Result<T, RetryError>,
    operation: &'static str,
) -> Result<Option<T>, PipelineError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(RetryError::Cancelled) => Ok(None),
        Err(RetryError::Storage(source)) => Err(PipelineError::Storage { operation, source }),
    }
}
