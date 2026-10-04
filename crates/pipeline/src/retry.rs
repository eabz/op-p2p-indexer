//! How the pipeline retries store calls: storage's helper, without a time limit, and one way
//! to end a task on what it returns.
//!
//! A store that is down is waited for: the pipeline has nothing better to do than store.

use op_indexer_storage::{StorageError, Store};
use tokio_util::sync::CancellationToken;

pub(crate) use op_indexer_storage::RetryError;

use crate::PipelineError;

/// Runs `call`, a request to `store`, and repeats it while it fails with a transient error,
/// for as long as it takes. See [`op_indexer_storage::retry`].
///
/// # Errors
///
/// Returns [`RetryError::Storage`] with the first error that is not transient, and
/// [`RetryError::Cancelled`] if `cancel` fires after the first attempt has failed.
pub(crate) async fn retry<T, F, Fut>(
    cancel: &CancellationToken,
    store: Store,
    operation: &'static str,
    call: F,
) -> Result<T, RetryError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    op_indexer_storage::retry(cancel, store, operation, None, call).await
}

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
