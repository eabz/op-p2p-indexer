//! The retry policy storage deliberately does not have.
//!
//! One helper for both tasks: it repeats a store call while its error is transient and hands
//! every other outcome back. It does not decide what an expected or fatal error means; the
//! caller does.

use std::time::Duration;

use op_indexer_storage::{Severity, StorageError, Store};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::metrics;

/// Wait before the first retry.
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Longest wait between retries; reached after eight failures in a row.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Why [`retry`] gave up.
#[derive(Debug)]
pub(crate) enum RetryError {
    /// Cancelled while waiting to retry.
    Cancelled,
    /// Not transient: expected or fatal, for the caller to decide.
    Storage(StorageError),
}

/// Runs `call`, a request to `store`, and repeats it while it fails with a transient error.
///
/// Waits between attempts with exponential backoff from [`INITIAL_BACKOFF`] to [`MAX_BACKOFF`],
/// each wait shortened at random by up to half so that tasks do not retry in step. There is no
/// attempt limit: a store that is down is waited for.
///
/// The first attempt always runs to its end, even if `cancel` has already fired, so a write
/// that is due is neither skipped nor cut short. After a failure, cancellation ends the retry
/// at once, whether it is waiting or in the middle of another attempt against a store that is
/// down.
///
/// # Errors
///
/// Returns [`RetryError::Storage`] with the first error that is not transient, and
/// [`RetryError::Cancelled`] if `cancel` fires after the first attempt has failed.
pub(crate) async fn retry<T, F, Fut>(
    cancel: &CancellationToken,
    store: Store,
    operation: &'static str,
    mut call: F,
) -> Result<T, RetryError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let mut backoff = INITIAL_BACKOFF;
    let mut attempt = 0_u32;
    loop {
        let result = if attempt == 0 {
            call().await
        } else {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(RetryError::Cancelled),
                result = call() => result,
            }
        };
        let err = match result {
            Ok(value) => return Ok(value),
            Err(err) if err.severity() == Severity::Transient => err,
            Err(err) => return Err(RetryError::Storage(err)),
        };
        attempt = attempt.saturating_add(1);
        let delay = jittered(backoff);
        metrics::retry(store);
        warn!(%store, operation, attempt, ?delay, ?err, "store call failed, retrying");
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(RetryError::Cancelled),
            () = sleep(delay) => {}
        }
        backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
    }
}

/// Returns a random duration between half of `backoff` and all of it.
fn jittered(backoff: Duration) -> Duration {
    let half = backoff / 2;
    half + half.mul_f64(fastrand::f64())
}
