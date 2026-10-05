//! The retry policy for store calls, for the callers of this crate.
//!
//! The stores themselves never retry: each call is one attempt with a timeout. This helper
//! repeats a call while its error is [`Severity::Transient`] and hands every other outcome
//! back. It does not decide what an expected or fatal error means; the caller does.

use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{Severity, StorageError, Store};

/// Wait before the first retry.
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Longest wait between retries; reached after eight failures in a row.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Why [`retry`] gave up.
#[derive(Debug)]
pub enum RetryError {
    /// Cancelled while waiting to retry.
    Cancelled,
    /// Not transient: expected or fatal, for the caller to decide.
    Storage(StorageError),
}

/// Runs `call`, a request to `store`, and repeats it while it fails with a transient error.
///
/// Waits between attempts with exponential backoff from 200 ms to 30 s, each wait shortened
/// at random by up to half so that tasks do not retry in step. A store that is down is waited
/// for, without a time limit.
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
pub async fn retry<T, F, Fut>(
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
        let half = backoff / 2;
        // A random duration between half of the backoff and all of it.
        let delay = half + half.mul_f64(fastrand::f64());
        warn!(%store, operation, attempt, ?delay, ?err, "store call failed, retrying");
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(RetryError::Cancelled),
            () = sleep(delay) => {}
        }
        backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
    }
}
