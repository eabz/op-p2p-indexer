//! The retry policy for requests to the external services (the archive service and the
//! chain's RPC): a few attempts, with capped exponential backoff and jitter.

use std::time::Duration;

/// Attempts per request before the step stops.
const MAX_ATTEMPTS: u32 = 6;
/// Wait before the second attempt; doubled for each further one.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
/// Longest wait between two attempts.
const BACKOFF_CAP: Duration = Duration::from_secs(20);

/// Where a request is in its attempts.
#[derive(Debug)]
pub(crate) struct Backoff {
    attempt: u32,
    wait: Duration,
}

impl Backoff {
    pub(crate) const fn new() -> Self {
        Self {
            attempt: 1,
            wait: BACKOFF_BASE,
        }
    }

    /// The attempt running, the first being 1.
    pub(crate) const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The wait before the next attempt, or `None` when the attempts are spent. Up to half of
    /// it is random, so parallel requests do not retry together.
    pub(crate) fn next(&mut self) -> Option<Duration> {
        if self.attempt >= MAX_ATTEMPTS {
            return None;
        }
        let wait = self.wait.mul_f64(1.0 - fastrand::f64() / 2.0);
        self.wait = self.wait.saturating_mul(2).min(BACKOFF_CAP);
        self.attempt += 1;
        Some(wait)
    }
}
