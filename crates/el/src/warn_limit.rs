//! Keeping a repeated warning to one line per interval.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// When a warning was last logged, to log it at most once per interval. Counts what it held
/// back, so the next line can say how many there were.
#[derive(Debug, Default)]
pub(crate) struct WarnLimit(Mutex<(Option<Instant>, u64)>);

impl WarnLimit {
    /// Whether the warning may be logged now, at least `interval` after the last one; if so,
    /// how many were held back since (0 or more). `None` counts this one as held back.
    pub(crate) fn allow(&self, interval: Duration) -> Option<u64> {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let (last, held) = &mut *state;
        if last.is_some_and(|at| at.elapsed() < interval) {
            *held = held.saturating_add(1);
            return None;
        }
        *last = Some(Instant::now());
        Some(std::mem::take(held))
    }
}
