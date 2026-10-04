//! The recent speed of a step, for its progress lines.
//!
//! The time left is estimated from the last minute, not from the whole run: blocks get
//! larger as the chain gets busier, so an average over the run promises too much.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// How often a step logs its progress.
pub(crate) const INTERVAL: Duration = Duration::from_secs(10);

/// The time the speed is measured over.
const WINDOW: Duration = Duration::from_secs(60);

/// A count sampled at each progress line, to give its speed over the last [`WINDOW`].
#[derive(Debug)]
pub(crate) struct Rate {
    /// When each recent sample was taken and the count then, oldest first. Never empty.
    samples: VecDeque<(Instant, u64)>,
}

impl Rate {
    /// Starts measuring now, from a count of zero.
    pub(crate) fn new() -> Self {
        Self {
            samples: VecDeque::from([(Instant::now(), 0)]),
        }
    }

    /// Records that the count is `total` now and returns its speed per second over the last
    /// [`WINDOW`], or since the start if that is shorter.
    pub(crate) fn per_sec(&mut self, total: u64) -> u64 {
        let now = Instant::now();
        while self.samples.len() > 1
            && self
                .samples
                .front()
                .is_some_and(|(at, _)| now.duration_since(*at) > WINDOW)
        {
            self.samples.pop_front();
        }
        let (since, then) = self.samples.front().copied().unwrap_or((now, total));
        self.samples.push_back((now, total));
        total.saturating_sub(then) / now.duration_since(since).as_secs().max(1)
    }
}
