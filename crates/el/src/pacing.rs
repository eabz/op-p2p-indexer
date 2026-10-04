//! Pacing of the requests one requester makes of one peer: one at a time, a pause between
//! two, and a count of the answers that did not come.
//!
//! This node only asks, so it asks slowly. Each requester (the receipts fetcher, range sync)
//! keeps one [`Pacing`] per open session; they are not shared, so a peer can have one request
//! of each in flight.

use std::time::Duration;

use tokio::time::Instant;

/// Pause between two requests to the same peer.
pub(crate) const REQUEST_SPACING: Duration = Duration::from_millis(200);
/// Requests in a row without a timely answer after which a peer is unresponsive.
const MAX_TIMEOUTS: u32 = 3;

/// When one peer may be asked again, and how often in a row it did not answer in time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Pacing {
    busy: bool,
    /// Not asked before this.
    next_request: Instant,
    /// Requests in a row without a timely answer.
    timeouts: u32,
}

impl Pacing {
    /// A peer that may be asked from `now` on.
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            busy: false,
            next_request: now,
            timeouts: 0,
        }
    }

    /// Whether the peer may be asked at `now`: nothing in flight and the pause has passed.
    pub(crate) fn is_ready(&self, now: Instant) -> bool {
        !self.busy && self.next_request <= now
    }

    /// When the peer may be asked again; `None` while a request is in flight.
    pub(crate) const fn ready_at(&self) -> Option<Instant> {
        if self.busy {
            None
        } else {
            Some(self.next_request)
        }
    }

    /// A request was sent.
    pub(crate) const fn started(&mut self) {
        self.busy = true;
    }

    /// The request ended; `timely` unless it timed out (or was answered so late that it counts
    /// as that). Returns whether the peer is now unresponsive: [`MAX_TIMEOUTS`] in a row.
    pub(crate) fn finished(&mut self, timely: bool) -> bool {
        self.busy = false;
        self.next_request = Instant::now() + REQUEST_SPACING;
        self.timeouts = if timely {
            0
        } else {
            self.timeouts.saturating_add(1)
        };
        self.timeouts >= MAX_TIMEOUTS
    }
}
