//! Pacing of the requests one requester makes of one peer: a few at a time at most (one for the
//! receipts fetcher), a pause between two, and a count of the answers that did not come.
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
    /// Requests sent and not answered yet.
    in_flight: u32,
    /// Most requests in flight at once.
    max_in_flight: u32,
    /// Not asked before this.
    next_request: Instant,
    /// Requests in a row without a timely answer.
    timeouts: u32,
}

impl Pacing {
    /// A peer that may be asked from `now` on, one request at a time.
    pub(crate) const fn new(now: Instant) -> Self {
        Self::with_limit(now, 1)
    }

    /// A peer that may be asked from `now` on, with up to `max_in_flight` requests at once.
    pub(crate) const fn with_limit(now: Instant, max_in_flight: u32) -> Self {
        Self {
            in_flight: 0,
            max_in_flight,
            next_request: now,
            timeouts: 0,
        }
    }

    /// Whether the peer may be asked at `now`: room for another request and the pause has
    /// passed.
    pub(crate) fn is_ready(&self, now: Instant) -> bool {
        self.in_flight < self.max_in_flight && self.next_request <= now
    }

    /// When the peer may be asked again; `None` while it has as many requests in flight as it
    /// may.
    pub(crate) const fn ready_at(&self) -> Option<Instant> {
        if self.in_flight >= self.max_in_flight {
            None
        } else {
            Some(self.next_request)
        }
    }

    /// A request was sent. The next one waits the pause.
    pub(crate) fn started(&mut self) {
        self.in_flight = self.in_flight.saturating_add(1);
        self.next_request = Instant::now() + REQUEST_SPACING;
    }

    /// The request ended; `timely` unless it timed out (or was answered so late that it counts
    /// as that). Returns whether the peer is now unresponsive: [`MAX_TIMEOUTS`] in a row.
    pub(crate) fn finished(&mut self, timely: bool) -> bool {
        self.in_flight = self.in_flight.saturating_sub(1);
        self.next_request = self.next_request.max(Instant::now() + REQUEST_SPACING);
        self.timeouts = if timely {
            0
        } else {
            self.timeouts.saturating_add(1)
        };
        self.timeouts >= MAX_TIMEOUTS
    }
}
