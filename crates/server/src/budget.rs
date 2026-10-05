//! The global budget of R2 reads made for p2p peers (D14).
//!
//! Peers' history requests are read from R2 on demand. Next to `el`'s per-peer serving
//! limits, every peer read takes a place among [`MAX_PEER_READS`] and counts its bytes against
//! [`PEER_BYTES_PER_MINUTE`]; a read that finds no place or no bytes left is not made, and the
//! peer gets the empty answer eth/69 allows. Streams and Flight (the node's own consumers) do
//! not count here: they have their own limits in the stream server.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// R2 reads for peers in progress at once.
const MAX_PEER_READS: usize = 4;
/// Bytes read from R2 for peers per minute: about 8 MB/s, a fraction of a droplet's link.
const PEER_BYTES_PER_MINUTE: u64 = 512 * 1024 * 1024;
const WINDOW: Duration = Duration::from_mins(1);

/// The budget, shared by every clone.
#[derive(Debug, Clone)]
pub(crate) struct PeerBudget {
    reads: Arc<Semaphore>,
    window: Arc<Mutex<(Instant, u64)>>,
}

impl PeerBudget {
    pub(crate) fn new() -> Self {
        Self {
            reads: Arc::new(Semaphore::new(MAX_PEER_READS)),
            window: Arc::new(Mutex::new((Instant::now(), 0))),
        }
    }

    /// A place for one peer read, if one is free and the minute's bytes are not spent.
    pub(crate) fn try_read(&self) -> Option<OwnedSemaphorePermit> {
        {
            let mut window = self.window.lock().unwrap_or_else(PoisonError::into_inner);
            if window.0.elapsed() >= WINDOW {
                *window = (Instant::now(), 0);
            }
            if window.1 >= PEER_BYTES_PER_MINUTE {
                return None;
            }
        }
        Arc::clone(&self.reads).try_acquire_owned().ok()
    }

    /// Counts `bytes` read from R2 for a peer.
    pub(crate) fn spent(&self, bytes: u64) {
        let mut window = self.window.lock().unwrap_or_else(PoisonError::into_inner);
        window.1 = window.1.saturating_add(bytes);
    }
}
