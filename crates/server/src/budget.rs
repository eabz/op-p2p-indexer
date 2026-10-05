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

/// R2 reads for peers in progress at once: as many as `el` answers at once (`MAX_CONCURRENT`
/// in `crates/el/src/serve.rs`; the two change together). A read waits on
/// R2 (100 to 200 ms a GET), not on this host, and a syncing peer keeps four requests open:
/// four places would let one syncing peer starve every other.
const MAX_PEER_READS: usize = 16;
/// Bytes read from R2 for peers per minute: about 70 MB/s, a part of a droplet's link. A
/// syncing peer's segment of OP Mainnet costs about 60 MB (its headers, bodies and receipts
/// each read the segment's blocks), so this serves about 300 such blocks a second; R2 egress
/// is free, the droplet's link is what it spends.
const PEER_BYTES_PER_MINUTE: u64 = 4 * 1024 * 1024 * 1024;
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
