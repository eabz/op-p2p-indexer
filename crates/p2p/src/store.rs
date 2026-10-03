//! Persistent node state, stored in an embedded [fjall](https://docs.rs/fjall) database.
//!
//! Holds the node's secp256k1 identity, so it keeps a stable peer id across restarts, and the
//! peers that recently delivered valid blocks, so a restart can reconnect without waiting for
//! discovery.
//!
//! This is a second fjall database next to the block archive's, on purpose: the archive lives
//! in the storage crate, and p2p and storage must not depend on each other. It holds one key
//! and a few dozen small entries, far below every size limit fjall has, so the only setting
//! that matters is the number of background threads.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use libp2p::Multiaddr;
use libp2p::identity::{DecodingError, secp256k1};

const NODE: &str = "node";
const IDENTITY_KEY: &str = "secp256k1_secret";
/// Known good peers: multiaddr bytes -> last time (Unix seconds, big-endian) they delivered a
/// valid block.
const PEERS: &str = "known_peers";
/// Peers kept; the least recently seen is evicted beyond this.
const MAX_KNOWN_PEERS: usize = 64;

/// Background threads for flushes and compactions (fjall starts up to four by default); there
/// is almost nothing for them to do.
const WORKER_THREADS: usize = 1;

/// Errors from the node store.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The database could not be opened, read, or written.
    #[error("node store database error")]
    Database(#[from] fjall::Error),
    /// The store directory could not be created.
    #[error("failed to create the node store directory")]
    CreateDir(#[source] std::io::Error),
    /// The store directory's permissions could not be restricted to its owner.
    #[error("failed to restrict node store permissions")]
    Permissions(#[source] std::io::Error),
    /// The persisted identity key could not be decoded.
    #[error("stored identity key is invalid")]
    InvalidIdentity(#[source] DecodingError),
}

/// Node state backed by a fjall database, which is a directory.
///
/// The directory holds the node's secret key, so on Unix it is made accessible by its owner
/// only (0700). Every write is synced to disk before it returns.
///
/// All methods do blocking disk I/O: call them during startup or from
/// [`tokio::task::spawn_blocking`], never directly from async code.
pub struct NodeStore {
    db: Database,
    node: Keyspace,
    peers: Keyspace,
    /// Serializes the read-then-write of [`Self::identity`] and [`Self::save_peer`].
    write: Mutex<()>,
}

// fjall's handles are not `Debug`.
impl fmt::Debug for NodeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeStore").finish_non_exhaustive()
    }
}

impl NodeStore {
    /// Opens the store in the directory `path`, creating it and its keyspaces if they do not
    /// exist.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::CreateDir`] if the directory cannot be created,
    /// [`StoreError::Permissions`] if its permissions cannot be restricted, and
    /// [`StoreError::Database`] if the database in it cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        // Restricted before fjall writes anything into it.
        std::fs::create_dir_all(path).map_err(StoreError::CreateDir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .map_err(StoreError::Permissions)?;
        }
        let db = Database::builder(path)
            .worker_threads(WORKER_THREADS)
            .open()?;
        Ok(Self {
            node: db.keyspace(NODE, KeyspaceCreateOptions::default)?,
            peers: db.keyspace(PEERS, KeyspaceCreateOptions::default)?,
            db,
            write: Mutex::new(()),
        })
    }

    /// Returns the node's secp256k1 identity, generating and persisting one on first use.
    ///
    /// The OP Stack uses the same secp256k1 key for libp2p and discv5.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading or writing fails, and
    /// [`StoreError::InvalidIdentity`] if the stored key cannot be decoded.
    pub fn identity(&self) -> Result<secp256k1::Keypair, StoreError> {
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(secret) = self.node.get(IDENTITY_KEY)? {
            let secret = secp256k1::SecretKey::try_from_bytes(secret.to_vec())
                .map_err(StoreError::InvalidIdentity)?;
            return Ok(secret.into());
        }
        let keypair = secp256k1::Keypair::generate();
        let mut batch = self.durable_batch();
        batch.insert(&self.node, IDENTITY_KEY, keypair.secret().to_bytes());
        batch.commit()?;
        Ok(keypair)
    }

    /// Returns the known good peers' dial addresses, most recently seen first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading fails.
    pub fn known_peers(&self) -> Result<Vec<Multiaddr>, StoreError> {
        let mut peers: Vec<(u64, Multiaddr)> = self
            .read_peers()?
            .into_iter()
            // Skip entries that no longer parse rather than failing startup.
            .filter_map(|(addr, seen_secs)| Some((seen_secs, Multiaddr::try_from(addr).ok()?)))
            .collect();
        peers.sort_unstable_by_key(|(seen_secs, _)| Reverse(*seen_secs));
        Ok(peers.into_iter().map(|(_, addr)| addr).collect())
    }

    /// Records that the peer at `addr` delivered a valid block at `seen_secs` (Unix seconds),
    /// evicting the least recently seen peer if the table is full.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if writing fails.
    pub fn save_peer(&self, addr: &Multiaddr, seen_secs: u64) -> Result<(), StoreError> {
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        let mut peers = self.read_peers()?;
        peers.insert(addr.to_vec(), seen_secs);
        let evicted = if peers.len() > MAX_KNOWN_PEERS {
            let oldest = peers.iter().min_by_key(|(_, seen_secs)| **seen_secs);
            oldest.map(|(addr, _)| addr.as_slice())
        } else {
            None
        };
        // The new peer is itself the least recently seen: nothing changes.
        if evicted == Some(addr.as_ref()) {
            return Ok(());
        }

        let mut batch = self.durable_batch();
        batch.insert(&self.peers, addr.as_ref(), seen_secs.to_be_bytes());
        if let Some(evicted) = evicted {
            batch.remove(&self.peers, evicted);
        }
        batch.commit()?;
        Ok(())
    }

    /// Returns the stored peers: multiaddr bytes and when each was last seen.
    fn read_peers(&self) -> Result<BTreeMap<Vec<u8>, u64>, fjall::Error> {
        let mut peers = BTreeMap::new();
        for entry in self.peers.iter() {
            let (addr, seen_secs) = entry.into_inner()?;
            // An entry of another shape was not written by this code; skip it.
            if let Ok(seen_secs) = <[u8; 8]>::try_from(seen_secs.as_ref()) {
                peers.insert(addr.to_vec(), u64::from_be_bytes(seen_secs));
            }
        }
        Ok(peers)
    }

    /// A write batch that is synced to disk before `commit` returns.
    fn durable_batch(&self) -> fjall::OwnedWriteBatch {
        self.db.batch().durability(Some(PersistMode::SyncAll))
    }
}
