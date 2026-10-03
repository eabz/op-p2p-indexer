//! Persistent node state, stored in an embedded [redb](https://docs.rs/redb) database.
//!
//! Holds the node's secp256k1 identity, so it keeps a stable peer id across restarts, and the
//! peers that recently delivered valid blocks, so a restart can reconnect without waiting for
//! discovery.

use std::path::Path;

use libp2p::Multiaddr;
use libp2p::identity::{DecodingError, secp256k1};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

const NODE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("node");
const IDENTITY_KEY: &str = "secp256k1_secret";
/// Known good peers: multiaddr bytes -> last time (Unix seconds) they delivered a valid block.
const PEERS: TableDefinition<'_, &[u8], u64> = TableDefinition::new("known_peers");
/// Peers kept; the least recently seen is evicted beyond this.
const MAX_KNOWN_PEERS: u64 = 64;

/// Errors from the node store.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The database could not be opened, read, or written.
    #[error("node store database error")]
    Database(#[from] redb::Error),
    /// The store file's permissions could not be restricted to its owner.
    #[error("failed to restrict node store permissions")]
    Permissions(#[source] std::io::Error),
    /// The persisted identity key could not be decoded.
    #[error("stored identity key is invalid")]
    InvalidIdentity(#[source] DecodingError),
}

/// Node state backed by a single redb file.
///
/// The file holds the node's secret key, so on Unix it is made readable by its owner only (0600).
///
/// All methods do blocking disk I/O: call them during startup or from
/// [`tokio::task::spawn_blocking`], never directly from async code.
#[derive(Debug)]
pub struct NodeStore {
    db: Database,
}

impl NodeStore {
    /// Opens the store at `path`, creating the file and its tables if they do not exist.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if the file cannot be created or opened, and
    /// [`StoreError::Permissions`] if its permissions cannot be restricted.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let db = Database::create(path).map_err(redb::Error::from)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(StoreError::Permissions)?;
        }
        let store = Self { db };
        store.create_tables()?;
        Ok(store)
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
        if let Some(mut secret) = self.read_node(IDENTITY_KEY)? {
            let secret = secp256k1::SecretKey::try_from_bytes(&mut secret)
                .map_err(StoreError::InvalidIdentity)?;
            return Ok(secret.into());
        }
        let keypair = secp256k1::Keypair::generate();
        self.write_node(IDENTITY_KEY, &keypair.secret().to_bytes())?;
        Ok(keypair)
    }

    /// Returns the known good peers' dial addresses, most recently seen first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading fails.
    pub fn known_peers(&self) -> Result<Vec<Multiaddr>, StoreError> {
        Ok(self.read_peers()?)
    }

    /// Records that the peer at `addr` delivered a valid block at `seen_secs` (Unix seconds),
    /// evicting the least recently seen peer if the table is full.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if writing fails.
    pub fn save_peer(&self, addr: &Multiaddr, seen_secs: u64) -> Result<(), StoreError> {
        Ok(self.write_peer(addr, seen_secs)?)
    }

    fn create_tables(&self) -> Result<(), redb::Error> {
        let tx = self.db.begin_write()?;
        tx.open_table(NODE)?;
        tx.open_table(PEERS)?;
        tx.commit()?;
        Ok(())
    }

    fn read_node(&self, key: &str) -> Result<Option<Vec<u8>>, redb::Error> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(NODE)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
    }

    fn write_node(&self, key: &str, value: &[u8]) -> Result<(), redb::Error> {
        let tx = self.db.begin_write()?;
        tx.open_table(NODE)?.insert(key, value)?;
        tx.commit()?;
        Ok(())
    }

    fn read_peers(&self) -> Result<Vec<Multiaddr>, redb::Error> {
        let tx = self.db.begin_read()?;
        let mut peers = Vec::new();
        for entry in tx.open_table(PEERS)?.iter()? {
            let (addr, seen_secs) = entry?;
            // Skip entries that no longer parse rather than failing startup.
            if let Ok(addr) = Multiaddr::try_from(addr.value().to_vec()) {
                peers.push((seen_secs.value(), addr));
            }
        }
        peers.sort_unstable_by(|(a, _), (b, _)| b.cmp(a));
        Ok(peers.into_iter().map(|(_, addr)| addr).collect())
    }

    fn write_peer(&self, addr: &Multiaddr, seen_secs: u64) -> Result<(), redb::Error> {
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(PEERS)?;
            table.insert(addr.as_ref(), seen_secs)?;
            if table.len()? > MAX_KNOWN_PEERS {
                let mut oldest: Option<(Vec<u8>, u64)> = None;
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    if oldest.as_ref().is_none_or(|(_, at)| value.value() < *at) {
                        oldest = Some((key.value().to_vec(), value.value()));
                    }
                }
                if let Some((key, _)) = oldest {
                    table.remove(key.as_slice())?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
}
