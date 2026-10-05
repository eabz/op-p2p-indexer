//! Persistent node state, stored in an embedded [fjall](https://docs.rs/fjall) database.
//!
//! Holds the node's secp256k1 identity, so it keeps a stable peer id across restarts, a second
//! key for its identity on the execution network, and the peers worth returning to, so a
//! restart can reconnect without waiting for discovery: consensus peers that recently
//! delivered valid blocks, and execution peers that served requests. It also keeps the progress
//! of a range sync, so a restart resumes it, and the L1 light client's newest finalized beacon
//! block, so a restart bootstraps from it. It records the chain it was made for, so a node of
//! another chain refuses it instead of dialing that chain's peers.
//!
//! This is a second fjall database next to the block archive's, on purpose: the archive lives
//! in the storage crate, and p2p and storage must not depend on each other. It holds two keys,
//! a few dozen small entries and, for a range sync, one 32-byte hash per 256 blocks of the
//! range, far below every size limit fjall has, so the only setting that matters is the number
//! of background threads.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use alloy_primitives::{B256, B512, BlockNumber};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use libp2p::Multiaddr;
use libp2p::identity::{DecodingError, secp256k1};
use op_indexer_primitives::{BeaconCheckpoint, BlockRef, ChainIdentity, ExecutionPeer};

const NODE: &str = "node";
const IDENTITY_KEY: &str = "secp256k1_secret";
/// Secret of the node's identity on the execution network; never the same as [`IDENTITY_KEY`].
const EXECUTION_KEY: &str = "execution_secp256k1_secret";
/// Secret of the node's identity on L1's execution network; a third key.
const L1_KEY: &str = "l1_secp256k1_secret";
/// The chain the store's peers and sync progress belong to ([`ChainIdentity::to_bytes`]).
const CHAIN_KEY: &str = "chain";
/// The newest finalized beacon block the L1 light client verified ([`BeaconCheckpoint`]): the
/// configured checkpoint it descends from (32 bytes), its root (32 bytes) and its slot (8
/// bytes, big-endian).
const L1_CHECKPOINT_KEY: &str = "l1_beacon_checkpoint";
/// Known good peers: multiaddr bytes -> last time (Unix seconds, big-endian) they delivered a
/// valid block.
const PEERS: &str = "known_peers";
/// Failed dials in a row, across restarts, after which a known peer is forgotten.
const KNOWN_PEER_FAILURES: u8 = 3;
/// Peers kept; the least recently seen is evicted beyond this.
const MAX_KNOWN_PEERS: usize = 64;
/// Execution peers that served us: node id (64 bytes) -> last time they served a request
/// (Unix seconds, big-endian) followed by their TCP address as text.
const EXECUTION_PEERS: &str = "execution_peers";
/// Peers of L1's execution network that served us, in the shape of [`EXECUTION_PEERS`].
const L1_PEERS: &str = "l1_execution_peers";
/// Execution peers kept per network; the least recently served is evicted beyond this.
const MAX_EXECUTION_PEERS: usize = 32;
/// The verified checkpoints of a range sync, and the anchor they were verified from.
const SYNC: &str = "sync";
/// The anchor the checkpoints belong to: its number (big-endian) and hash. Checkpoints of
/// another anchor are discarded.
const SYNC_ANCHOR_KEY: &[u8] = b"anchor";
/// Prefix of a checkpoint: followed by the block number, big-endian, so they iterate in
/// block order; the value is the block hash.
const SYNC_CHECKPOINT_PREFIX: u8 = b'c';

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
    /// A persisted execution-network key (the L2 one or the L1 one) has the wrong length.
    #[error("stored execution key is {len} bytes, not 32")]
    InvalidExecutionKey {
        /// Length of the stored value.
        len: usize,
    },
    /// The store was made by a node of another chain: its saved peers and sync progress are
    /// that chain's. Nothing is deleted.
    #[error(
        "the node store in {} belongs to {found}, but this node runs {expected}; use a data \
         directory of chain {}, or delete this one",
        path.display(),
        expected.chain_id
    )]
    WrongChain {
        /// The store directory.
        path: PathBuf,
        /// The chain recorded in the store.
        found: ChainIdentity,
        /// The chain this node runs.
        expected: ChainIdentity,
    },
    /// The store's chain record does not decode. Nothing is deleted.
    #[error(
        "the chain record of the node store in {} is unreadable; the directory is left as it is",
        path.display()
    )]
    UnreadableChain {
        /// The store directory.
        path: PathBuf,
    },
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
    execution_peers: Keyspace,
    l1_peers: Keyspace,
    sync: Keyspace,
    /// Serializes the read-then-write of the keys, both peer tables and the sync progress.
    write: Mutex<()>,
}

// fjall's handles are not `Debug`.
impl fmt::Debug for NodeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeStore").finish_non_exhaustive()
    }
}

impl NodeStore {
    /// Opens the store of `chain` in the directory `path`, creating it and its keyspaces if
    /// they do not exist.
    ///
    /// The store records its chain when it is created; see `check_chain` for one made before
    /// the record.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::CreateDir`] if the directory cannot be created,
    /// [`StoreError::Permissions`] if its permissions cannot be restricted,
    /// [`StoreError::WrongChain`] if it belongs to another chain,
    /// [`StoreError::UnreadableChain`] if its chain record does not decode, and
    /// [`StoreError::Database`] if the database in it cannot be opened.
    pub fn open(path: impl AsRef<Path>, chain: ChainIdentity) -> Result<Self, StoreError> {
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
        let store = Self {
            node: db.keyspace(NODE, KeyspaceCreateOptions::default)?,
            peers: db.keyspace(PEERS, KeyspaceCreateOptions::default)?,
            execution_peers: db.keyspace(EXECUTION_PEERS, KeyspaceCreateOptions::default)?,
            l1_peers: db.keyspace(L1_PEERS, KeyspaceCreateOptions::default)?,
            sync: db.keyspace(SYNC, KeyspaceCreateOptions::default)?,
            db,
            write: Mutex::new(()),
        };
        store.check_chain(path, chain)?;
        Ok(store)
    }

    /// Checks that the store belongs to `chain`. A store with no chain recorded is given one
    /// first: if it holds anything (an identity, saved peers, sync progress) it was made by a
    /// build before the record, so it is [`ChainIdentity::BEFORE_RECORD`]'s; if it is empty,
    /// `chain`'s.
    fn check_chain(&self, path: &Path, chain: ChainIdentity) -> Result<(), StoreError> {
        let found = if let Some(record) = self.node.get(CHAIN_KEY)? {
            ChainIdentity::from_bytes(&record).ok_or_else(|| StoreError::UnreadableChain {
                path: path.to_owned(),
            })?
        } else {
            let holds_data = [
                &self.node,
                &self.peers,
                &self.execution_peers,
                &self.l1_peers,
                &self.sync,
            ]
            .iter()
            .any(|keyspace| keyspace.first_key_value().is_some());
            let found = if holds_data {
                ChainIdentity::BEFORE_RECORD
            } else {
                chain
            };
            let mut batch = self.durable_batch();
            batch.insert(&self.node, CHAIN_KEY, found.to_bytes());
            batch.commit()?;
            found
        };
        if found == chain {
            return Ok(());
        }
        Err(StoreError::WrongChain {
            path: path.to_owned(),
            found,
            expected: chain,
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

    /// Returns the secret key of the node's identity on the execution network (devp2p),
    /// generating and persisting one on first use.
    ///
    /// It is a second secp256k1 key, not the one of [`Self::identity`]: both networks run a
    /// discv5 node, and one node id announcing two different records would look like a node
    /// flapping between two addresses.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading or writing fails, and
    /// [`StoreError::InvalidExecutionKey`] if the stored key is not 32 bytes.
    pub fn execution_key(&self) -> Result<B256, StoreError> {
        self.network_key(EXECUTION_KEY)
    }

    /// Returns the secp256k1 secret of the node's identity on L1's execution network,
    /// generating and persisting one on first use. A third key, for the same reason
    /// [`Self::execution_key`] is a second one.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading or writing fails, and
    /// [`StoreError::InvalidExecutionKey`] if the stored key is not 32 bytes.
    pub fn l1_key(&self) -> Result<B256, StoreError> {
        self.network_key(L1_KEY)
    }

    /// Returns the secret stored under `name`, generating and persisting one on first use.
    fn network_key(&self, name: &str) -> Result<B256, StoreError> {
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(secret) = self.node.get(name)? {
            return B256::try_from(secret.as_ref())
                .map_err(|_err| StoreError::InvalidExecutionKey { len: secret.len() });
        }
        let secret = B256::from(secp256k1::Keypair::generate().secret().to_bytes());
        let mut batch = self.durable_batch();
        batch.insert(&self.node, name, secret.as_slice());
        batch.commit()?;
        Ok(secret)
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
        self.save_evicting(
            &self.peers,
            MAX_KNOWN_PEERS,
            addr.as_ref(),
            &seen_secs.to_be_bytes(),
        )
    }

    /// Records that dialing the known peer at `addr` failed. After three
    /// failures in a row, across restarts, it is forgotten; [`Self::save_peer`] starts the count
    /// again. Returns whether it was forgotten. A peer not saved is a no-op.
    ///
    /// The count is one byte after the time the peer was last seen.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading or writing fails.
    pub fn peer_failed(&self, addr: &Multiaddr) -> Result<bool, StoreError> {
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(value) = self.peers.get(addr.as_ref())? else {
            return Ok(false);
        };
        let Some((seen, rest)) = value.split_first_chunk::<8>() else {
            return Ok(false);
        };
        let failures = rest.first().copied().unwrap_or(0).saturating_add(1);
        let mut batch = self.durable_batch();
        let forgotten = failures >= KNOWN_PEER_FAILURES;
        if forgotten {
            batch.remove(&self.peers, addr.as_ref());
        } else {
            let mut value = seen.to_vec();
            value.push(failures);
            batch.insert(&self.peers, addr.as_ref(), value);
        }
        batch.commit()?;
        Ok(forgotten)
    }

    /// Returns the saved execution peers, most recently served first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading fails.
    pub fn execution_peers(&self) -> Result<Vec<ExecutionPeer>, StoreError> {
        Self::read_execution_peers(&self.execution_peers)
    }

    /// Records an execution peer that served a request, replacing what was saved for its node
    /// id and evicting the least recently served peer if the table is full.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if writing fails.
    pub fn save_execution_peer(&self, peer: &ExecutionPeer) -> Result<(), StoreError> {
        self.write_execution_peer(&self.execution_peers, peer)
    }

    /// Returns the saved peers of L1's execution network, most recently served first. A
    /// table of its own: the two networks do not share peers.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading fails.
    pub fn l1_peers(&self) -> Result<Vec<ExecutionPeer>, StoreError> {
        Self::read_execution_peers(&self.l1_peers)
    }

    /// Records a peer of L1's execution network that served a request, as
    /// [`Self::save_execution_peer`] does for the chain's own.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if writing fails.
    pub fn save_l1_peer(&self, peer: &ExecutionPeer) -> Result<(), StoreError> {
        self.write_execution_peer(&self.l1_peers, peer)
    }

    /// Returns the newest finalized beacon block the L1 light client saved, if any. One of
    /// another shape (not written by this code) is taken as none.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading fails.
    pub fn l1_checkpoint(&self) -> Result<Option<BeaconCheckpoint>, StoreError> {
        Ok(self
            .node
            .get(L1_CHECKPOINT_KEY)?
            .as_deref()
            .and_then(decode_l1_checkpoint))
    }

    /// Saves the newest finalized beacon block the L1 light client verified, replacing the
    /// one saved before.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if writing fails.
    pub fn save_l1_checkpoint(&self, checkpoint: &BeaconCheckpoint) -> Result<(), StoreError> {
        let mut value = checkpoint.origin.to_vec();
        value.extend_from_slice(checkpoint.root.as_slice());
        value.extend_from_slice(&checkpoint.slot.to_be_bytes());
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        let mut batch = self.durable_batch();
        batch.insert(&self.node, L1_CHECKPOINT_KEY, value);
        batch.commit()?;
        Ok(())
    }

    fn read_execution_peers(table: &Keyspace) -> Result<Vec<ExecutionPeer>, StoreError> {
        let mut peers = Vec::new();
        for entry in table.iter() {
            let (id, value) = entry.into_inner()?;
            // An entry of another shape was not written by this code; skip it.
            peers.extend(decode_execution_peer(&id, &value));
        }
        peers.sort_unstable_by_key(|peer| Reverse(peer.last_served_secs));
        Ok(peers)
    }

    fn write_execution_peer(
        &self,
        table: &Keyspace,
        peer: &ExecutionPeer,
    ) -> Result<(), StoreError> {
        let mut value = peer.last_served_secs.to_be_bytes().to_vec();
        value.extend_from_slice(peer.addr.to_string().as_bytes());
        self.save_evicting(table, MAX_EXECUTION_PEERS, peer.id.as_slice(), &value)
    }

    /// Writes `key -> value` to a peer table, whose values start with a time (Unix seconds,
    /// big-endian), and evicts the entry with the oldest time if the table then holds more
    /// than `max`. When the new entry is itself the oldest, nothing changes.
    fn save_evicting(
        &self,
        table: &Keyspace,
        max: usize,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        let time = |value: &[u8]| {
            value
                .first_chunk::<8>()
                .map(|secs| u64::from_be_bytes(*secs))
        };
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        let mut oldest = time(value).map(|secs| (secs, key.to_vec()));
        let mut entries = 1_usize;
        for entry in table.iter() {
            let (other, other_value) = entry.into_inner()?;
            // An entry of another shape was not written by this code; it is not counted.
            let Some(secs) = time(&other_value).filter(|_| *other != *key) else {
                continue;
            };
            entries = entries.saturating_add(1);
            if oldest.as_ref().is_none_or(|(oldest, _)| secs < *oldest) {
                oldest = Some((secs, other.to_vec()));
            }
        }
        let evicted = oldest.filter(|_| entries > max).map(|(_, key)| key);
        if evicted.as_deref() == Some(key) {
            return Ok(());
        }

        let mut batch = self.durable_batch();
        batch.insert(table, key, value);
        if let Some(evicted) = evicted {
            batch.remove(table, evicted);
        }
        batch.commit()?;
        Ok(())
    }

    /// Returns the anchor of the range sync whose checkpoints are saved, if any.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading fails.
    pub fn sync_anchor(&self) -> Result<Option<BlockRef>, StoreError> {
        let Some(id) = self.sync.get(SYNC_ANCHOR_KEY)? else {
            return Ok(None);
        };
        Ok(id.split_first_chunk::<8>().and_then(|(number, hash)| {
            Some(BlockRef {
                number: u64::from_be_bytes(*number),
                hash: B256::try_from(hash).ok()?,
            })
        }))
    }

    /// Returns the blocks whose hash a range sync up to `anchor` has verified, ascending, and
    /// records `anchor` as the one being synced to.
    /// Checkpoints saved for another anchor are removed first: they prove nothing about this
    /// one.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if reading or writing fails.
    pub fn sync_checkpoints(&self, anchor: BlockRef) -> Result<Vec<BlockRef>, StoreError> {
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        let mut id = anchor.number.to_be_bytes().to_vec();
        id.extend_from_slice(anchor.hash.as_slice());
        if self.sync.get(SYNC_ANCHOR_KEY)?.as_deref() != Some(id.as_slice()) {
            let mut batch = self.durable_batch();
            for key in self.sync.iter() {
                batch.remove(&self.sync, key.key()?);
            }
            batch.insert(&self.sync, SYNC_ANCHOR_KEY, id);
            batch.commit()?;
            return Ok(Vec::new());
        }

        let mut checkpoints = Vec::new();
        for entry in self.sync.prefix([SYNC_CHECKPOINT_PREFIX]) {
            let (key, hash) = entry.into_inner()?;
            // An entry of another shape was not written by this code; skip it.
            if let Some(checkpoint) = decode_checkpoint(&key, &hash) {
                checkpoints.push(checkpoint);
            }
        }
        Ok(checkpoints)
    }

    /// Saves blocks of the range sync whose hash is verified.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] if writing fails.
    pub fn save_sync_checkpoints(&self, checkpoints: &[BlockRef]) -> Result<(), StoreError> {
        let _write = self.write.lock().unwrap_or_else(PoisonError::into_inner);
        let mut batch = self.durable_batch();
        for checkpoint in checkpoints {
            batch.insert(
                &self.sync,
                checkpoint_key(checkpoint.number),
                checkpoint.hash.as_slice(),
            );
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

/// The key of the checkpoint at block `number`.
fn checkpoint_key(number: BlockNumber) -> [u8; 9] {
    let mut key = [SYNC_CHECKPOINT_PREFIX; 9];
    key[1..].copy_from_slice(&number.to_be_bytes());
    key
}

/// Decodes one checkpoint of the sync table; `None` if it has another shape.
fn decode_checkpoint(key: &[u8], hash: &[u8]) -> Option<BlockRef> {
    let (_prefix, number) = key.split_first()?;
    Some(BlockRef {
        number: u64::from_be_bytes(number.try_into().ok()?),
        hash: B256::try_from(hash).ok()?,
    })
}

/// Decodes the saved beacon checkpoint; `None` if it has another shape.
fn decode_l1_checkpoint(value: &[u8]) -> Option<BeaconCheckpoint> {
    let (origin, rest) = value.split_first_chunk::<32>()?;
    let (root, slot) = rest.split_first_chunk::<32>()?;
    Some(BeaconCheckpoint {
        origin: B256::from(*origin),
        root: B256::from(*root),
        slot: u64::from_be_bytes(<[u8; 8]>::try_from(slot).ok()?),
    })
}

/// Decodes one entry of the execution peers table: the time it last served (8 bytes) and its
/// address; `None` if it has another shape, such as an entry of an earlier build, which is
/// then forgotten.
fn decode_execution_peer(id: &[u8], value: &[u8]) -> Option<ExecutionPeer> {
    let (served, addr) = value.split_first_chunk::<8>()?;
    Some(ExecutionPeer {
        id: B512::try_from(id).ok()?,
        addr: std::str::from_utf8(addr).ok()?.parse::<SocketAddr>().ok()?,
        last_served_secs: u64::from_be_bytes(*served),
    })
}
