//! The Redis key layout (docs/storage.md section 3.1) and the limits the store runs with
//! (section 8). Every key is built here, from the prefix `opidx:{chain_id}:`; the Lua scripts
//! receive these keys and only append a hash or a number.

use std::time::Duration;

use alloy_primitives::{BlockHash, ChainId};

/// Limit for establishing a connection.
pub(super) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Limit for one request, including a reconnect it may have to wait for. The only limit: the
/// connection's own response timeout is off, so every timeout is a
/// [`StorageError::Timeout`](crate::StorageError::Timeout).
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Limit for an operation that makes several requests:
/// [`ancestry`](crate::UnsafeStore::ancestry), [`prune`](crate::UnsafeStore::prune), the schema
/// wipe.
/// Checked between requests, so the operation ends within this plus one [`REQUEST_TIMEOUT`].
pub(super) const OPERATION_DEADLINE: Duration = Duration::from_secs(60);
/// Keys asked for per `SCAN` step when wiping a store with another schema version.
pub(super) const WIPE_SCAN_COUNT: u32 = 1000;

/// Version of this key layout, stored in `…:schema_version`. Bump it on any change to the keys
/// or to the shape of their values: a store written with another version is wiped on connect.
pub(super) const SCHEMA_VERSION: &str = "1";
/// Lifetime of block-scoped keys, a backstop for when nothing prunes them. Also the horizon of
/// the heights kept below the head: `insert` drops heights whose keys have expired.
pub(super) const UNSAFE_TTL: Duration = Duration::from_hours(24);
/// Most parent links fork choice follows to connect a block to the canonical chain.
pub(super) const MAX_REORG_DEPTH: u32 = 256;
/// Expired heights that one insert removes. More than one, so the store
/// catches up after falling behind.
pub(super) const RETENTION_HEIGHTS_PER_INSERT: u32 = 16;
/// Heights one prune script call removes; a longer prune takes several calls, so that no
/// single script blocks Redis for long.
pub(super) const PRUNE_HEIGHTS_PER_CALL: u32 = 1024;
/// Blocks removed from one height per step; a height holding more takes another step.
pub(super) const REMOVE_BLOCKS_PER_STEP: u32 = 64;
/// Most blocks one [`ancestry`](crate::UnsafeStore::ancestry) call loads, bounding its memory
/// and round trips.
pub(super) const MAX_ANCESTRY_BLOCKS: u64 = 1024;
/// Approximate cap of the event stream (`MAXLEN ~`).
pub(super) const EVENTS_MAXLEN: u32 = 10_000;

/// Builds the keys of one chain.
#[derive(Debug)]
pub(super) struct Keys {
    prefix: String,
}

impl Keys {
    pub(super) fn new(chain_id: ChainId) -> Self {
        Self {
            prefix: format!("opidx:{chain_id}:"),
        }
    }

    /// `SCAN` pattern matching every key of the chain.
    pub(super) fn pattern(&self) -> String {
        self.key("*")
    }

    pub(super) fn schema_version(&self) -> String {
        self.key("schema_version")
    }

    /// Hash of one stored block.
    pub(super) fn block(&self, hash: BlockHash) -> String {
        format!("{}{hash}", self.block_prefix())
    }

    /// Prefix of [`Self::block`] keys; the scripts append the hash.
    pub(super) fn block_prefix(&self) -> String {
        self.key("block:")
    }

    /// Prefix of the per-height sets of block hashes; the scripts append the number.
    pub(super) fn height_prefix(&self) -> String {
        self.key("height:")
    }

    /// Sorted set of the canonical chain: score = number, member = hash.
    pub(super) fn canonical(&self) -> String {
        self.key("canonical")
    }

    pub(super) fn head(&self) -> String {
        self.key("head")
    }

    pub(super) fn safe_head(&self) -> String {
        self.key("safe_head")
    }

    pub(super) fn finalized_head(&self) -> String {
        self.key("finalized_head")
    }

    /// Sorted set of the heights that hold blocks: score = member = number.
    pub(super) fn heights(&self) -> String {
        self.key("heights")
    }

    /// Stream of events for live readers.
    pub(super) fn events(&self) -> String {
        self.key("events")
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}
