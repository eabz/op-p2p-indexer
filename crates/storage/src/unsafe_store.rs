//! Unsafe store on Redis: unsafe blocks, fork choice and reorgs, and the event stream for readers.
//!
//! Layout and rules are in `docs/storage.md` section 3. Writes that touch several keys run as
//! Lua scripts (`crates/storage/scripts/`), so readers never see a half-applied change; the
//! scripts own fork choice, this module only encodes, calls and decodes.
//!
//! Blocks leave Redis when the caller prunes, when they fall behind the retention horizon, or
//! when their keys expire; the store never decides that a block is committed.

mod codec;
mod layout;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, UNIX_EPOCH};

use alloy_primitives::{BlockHash, BlockNumber, ChainId};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_primitives::{BlockRef, DecodedBlock, InsertOutcome, L1Heads, UnsafeEvent};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{RedisResult, Script, ScriptInvocation};
use tokio::time::{Instant, timeout};
use tracing::{debug, warn};

use self::layout::{
    CONNECT_TIMEOUT, EVENTS_MAXLEN, Keys, MAX_ANCESTRY_BLOCKS, MAX_REORG_DEPTH, OPERATION_DEADLINE,
    PRUNE_HEIGHTS_PER_CALL, REMOVE_BLOCKS_PER_STEP, REQUEST_TIMEOUT, RETENTION_HEIGHTS_PER_INSERT,
    SCHEMA_VERSION, UNSAFE_TTL, WIPE_SCAN_COUNT,
};
use crate::metrics::{self, Operation};
use crate::validate::validate_block;
use crate::{EventId, Events, InvalidBlockReason, RedisConfig, StorageError, Store, UnsafeStore};

/// Entries of the event stream as `XRANGE` and `XREAD` return them: id and fields.
type StreamEntries = Vec<(String, HashMap<String, String>)>;

static INSERT: LazyLock<Script> = LazyLock::new(|| script(include_str!("../scripts/insert.lua")));
static SET_RECEIPTS: LazyLock<Script> =
    LazyLock::new(|| script(include_str!("../scripts/set_receipts.lua")));
static PRUNE: LazyLock<Script> = LazyLock::new(|| script(include_str!("../scripts/prune.lua")));

/// The unsafe store of one chain. Cheap to clone: clones share one multiplexed connection, which
/// reconnects on its own, and a second one for [`UnsafeStore::events`], whose waits would hold
/// up every other call on the first (Redis answers one connection's commands in order).
///
/// The redis client's `Debug` redacts the password, so deriving it here is safe.
#[derive(Debug, Clone)]
pub struct RedisStore {
    connection: ConnectionManager,
    /// For blocking reads of the event stream only.
    events: ConnectionManager,
    keys: Arc<Keys>,
}

impl RedisStore {
    /// Connects, pings, and checks the schema version: a store written with another key layout
    /// is emptied, since unsafe blocks are disposable. The wipe has an overall deadline; one
    /// that is cut short is run again by the next connect.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the URL is invalid or Redis cannot be reached.
    pub async fn connect(config: &RedisConfig, chain_id: ChainId) -> Result<Self, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Connect, async {
            let client =
                redis::Client::open(config.url.as_str()).map_err(|source| StorageError::Redis {
                    operation: "connect",
                    source,
                })?;
            let manager_config = ConnectionManagerConfig::new()
                .set_connection_timeout(Some(CONNECT_TIMEOUT))
                .set_response_timeout(None);
            let connection = request(
                "connect",
                ConnectionManager::new_with_config(client.clone(), manager_config.clone()),
            )
            .await?;
            let events = request(
                "connect",
                ConnectionManager::new_with_config(client, manager_config),
            )
            .await?;
            let store = Self {
                connection,
                events,
                keys: Arc::new(Keys::new(chain_id)),
            };
            store.ping().await?;
            store.check_schema().await?;
            Ok(store)
        })
        .await
    }

    /// Checks that Redis answers.
    async fn ping(&self) -> Result<(), StorageError> {
        let mut connection = self.connection.clone();
        request("ping", redis::cmd("PING").query_async(&mut connection)).await
    }

    /// Applies the schema-version rule (docs/storage.md section 5.2).
    async fn check_schema(&self) -> Result<(), StorageError> {
        let mut connection = self.connection.clone();
        let key = self.keys.schema_version();
        let stored: Option<String> = request(
            "schema check",
            redis::cmd("GET").arg(&key).query_async(&mut connection),
        )
        .await?;
        match stored.as_deref() {
            Some(SCHEMA_VERSION) => return Ok(()),
            Some(stored) => {
                warn!(
                    stored,
                    current = SCHEMA_VERSION,
                    "unsafe store was written with another schema version, deleting its keys"
                );
                self.wipe(&key).await?;
            }
            None => {}
        }
        request(
            "schema check",
            redis::cmd("SET")
                .arg(&key)
                .arg(SCHEMA_VERSION)
                .query_async(&mut connection),
        )
        .await
    }

    /// Deletes every key of this chain except `keep`, in `SCAN` steps so Redis stays
    /// responsive. The schema version is kept and only overwritten once this returns, so a
    /// wipe that was interrupted is run again on the next connect.
    async fn wipe(&self, keep: &str) -> Result<(), StorageError> {
        let mut connection = self.connection.clone();
        let pattern = self.keys.pattern();
        let deadline = Deadline::start("wipe");
        let mut cursor = 0_u64;
        loop {
            deadline.check()?;
            let (next, mut keys): (u64, Vec<String>) = request(
                "wipe",
                redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(WIPE_SCAN_COUNT)
                    .query_async(&mut connection),
            )
            .await?;
            keys.retain(|key| key != keep);
            if !keys.is_empty() {
                request::<()>(
                    "wipe",
                    redis::cmd("UNLINK").arg(keys).query_async(&mut connection),
                )
                .await?;
            }
            if next == 0 {
                return Ok(());
            }
            cursor = next;
        }
    }

    /// Starts a call of `script` with the keys every script takes.
    fn invoke<'a>(&self, script: &'a Script) -> ScriptInvocation<'a> {
        let keys = &self.keys;
        let mut invocation = script.prepare_invoke();
        invocation
            .key(keys.block_prefix())
            .key(keys.height_prefix())
            .key(keys.canonical())
            .key(keys.head())
            .key(keys.safe_head())
            .key(keys.events())
            .key(keys.heights());
        invocation
    }

    /// Reads the fields of the Redis hash at `key`; empty if the key does not exist.
    async fn hash_fields(
        &self,
        operation: &'static str,
        key: String,
    ) -> Result<HashMap<String, String>, StorageError> {
        let mut connection = self.connection.clone();
        request(
            operation,
            redis::cmd("HGETALL").arg(key).query_async(&mut connection),
        )
        .await
    }

    /// Runs the prune script until it reports that nothing at or below `up_to` is left, adding
    /// the blocks each step removed to `removed`. Every step removes blocks or heights, so it
    /// ends; a prune cut short by an error or the deadline is finished by the next one.
    async fn prune_steps(&self, up_to: BlockRef, removed: &mut usize) -> Result<(), StorageError> {
        let mut connection = self.connection.clone();
        let deadline = Deadline::start("prune");
        loop {
            deadline.check()?;
            let mut invocation = self.invoke(&PRUNE);
            invocation.arg(up_to.number).arg(up_to.hash.to_string());
            let (done, blocks): (bool, usize) =
                request("prune", invocation.invoke_async(&mut connection)).await?;
            *removed = removed.saturating_add(blocks);
            if done {
                return Ok(());
            }
        }
    }

    async fn stored_block(
        &self,
        operation: &'static str,
        hash: BlockHash,
    ) -> Result<Option<DecodedBlock>, StorageError> {
        let fields = self.hash_fields(operation, self.keys.block(hash)).await?;
        if fields.is_empty() {
            return Ok(None);
        }
        codec::decode_block(hash, &fields).map(Some)
    }
}

impl UnsafeStore for RedisStore {
    async fn insert(&self, block: &DecodedBlock) -> Result<InsertOutcome, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Insert, async {
            let header = &block.block.header;
            validate_block(block)?;
            let encoded = codec::encode_block(block)?;
            // A clock set before 1970 gives 0; the field is informational.
            let received_at_ms = UNIX_EPOCH
                .elapsed()
                .map_or(0, |elapsed| elapsed.as_millis());
            let mut connection = self.connection.clone();
            let mut invocation = self.invoke(&INSERT);
            invocation
                .arg(header.number)
                .arg(block.hash.to_string())
                .arg(header.parent_hash.to_string())
                .arg(header.timestamp)
                .arg(encoded.header)
                .arg(encoded.transactions)
                .arg(encoded.receipts)
                .arg(encoded.source)
                .arg(received_at_ms)
                .arg(encoded.tx_count);
            let (status, events): (i8, Vec<HashMap<String, String>>) =
                request("insert", invocation.invoke_async(&mut connection)).await?;
            // 1: stored. 0: not stored. -1: its number contradicts its parent's.
            let stored = match status {
                1 => true,
                0 => false,
                -1 => {
                    return Err(StorageError::InvalidBlock {
                        number: header.number,
                        reason: InvalidBlockReason::ParentNumber,
                    });
                }
                _ => return Err(invalid_reply(block.hash)),
            };
            let events = codec::decode_events(&events)?;

            if stored {
                metrics::blocks_inserted(Store::Unsafe, 1);
            }
            for event in &events {
                if let UnsafeEvent::Reorg(reorg) = event {
                    metrics::reorg(reorg.replaced.len());
                }
            }
            debug!(number = header.number, hash = %block.hash, stored, ?events, "inserted block");
            Ok(InsertOutcome { stored, events })
        })
        .await
    }

    async fn set_receipts(
        &self,
        block: BlockRef,
        receipts: &[OpReceiptEnvelope],
    ) -> Result<bool, StorageError> {
        metrics::timed(Store::Unsafe, Operation::SetReceipts, async {
            let encoded = codec::encode_receipts(receipts)?;
            let mut connection = self.connection.clone();
            let mut invocation = self.invoke(&SET_RECEIPTS);
            invocation
                .arg(block.number)
                .arg(block.hash.to_string())
                .arg(encoded)
                .arg(receipts.len());
            // 1: written. 0: the block is not stored. -1, -2: the receipts do not fit it.
            let status: i8 =
                request("set_receipts", invocation.invoke_async(&mut connection)).await?;
            let reason = match status {
                1 => {
                    metrics::receipts_attached();
                    return Ok(true);
                }
                0 => return Ok(false),
                -1 => InvalidBlockReason::ReceiptCount,
                -2 => InvalidBlockReason::StoredNumber,
                _ => return Err(invalid_reply(block.hash)),
            };
            Err(StorageError::InvalidBlock {
                number: block.number,
                reason,
            })
        })
        .await
    }

    async fn ancestry(
        &self,
        head: BlockRef,
        stop_at: BlockNumber,
    ) -> Result<Vec<DecodedBlock>, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Ancestry, async {
            let requested = head.number.saturating_sub(stop_at);
            if requested > MAX_ANCESTRY_BLOCKS {
                return Err(StorageError::AncestryTooLong {
                    requested,
                    max: MAX_ANCESTRY_BLOCKS,
                });
            }
            let deadline = Deadline::start("ancestry");
            let mut blocks = Vec::new();
            let mut next = head;
            while next.number > stop_at {
                deadline.check()?;
                let block = self.stored_block("ancestry", next.hash).await?.ok_or(
                    StorageError::MissingAncestor {
                        hash: next.hash,
                        number: next.number,
                    },
                )?;
                let header = &block.block.header;
                if header.number != next.number {
                    return Err(StorageError::InvalidData {
                        store: Store::Unsafe,
                        what: "block number",
                        block: Some(next.hash),
                        source: None,
                    });
                }
                next = BlockRef {
                    // Above `stop_at`, so at least 1.
                    number: next.number.saturating_sub(1),
                    hash: header.parent_hash,
                };
                blocks.push(block);
            }
            blocks.reverse();
            Ok(blocks)
        })
        .await
    }

    async fn prune(&self, up_to: BlockRef) -> Result<(), StorageError> {
        metrics::timed(Store::Unsafe, Operation::Prune, async {
            let mut removed = 0;
            let result = self.prune_steps(up_to, &mut removed).await;
            // Counted even when a step failed: those blocks are gone.
            metrics::blocks_pruned(removed);
            result
        })
        .await
    }

    async fn head(&self) -> Result<Option<BlockRef>, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Head, async {
            let fields = self.hash_fields("head", self.keys.head()).await?;
            if fields.is_empty() {
                return Ok(None);
            }
            codec::block_ref(&fields, "number", "hash").map(Some)
        })
        .await
    }

    async fn block(&self, hash: BlockHash) -> Result<Option<DecodedBlock>, StorageError> {
        metrics::timed(
            Store::Unsafe,
            Operation::Block,
            self.stored_block("block", hash),
        )
        .await
    }

    async fn canonical(&self, number: BlockNumber) -> Result<Option<DecodedBlock>, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Block, async {
            let mut connection = self.connection.clone();
            // The canonical chain is a sorted set scored by number: at most one hash per height.
            let hashes: Vec<String> = request(
                "canonical",
                redis::cmd("ZRANGEBYSCORE")
                    .arg(self.keys.canonical())
                    .arg(number)
                    .arg(number)
                    .query_async(&mut connection),
            )
            .await?;
            let Some(hash) = hashes.first() else {
                return Ok(None);
            };
            self.stored_block("canonical", codec::parse_hash(hash)?)
                .await
        })
        .await
    }

    /// A `None` head is unknown, not absent: its stored key is left as it is.
    async fn set_l1_heads(&self, heads: L1Heads) -> Result<(), StorageError> {
        metrics::timed(Store::Unsafe, Operation::SetL1Heads, async {
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            for (key, head) in [
                (self.keys.safe_head(), heads.safe),
                (self.keys.finalized_head(), heads.finalized),
            ] {
                if let Some(head) = head {
                    pipeline
                        .cmd("HSET")
                        .arg(key)
                        .arg("number")
                        .arg(head.number)
                        .arg("hash")
                        .arg(head.hash.to_string());
                }
            }
            let mut connection = self.connection.clone();
            request("set_l1_heads", pipeline.query_async(&mut connection)).await
        })
        .await
    }

    async fn last_event_id(&self) -> Result<EventId, StorageError> {
        metrics::timed(Store::Unsafe, Operation::LastEventId, async {
            let mut connection = self.connection.clone();
            let newest: StreamEntries = request(
                "last_event_id",
                redis::cmd("XREVRANGE")
                    .arg(self.keys.events())
                    .arg("+")
                    .arg("-")
                    .arg("COUNT")
                    .arg(1)
                    .query_async(&mut connection),
            )
            .await?;
            newest
                .first()
                .map_or(Ok(EventId::START), |(id, _)| parse_event_id(id))
        })
        .await
    }

    async fn events(
        &self,
        after: EventId,
        count: usize,
        block_for: Duration,
    ) -> Result<Events, StorageError> {
        metrics::timed(Store::Unsafe, Operation::Events, async {
            const READ: &str = "events";
            let key = self.keys.events();
            let mut connection = self.events.clone();
            let mut command = redis::cmd("XREAD");
            command.arg("COUNT").arg(count.max(1));
            if !block_for.is_zero() {
                let millis = u64::try_from(block_for.as_millis()).unwrap_or(u64::MAX);
                command.arg("BLOCK").arg(millis.max(1));
            }
            command.arg("STREAMS").arg(&key).arg(after.to_string());
            // A nil reply (nothing came within the wait) is `None`.
            let reply: Option<Vec<(String, StreamEntries)>> = match timeout(
                block_for.saturating_add(REQUEST_TIMEOUT),
                command.query_async(&mut connection),
            )
            .await
            {
                Ok(result) => result.map_err(|source| StorageError::Redis {
                    operation: READ,
                    source,
                })?,
                Err(_elapsed) => {
                    return Err(StorageError::Timeout {
                        store: Store::Unsafe,
                        operation: READ,
                    });
                }
            };
            let events = reply
                .into_iter()
                .flatten()
                .flat_map(|(_stream, entries)| entries)
                .map(|(id, fields)| Ok((parse_event_id(&id)?, codec::decode_event(&fields)?)))
                .collect::<Result<Vec<_>, StorageError>>()?;
            // Checked after the read, so a trim before it shows: entries after `after` were
            // removed exactly when the oldest one left is above it.
            let missed = if after == EventId::START {
                false
            } else {
                let oldest: StreamEntries = request(
                    READ,
                    redis::cmd("XRANGE")
                        .arg(&key)
                        .arg("-")
                        .arg("+")
                        .arg("COUNT")
                        .arg(1)
                        .query_async(&mut connection),
                )
                .await?;
                match oldest.first() {
                    Some((id, _)) => after < parse_event_id(id)?,
                    None => false,
                }
            };
            Ok(Events { events, missed })
        })
        .await
    }
}

/// The overall limit of an operation that makes several requests.
struct Deadline {
    operation: &'static str,
    at: Instant,
}

impl Deadline {
    fn start(operation: &'static str) -> Self {
        Self {
            operation,
            at: Instant::now() + OPERATION_DEADLINE,
        }
    }

    /// Fails with [`StorageError::Timeout`] once the deadline has passed.
    fn check(&self) -> Result<(), StorageError> {
        if Instant::now() < self.at {
            return Ok(());
        }
        Err(StorageError::Timeout {
            store: Store::Unsafe,
            operation: self.operation,
        })
    }
}

/// Builds a script: a line of constants, the shared prelude, then the script's own body. The
/// constants are written into the source so the scripts' loop bounds are fixed, not arguments.
fn script(body: &str) -> Script {
    let constants = format!(
        "local EVENTS_MAXLEN, UNSAFE_TTL_SECS, MAX_REORG_DEPTH = {EVENTS_MAXLEN}, {}, {MAX_REORG_DEPTH}\n\
         local RETENTION_HEIGHTS_PER_INSERT = {RETENTION_HEIGHTS_PER_INSERT}\n\
         local PRUNE_HEIGHTS_PER_CALL, REMOVE_BLOCKS_PER_STEP = \
         {PRUNE_HEIGHTS_PER_CALL}, {REMOVE_BLOCKS_PER_STEP}\n",
        UNSAFE_TTL.as_secs(),
    );
    Script::new(&[&constants, include_str!("../scripts/lib.lua"), body].concat())
}

/// A stream entry id as Redis writes it, `millis-seq`.
fn parse_event_id(id: &str) -> Result<EventId, StorageError> {
    id.parse().map_err(|err| StorageError::InvalidData {
        store: Store::Unsafe,
        what: "event id",
        block: None,
        source: Some(crate::ParseError::from(err)),
    })
}

/// A script replied with a status this binary does not know.
const fn invalid_reply(block: BlockHash) -> StorageError {
    StorageError::InvalidData {
        store: Store::Unsafe,
        what: "script reply",
        block: Some(block),
        source: None,
    }
}

/// Awaits one Redis request, bounded by [`REQUEST_TIMEOUT`].
async fn request<T>(
    operation: &'static str,
    call: impl Future<Output = RedisResult<T>>,
) -> Result<T, StorageError> {
    match timeout(REQUEST_TIMEOUT, call).await {
        Ok(result) => result.map_err(|source| StorageError::Redis { operation, source }),
        Err(_elapsed) => Err(StorageError::Timeout {
            store: Store::Unsafe,
            operation,
        }),
    }
}
