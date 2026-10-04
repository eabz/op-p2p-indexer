# Storage specification

Scope: the `storage` crate only, plus the shared types it needs in `primitives`. See
[roadmap.md](roadmap.md) for why storage is built first and what feeds it later.

| Store | Holds | Why |
|---|---|---|
| **Redis** (unsafe store) | Unsafe blocks: live, not yet committed to L1. Decoded, readable by other services. | Small, changes shape on reorgs. |
| **ClickHouse** (committed store) | Blocks committed to L1, and historical backfill. | Large, append-mostly, analytical queries. |
| **fjall** (archive) | A window of committed blocks, or optionally all of them, in their consensus encoding. | Serving other nodes by number or hash without touching ClickHouse (section 9). |

**Input.** Storage takes a block that is already decoded, with or without its receipts. (The
type is named `DecodedBlock` from an earlier plan; receipts are now expected to come from
execution-network peers, see the roadmap. "Executed" below means "has receipts".) It does not
decode gossip payloads, execute transactions or know about L1. Whoever calls it supplies:

- the block (header and transactions), and its receipts if they are known;
- the L1 safe and finalized heads, when known.

**Out of scope here:** decoding gossip payloads, execution, the promotion loop that moves blocks
from Redis to ClickHouse (that is `pipeline`). Storage provides the operations promotion needs.

## 1. Shared types (`crates/primitives`)

Use alloy and op-alloy types; do not redefine blocks, transactions or receipts.

| Type | Meaning |
|---|---|
| `DecodedBlock` | `block: op_alloy_consensus::OpBlock`, `hash: BlockHash`, `senders: Vec<Address>` (one per transaction, recovered by the caller), `receipts: Option<Vec<OpReceiptEnvelope>>` (`None` until known; when present, one per transaction), `source: BlockSource`. |
| `BlockSource { Gossip, L1 }` | Where the block came from. |
| `BlockRef { number: BlockNumber, hash: BlockHash }` | A block identified by height and hash. Used for heads. |
| `Reorg { common_ancestor: Option<BlockRef>, old_head: BlockRef, new_head: BlockRef, replaced: Vec<BlockHash> }` | Canonical entries were replaced or removed. `replaced` is newest first. `common_ancestor` is `None` when it is not known (the replaced range ends in a gap). `old_head == new_head` when only entries below the head changed. |
| `UnsafeEvent { NewHead { head: BlockRef, gap: bool }, Reorg(Reorg), Filled(BlockRef), Receipts(BlockRef), Pruned { up_to: BlockRef } }` | What an unsafe-store write did. Also published to readers (section 3.3). |
| `L1Heads { safe: Option<BlockRef>, finalized: Option<BlockRef> }` | `None` until an L1 source exists. |
| `ArchivedBlock { header: Bytes, body: Bytes, receipts: Option<Bytes> }` | An archived block as RLP, ready for the wire (section 9.2). |
| `InsertOutcome { stored: bool, events: Vec<UnsafeEvent> }` | Result of an unsafe-store insert. `stored` is `false` for a block that was already stored or is at or below the safe head; `events` is then empty. |

`UnsafeBlock` (a gossiped block, decoded and hash-checked by `p2p`, senders not yet recovered)
and `PayloadVersion` are not storage's concern; turning an `UnsafeBlock` into a `DecodedBlock`
is the pipeline's job.

## 2. Store traits and errors (`crates/storage`)

```rust
pub trait UnsafeStore {
    /// Stores a block and applies fork choice (section 3.2), atomically.
    async fn insert(&self, block: &DecodedBlock) -> Result<InsertOutcome, StorageError>;
    /// Attaches receipts to a stored block. Ok(false) if the block is no longer stored.
    async fn set_receipts(&self, block: BlockRef, receipts: &[OpReceiptEnvelope]) -> Result<bool, StorageError>;
    /// The ancestry of `head` back to (excluding) height `stop_at`, oldest first, by parent links.
    async fn ancestry(&self, head: BlockRef, stop_at: BlockNumber) -> Result<Vec<DecodedBlock>, StorageError>;
    /// Removes every block at or below `up_to`, canonical and side blocks.
    async fn prune(&self, up_to: BlockRef) -> Result<(), StorageError>;
    async fn head(&self) -> Result<Option<BlockRef>, StorageError>;
    async fn block(&self, hash: BlockHash) -> Result<Option<DecodedBlock>, StorageError>;
    async fn set_l1_heads(&self, heads: L1Heads) -> Result<(), StorageError>;
}

pub trait CommittedStore {
    /// Inserts blocks with their transactions, receipts and logs. Idempotent.
    async fn insert(&self, blocks: &[DecodedBlock]) -> Result<(), StorageError>;
    /// Deletes everything above `safe` (an L1 reorg moved the safe head back).
    async fn rollback_to(&self, safe: BlockRef) -> Result<(), StorageError>;
    async fn l1_heads(&self) -> Result<L1Heads, StorageError>;
    async fn set_l1_heads(&self, heads: L1Heads) -> Result<(), StorageError>;
}
```

- `RedisStore` and `ClickHouseStore` are the implementations. Both are cheap to clone.
- `StorageError` is one `thiserror` enum with `severity(&self) -> Severity`:
  - **Transient** (retry can help): connection lost, timeout, a local disk I/O failure in the
    archive, and a busy server. Busy is
    recognised by the server's error code: Redis `BUSY`, `LOADING`, `READONLY`, `TRYAGAIN`,
    `CLUSTERDOWN`, `MASTERDOWN`; ClickHouse `Code: 159` (timeout exceeded), `202` (too many
    simultaneous queries), `241` (memory limit), `252` (too many parts).
  - **Expected** (the caller handles it, nothing is wrong with the store): `MissingAncestor`,
    `AncestryTooLong`, and the archive's `NotContiguous`.
  - **Fatal** (needs an operator): everything else, including schema or checksum mismatch, bad
    credentials, undecodable stored data, and a block that does not fit the schema.
  Variants carry what failed: the operation for driver errors, the block for decode errors.
  Storage does not retry; the caller does.
- **Retries and lost replies.** A write that times out may still have been applied. Every write
  is idempotent, so retrying is safe, but a retried `insert` returns `stored = false` and no
  events: the events of the first attempt are on the stream only. A caller that needs them
  re-reads `head()`.
- **Cancel safety.** Dropping a future never corrupts a store. A multi-step operation that is
  dropped part-way (`prune`, `ancestry`, `rollback_to`, `migrate`, a batched `insert`) is
  finished by calling it again.
- Module layout: `storage::unsafe_store` (Redis; keys and every limit of that store in its `layout`
  module), `storage::committed_store` (ClickHouse: client,
  `rows`, `migrations`), `storage::archive_store` (fjall), `storage::metrics`; the traits, the
  configuration types, `StorageError`, `Severity`, `InvalidBlockReason` and `Store` are exported
  from the crate root.

## 3. Redis (unsafe store)

### 3.1 Key layout

All keys are prefixed `opidx:{chain_id}:` (shown as `…:`). Hashes are lowercase `0x` hex.
Values are JSON in Ethereum JSON-RPC field naming (alloy's `serde` output), so readers can use
any Ethereum library to parse them. Block-scoped keys get `UNSAFE_TTL` (24 hours) as a backstop
for when nothing prunes them. The sorted sets do not expire, so `insert` also enforces
**retention**: heights more than `UNSAFE_RETENTION_BLOCKS` (43200, 24 hours of 2-second blocks)
below the head are removed from `heights` and `canonical` together with their block keys, a
bounded number per call, without an event. Readers must not expect blocks older than that.

| Key | Type | Content |
|---|---|---|
| `…:schema_version` | string | Key-layout version (section 5.2). |
| `…:block:{hash}` | hash | `number`, `parent_hash`, `timestamp` (plain strings, used by fork choice), `header` (JSON), `transactions` (JSON array, each with its `from`), `tx_count`, `receipts` (JSON array, absent until set), `source`, `received_at_ms`. |
| `…:height:{number}` | set | Hashes of every block seen at this height, canonical and side blocks. |
| `…:heights` | sorted set | score = number, member = number. Every height that holds at least one stored block; lets prune and retention find blocks without scanning. |
| `…:canonical` | sorted set | score = number, member = hash. The canonical unsafe chain. A missing score is a gap. |
| `…:head` | hash | `number`, `hash`, `timestamp` of the unsafe head. |
| `…:safe_head`, `…:finalized_head` | hash | `number`, `hash`. Absent until known. |
| `…:events` | stream | Section 3.3. Capped with `MAXLEN ~ 10000`. |

### 3.2 Fork choice and reorgs

Blocks reaching the unsafe store are assumed valid (signature and hash checked upstream). The
unsafe store decides which are canonical. The whole decision for one block runs in one Lua script, so
readers never see a half-applied reorg.

The rule follows what an op-node follower does with gossiped unsafe payloads (verified against
optimism `develop` @ c8e4ba855d79, `op-node/rollup/engine/payloads_queue.go` and
`engine_controller.go`): the unsafe head only ever moves to a **strictly higher** number. A block
at or below the head never replaces it, and neither does a block at `head + 1` on a different
parent. A block two or more heights above the head becomes the head whatever chain it is on.
There is no "latest wins" and no chain-length comparison.

For a new block `B` (number `n`, hash `h`, parent `p`), with `H` the current head:

1. `h` already stored: no-op, no events.
2. `n` at or below the safe head: ignored. Live input never changes what L1 has committed.
3. Store `…:block:{h}` and add `h` to `…:height:{n}`.
4. **Bootstrap.** No head yet: `B` becomes the head; emit `NewHead`.
5. **Extend.** `p` is `H.hash` and `n = H.number + 1`: add `(n, h)` to `canonical`, move the
   head, emit `NewHead`.
6. **Jump.** `n >= H.number + 2`: `B` becomes the head. Walk parent links back from `B`
   through stored blocks, at most `MAX_REORG_DEPTH` (256) steps, collecting the path.
   - The walk meets the canonical chain at an ancestor `A` (or reaches the safe height and the
     block there is the safe head, by hash, even if it has been pruned): remove canonical
     entries above `A.number`, add the path, emit `Reorg` if any entry was actually replaced,
     then `NewHead`.
   - The walk ends at a block whose parent is not stored, or at its depth limit: the path it
     collected is still canonical truth (it is `B`'s own ancestry), so add it, remove every
     canonical entry at or above the path's lowest height that is not on the path, and remove
     the entry just below the path if it is above the safe head and is not the lowest block's
     parent. If that entry is at or below the safe head and is not the parent, the lowest
     path block contradicts what L1 committed: leave it out, so its height stays a gap. Emit
     `Reorg` (with no ancestor) if anything was replaced, then `NewHead` with `gap = true`.
   - `p` itself is not stored: add only `(n, h)`; emit `NewHead` with `gap = true`.
7. **Fill.** `n <= H.number`, height `n` has no canonical entry, and the canonical block at
   `n + 1` has parent `h`: add `(n, h)` to `canonical`, head unchanged, emit `Filled`. Then
   repeat downward: while the height below has no canonical entry and the parent of the block
   just filled is stored, fill it too (bounded by `MAX_REORG_DEPTH`). This repairs a gap when
   blocks arrive out of order, in either order.
   A fill can reveal that the entry below belongs to another branch (a gap jump landed on a
   fork we had not seen): if the canonical entry at `n - 1` exists and is not the filled
   block's parent, replace it with the parent if the parent is stored (and keep checking
   downward), otherwise remove it, leaving a gap. Either way emit `Reorg` with the replaced
   hashes; `common_ancestor` is `None` when the walk ended in a gap. Wherever the walk stops,
   the entry directly below the last block it wrote is either that block's parent or absent.
8. **Side block.** Anything else (at or below the head, or at `H.number + 1` on a different
   parent): `B` is kept as a side block only, with no events.

Unlike op-node, the unsafe store does not queue and reorder: a block two heights ahead is applied
at once and the gap is repaired by step 7 when the missing block arrives. Keep fork choice in
one function of the script.

Invariant after every write: wherever `canonical` has entries at two consecutive heights, the
upper block's parent is the lower block. A height with no entry is a gap, never a guess.

Limits and consequences:

- **Block numbers must be consistent.** A block whose parent is stored with a number other
  than `n - 1` is rejected (`InvalidBlock`) and not stored. Heights in the walks are checked
  against the stored numbers, never only computed.
- **The safe head bounds every rewrite.** Steps 6 and 7 never write, replace or remove a
  canonical entry at or below the safe head.
- **Nothing canonical may contradict the safe head.** A block directly above the safe height
  is made canonical only if its parent is the safe head, by hash. That holds whether the safe
  block is still in `canonical` or has been pruned, and if `canonical` holds a *different*
  block at the safe height (the safe head moved and prune has not run yet) the safe hash is
  not accepted as an ancestor there. In both cases the block above stays a side block and its
  height a gap.
- **Setting the safe head does not reconcile the store. Known gap, to close with the L1 work.**
  `set_l1_heads` only records the heads. If the safe head lands on a block other than the one
  `canonical` holds at that height (L1 derived a different chain), the store keeps its old
  branch until the caller prunes, and the `head` key keeps pointing at that branch until a block
  two or more heights above it arrives. Until then a block extending the stale head is still
  accepted, and blocks of the right branch stay side blocks, which can leave a gap just above
  the safe height that nothing fills later. An op-node follower resets its unsafe head in this
  situation. The fix is for `set_l1_heads` to become a script that, when the safe block
  contradicts `canonical`, removes the contradicted entries, moves the head to the safe block
  and emits a `reorg`. Nothing sets the safe head until the L1 crate exists, so this is not
  reachable today.
- **When the unsafe head is below the safe head**, a block directly above the safe height
  becomes the head only if its parent is the safe head; `gap` is then `0`, because the heights
  in between are committed, not missing.
- **A reorg can be long.** One `reorg` event lists up to `MAX_REORG_DEPTH` replaced hashes.
- **Every height a fill writes emits `Filled`**, whether the height was empty or held another
  branch's block, so a reader learns the new canonical hash at each height it changed.
- **The head can outlive its block.** Prune or retention may remove the block the `head` key
  points at; the key stays, because fork choice only compares hashes against it.
- **Retention is measured from the head**, so a block with a far-future number would start
  trimming real blocks. The guard is upstream: only sequencer-signed blocks reach the store.
- **Single Redis instance.** The scripts build block and height keys from a prefix, so they are
  not valid on Redis Cluster.

`set_receipts` writes the `receipts` field only if the block is still stored, and emits
`Receipts`. The receipts must be one per transaction, pass the same checks as receipts that
arrive with a block, and the number must match the stored block, otherwise `InvalidBlock`. `insert` checks the same for a block that arrives with receipts.

`prune(up_to)` removes, for every height in `heights` at or below `up_to.number`, its
`…:block:` keys (side blocks included), its `…:height:` set and its `canonical` entry, in
bounded batches until none are left, then emits `Pruned`. It is exact and resumable. Call
`set_l1_heads` with the new safe head **before** pruning up to it: otherwise a block gossiped
again at a pruned height would be stored and could be filled back in.

`set_l1_heads`: a `None` head means "unknown" and leaves the stored key untouched, as in the
committed store. Deleting `safe_head` would silently switch off step 2.

`ancestry(head, stop_at)` returns the complete range or an error, never a partial one:
`MissingAncestor` if a parent on the way down to `stop_at + 1` is not stored (pruned, expired,
or never received), and `AncestryTooLong` if the range exceeds `MAX_ANCESTRY_BLOCKS`, checked
before any read. Callers ask for bounded ranges.

Only the header and transactions are stored. `insert` rejects a block with ommers or
non-empty withdrawals (`InvalidBlock`), so nothing is dropped silently. One shared validation (`validate_block`) runs first in every store, so all three accept
exactly the same blocks: a transaction type the schema has no columns for is
`UnsupportedTransaction`, and a block the committed store could never accept is not stored
unsafe either. Block numbers above 2^53 are
rejected before a script is called, because Lua numbers are doubles.

Operations that make several calls (`ancestry`, `prune`, the schema wipe) have an overall
deadline as well as the per-request timeout.

### 3.3 Events for live readers

`…:events` is a Redis Stream; readers follow it with `XREAD` and fetch `…:block:{hash}`.

| `type` | Fields |
|---|---|
| `head` | `number`, `hash`, `parent_hash`, `timestamp`, `gap` (`0` / `1`) |
| `reorg` | `ancestor_number`, `ancestor_hash` (both absent when the ancestor is not known), `old_head_number`, `old_head_hash`, `new_head_number`, `new_head_hash`, `replaced` (comma-separated hashes, newest first) |
| `fill` | `number`, `hash`: a block below the head became canonical (a gap was repaired) |
| `receipts` | `number`, `hash` |
| `pruned` | `number`, `hash`: everything at or below this is gone from Redis |

A `reorg` event that moves the head is always followed by the `head` event for the new head, from the same script.

## 4. ClickHouse (committed store)

Database from configuration (default `op_indexer`). Conventions: hashes `FixedString(32)`,
addresses `FixedString(20)`, wei amounts `UInt256`, gas prices `UInt128`, timestamps `DateTime('UTC')`, every table
starts with `chain_id UInt64`. Block-data tables are `ReplacingMergeTree(version)`, `version` =
insert time in microseconds as `UInt64`: inserting the same block twice is harmless and the newest row
wins. Read with `FINAL`. Partition by `toYYYYMM` of the block timestamp.

No per-row status column: a block is finalized if `number <= finalized head`, else safe.

Exceptions to the conventions: `schema_migrations` has no `chain_id` (it describes the schema,
not a chain) and is created before the numbered migrations; `blocks.base_fee_per_gas` is
`Nullable(UInt64)`, the width alloy's header uses. The database itself must already exist.

**Codecs.** Every column has an explicit codec, chosen by the kind of data. These are the
standard choices and have not been measured on real data; revisit them with
`system.columns` compressed sizes once blocks are flowing.

| Kind of column | Columns | Codec |
|---|---|---|
| Steadily increasing | `number`, `block_number`, `timestamp`, `block_timestamp`, `version`, `updated_at` | `DoubleDelta, ZSTD(1)` for block numbers and timestamps; `Delta, ZSTD(1)` for `version` |
| Small or slowly changing integers | `tx_index`, `log_index`, `tx_count`, `logs_count`, `gas_limit`, `gas_used`, `base_fee_per_gas`, `nonce`, `cumulative_gas_used`, gas prices, `blob_gas_used`, `excess_blob_gas`, `tx_type`, `status` | `T64, ZSTD(1)`. ClickHouse 25.8 rejects `T64` on `UInt128`, so the three gas-price columns use `ZSTD(1)`; `T64` under `Nullable(UInt64)` is accepted. |
| Random 32-byte hashes | `hash`, `parent_hash`, `block_hash`, `tx_hash`, the header roots, `prev_randao`, `source_hash`, `topic1` to `topic3` | `NONE`: they do not compress |
| Repeating addresses and signatures | `fee_recipient`, `from`, `to`, `address`, `topic0` | `ZSTD(1)` |
| Sparse or mostly zero bytes | `value`, `mint`, `logs_bloom`, `extra_data` | `ZSTD(1)` |
| Large byte strings | `input`, `raw`, `data` | `ZSTD(3)` |
| Constant or tiny | `chain_id`, `source`, `has_receipts`, `is_system_tx`, `key` | `ZSTD(1)` |

If ClickHouse rejects a codec for a column type, use `ZSTD(1)` for that column and say so in
the migration file.

**Inserts and merges.** Also standard choices, to be confirmed on the compose ClickHouse and
revisited with real load:

- **Batch on our side first.** Each insert creates a part per table, so the caller writes many
  blocks per call, not one call per block. This is the main protection against too many parts.
- **Async inserts as a safety net.** Every insert request sets `async_insert = 1` and
  `wait_for_async_insert = 1`: the server groups small inserts that arrive close together into
  one part, and still acknowledges only after the data is written, so nothing is lost if the
  indexer or the server stops.
- `fee_recipient` is `LowCardinality(FixedString(20))`: a chain has one or a few fee vaults.
- **Old partitions merge down to one part.** The four block-data tables set
  `min_age_to_force_merge_seconds = 86400` and `min_age_to_force_merge_on_partition_only = 1`.
  A month that no longer receives inserts is merged into a single part, which also completes
  the `ReplacingMergeTree` deduplication there and makes `FINAL` on old data cheap.
- **`FINAL` stays inside a partition.** A block's rows only ever live in one monthly partition,
  so queries that use `FINAL` on the block-data tables set
  `do_not_merge_across_partitions_select_final = 1`. This crate has no such query yet (its only
  `FINAL` is on `chain_state`, which is not partitioned); the `query` crate applies it.
- **Not done here:** a fast path for "transaction by hash" (a projection or a lookup table
  ordered by hash). The bloom-filter index is enough for correctness; the `query` crate decides
  the fast path when it defines its queries.

| Table | Order key | Columns |
|---|---|---|
| `blocks` | `(chain_id, number)` | `hash`, `parent_hash`, `timestamp`, `fee_recipient`, `state_root`, `transactions_root`, `receipts_root`, `logs_bloom`, `prev_randao`, `gas_limit`, `gas_used`, `base_fee_per_gas` (nullable), `extra_data`, `tx_count`, `withdrawals_root` (nullable), `blob_gas_used` (nullable), `excess_blob_gas` (nullable), `parent_beacon_block_root` (nullable), `requests_hash` (nullable), `source` (`Enum8` gossip / l1), `has_receipts` (`Bool`), `version` |
| `transactions` | `(chain_id, block_number, tx_index)` | `block_hash`, `block_timestamp`, `hash`, `tx_type`, `from`, `to` (nullable), `nonce` (nullable: deposits have none), `value`, `gas_limit`, `gas_price` (nullable: legacy and EIP-2930), `max_fee_per_gas` and `max_priority_fee_per_gas` (nullable: EIP-1559 and EIP-7702), `input` (`CODEC(ZSTD(3))`), deposit fields `source_hash`, `mint`, `is_system_tx` (all nullable, set only for deposits), `raw` (EIP-2718 bytes, `CODEC(ZSTD(3))`), `version`. Bloom-filter index on `hash`. |
| `receipts` | `(chain_id, block_number, tx_index)` | `block_hash`, `block_timestamp`, `tx_hash`, `status`, `cumulative_gas_used`, `logs_count`, deposit fields `deposit_nonce`, `deposit_receipt_version` (both nullable, set only for deposits), `version` |
| `logs` | `(chain_id, block_number, log_index)` | `log_index` is the running index across the block's receipts. `block_hash`, `block_timestamp`, `tx_index`, `tx_hash`, `address`, `topic0` to `topic3` (nullable), `data` (`CODEC(ZSTD(3))`), `version`. Bloom-filter indexes on `address` and `topic0`. |
| `chain_state` | `(chain_id, key)` | `key` (`Enum8` safe_head / finalized_head), `number`, `hash`, `updated_at` (`UInt64` microseconds). `ReplacingMergeTree(updated_at)`. |
| `schema_migrations` | `version` | `version` (`UInt32`), `name`, `checksum` (`FixedString(32)`, raw SHA-256), `applied_at`. Plain `MergeTree`. |

Only what is in the block and its consensus receipts is stored. Fields that need more than
that (gas used per transaction, effective gas price, created contract address, OP L1 fee) are
left for the crate that supplies them to define (see the roadmap); add them with a migration
then.

`insert` writes `transactions` for every block, `receipts` and `logs` for blocks that have
receipts, and `blocks` last, so a `blocks` row means its child rows are stored. Large inputs
are written in chunks of at most `MAX_INSERT_BLOCKS` blocks.

Rows are deduplicated by position (`block_number`, `tx_index` / `log_index`), not by block
hash. Replacing the block at a height with a different one therefore requires `rollback_to`
below that height first; otherwise rows of the old block at higher indexes would remain.

`rollback_to(safe)` first writes the new safe head to `chain_state`, then runs a lightweight
delete of everything above it on the four block-data tables, `blocks` first (the block-number
column is `number` in `blocks` and `block_number` in the other three). Writing the head first
means an interrupted rollback never leaves the recorded safe head pointing at a deleted block;
call it again to finish. The finalized head is not touched. A rollback is rare (an L1 reorg),
so the cost of a delete is acceptable.

In `set_l1_heads`, a `None` head means "unknown": the stored row is left as it is.

## 5. Migrations

### 5.1 ClickHouse

- Until the first release the initial migrations may still be edited in place; a local
  database that already recorded them must be dropped and recreated. After a release, never.
- One statement per file: `crates/storage/migrations/clickhouse/NNNN_name.sql`, embedded with
  `include_str!` and listed in order in one Rust table. ClickHouse's HTTP interface runs one
  statement per request, so this avoids parsing SQL.
- `schema_migrations` records each applied version with the SHA-256 of its SQL.
- `ClickHouseStore::migrate()` runs before anything else uses the store: create
  `schema_migrations` if missing, read the applied versions, then
  - an applied version whose checksum differs from the embedded file is a **fatal error**
    (an applied migration was edited; add a new one instead);
  - an applied version the binary does not know is a fatal error (the binary is older than the schema);
  - pending versions are applied in order, each recorded after it succeeds.
- ClickHouse DDL is not transactional, so every migration must be safe to run twice
  (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`). A crash mid-migration is fixed by restarting.
- One indexer instance migrates at a time; there is no migration lock. Documented, not enforced.

### 5.2 Redis

`…:schema_version` holds the key-layout version the data was written with. On connect: absent
means write the current version; equal means continue; different means **delete every key under
the prefix and start empty**, with a warning. Unsafe blocks are disposable.

Lua scripts are embedded with `include_str!` from `crates/storage/scripts/` and run by hash.

## 6. Connectors and configuration

| Store | Crate | Notes |
|---|---|---|
| ClickHouse | `clickhouse` 0.15 (the official client, HTTP) | `Row` derive for the row types, batched inserts, LZ4 compression. |
| Redis | `redis` 1.x with `tokio-comp`, `connection-manager`, `script` | One multiplexed connection that reconnects on its own. |

Both keep `default-features = false` and get a justification comment in the root `Cargo.toml`.
TLS features are added only when a deployment needs them. Every network call has a timeout.

`storage::config` defines `StorageConfig { redis: RedisConfig, clickhouse: ClickHouseConfig,
archive: Option<ArchiveConfig>, chain_id }` as plain data, with `ArchiveConfig { path,
retention: ArchiveRetention }` and `ArchiveRetention { Blocks(u64), All }` (`None` disables the
archive). The binary fills it from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `OP_INDEXER_REDIS_URL` | `redis://127.0.0.1:6379` | Unsafe store. |
| `OP_INDEXER_CLICKHOUSE_URL` | `http://127.0.0.1:8123` | Committed store, HTTP interface. |
| `OP_INDEXER_CLICKHOUSE_DATABASE` | `op_indexer` | |
| `OP_INDEXER_CLICKHOUSE_USER` | `indexer` | |
| `OP_INDEXER_CLICKHOUSE_PASSWORD` | none | Never logged; `Debug` on the config redacts it. |

In this PR the binary connects to both stores at startup, pings them, runs the ClickHouse
migrations and the Redis schema check, and fails fast if either store is unreachable. It does
not write blocks yet; that is the pipeline.

`storage::metrics` follows `crates/p2p/src/metrics.rs` (the binary calls its `describe()` at
startup): operation counts and durations by store, operation and outcome (`ok`, `transient`,
`expected`, `fatal`), blocks inserted, reorgs and their depth, receipts attached, blocks pruned, rows
inserted per table, rollbacks.

## 7. Open points

- **Pre-Bedrock headers.** `blocks` has no `difficulty`, `nonce` or `ommers_hash`, so headers
  from before Bedrock could not be rebuilt from it. Add them with a migration if that history
  is ever backfilled.
- **Wall-clock versions.** Row versions come from the system clock; a clock stepping backwards
  could make an older row win.
- **Encoding runs on the calling task.** JSON and row encoding are not moved to a blocking
  thread; blocks are small. A caller writing large batches should do so from a blocking-friendly
  context.
- **Tests.** `CLAUDE.md` says no tests for now. Fork choice (3.2) is a state machine that live
  runs will rarely exercise. Until the rule changes, verify it by driving the script against the
  compose Redis with hand-made block sequences, and report the sequences and results.

## 8. Constants

| Constant | Value | Where |
|---|---|---|
| `UNSAFE_TTL` | 24 h | block and height keys |
| `UNSAFE_RETENTION_BLOCKS` | 43200 | retention horizon below the head |
| `RETENTION_HEIGHTS_PER_INSERT` | 16 | heights trimmed per insert |
| `MAX_REORG_DEPTH` | 256 | jump and fill walks |
| `MAX_ANCESTRY_BLOCKS` | 1024 | one `ancestry` call |
| `PRUNE_HEIGHTS_PER_CALL` | 1024 | one prune script call |
| `REMOVE_BLOCKS_PER_STEP` | 64 | blocks removed from one height per step |
| `EVENTS_MAXLEN` | 10000 | stream cap |
| Archive cache / journal cap / memtable | 64 MiB / 128 MiB / 16 MiB per keyspace | fjall archive |
| Archive background threads / delete batch | 2 / 1024 blocks | fjall archive |
| `REMOVE_DEADLINE` | 60 s | overall limit for one archive `trim` or `truncate_above`, checked between batches |
| `MAX_INSERT_BLOCKS` | 256 | blocks per chunk of a committed-store insert |
| Redis connect / request timeout | 5 s / 10 s | every request |
| `OPERATION_DEADLINE` | 60 s | overall limit for `ancestry`, `prune` and the schema wipe, checked between requests |
| ClickHouse query / insert timeout | 30 s / 120 s | also sent as `max_execution_time` |

## 9. Local block archive (fjall)

**Why.** The indexer will serve headers, bodies and receipts to execution-network peers (see
roadmap). Doing that from ClickHouse would cost several queries per block, with load driven by
strangers. The archive is a local, embedded copy of a recent window of committed blocks, kept
in the encoding peers ask for. Nothing reads or writes it yet outside this crate: promotion
(pipeline) will append to it and serving (`el`) will read it.

**Engine.** fjall (3.x): a log-structured store, pure Rust, published on crates.io, with no
native code and nothing our `cargo deny` rejects. It is a directory, `archive/` in the data
directory. Everything goes through the `ArchiveStore` trait, so the engine can be replaced.

Why not the others, all measured or evaluated for this crate:

- **redb** was built first and benchmarked against fjall with the same workload (360,000
  blocks, 20 GB, body sizes modelled on real blocks, a 40 KB placeholder for receipts, one
  durable atomic write per block; macOS, internal SSD):

  | Measure | redb 4.3 | fjall 3.1 |
  |---|---|---|
  | Disk for 20.2 GB of data | 34.4 GB (1.7x) | 20.6 GB (1.02x) |
  | Durable appends: empty / 10 GB / 20 GB | 171 / 143 / 133 per second | 212 / 207 / 192 per second |
  | Appends with relaxed durability | 3,200 to 3,700 per second | 6,300 to 6,600 per second |
  | Trim the oldest third | 36 s | 1.6 s |
  | Disk for a constant 240,000-block window (13.5 GB) | 21.8 GB after two offline compactions (611 s, 208 s) | 14.3 GB, by itself |
  | Read by number, first touch: p50 / p99 | 0.81 ms / 9.0 ms | 0.47 ms / 2.8 ms |
  | Read by number, warm: p50 / p99 | 23 us / 48 us | 76 us / 486 us |
  | Clean open | 5 to 10 ms | 54 to 157 ms |
  | Reopen after `kill -9` | not measured | 158 ms, range contiguous |

  redb is fine for a small window and wrong for a history of hundreds of GB. Limits of the
  comparison: 20 GB only, one fjall configuration, incompressible filler, memory use not
  recorded. fjall frees space lazily: a trim with no writes after it reclaimed nothing in 25
  seconds; with appends following, the size settles and stays flat.
- **NippyJar** (reth's static files): an unpublished internal crate needing a git dependency
  and two `cargo deny` exceptions; rows cannot be completed later, so receipts could only be
  written in strict block order.
- **RocksDB, MDBX**: not Rust.

**Invariant.** The archive holds one contiguous range of blocks, each the parent of the next.
That is what makes the range announceable.

### 9.1 Keyspaces

One fjall database with one keyspace per kind of data. Block-number keys are big-endian `u64`,
so key order is block order. Values are RLP, snappy-compressed (`snap`).

| Keyspace | Key | Value |
|---|---|---|
| `headers` | number | RLP of the header |
| `bodies` | number | RLP of the body (transactions in network encoding, ommers, withdrawals) |
| `receipts` | number | RLP list of the receipts in network encoding, with bloom (one `Receipts` entry up to eth/68; a caller serving eth/69 re-encodes without the bloom); absent until set |
| `numbers` | block hash (32 bytes) | number |
| `meta` | name | `schema_version` |

`bodies` and `receipts` use fjall's key-value separation, which keeps large values out of the
index tree; its own blob compression is off, because the values are already compressed.

Senders are not stored: a peer does not ask for them and they can be recovered.

### 9.2 Trait

```rust
pub trait ArchiveStore {
    /// Appends the next block. It must extend the held range: number = last + 1 and parent
    /// hash = last hash, or the archive is empty.
    async fn append(&self, block: &DecodedBlock) -> Result<(), StorageError>;
    /// Attaches receipts to an archived block. Ok(false) if it is not archived.
    async fn set_receipts(&self, block: BlockRef, receipts: &[OpReceiptEnvelope]) -> Result<bool, StorageError>;
    /// The encoded block at `number`, or None outside the held range.
    async fn block(&self, number: BlockNumber) -> Result<Option<ArchivedBlock>, StorageError>;
    /// The number of the archived block with this hash.
    async fn number_of(&self, hash: BlockHash) -> Result<Option<BlockNumber>, StorageError>;
    /// The first and last archived block, or None if empty.
    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError>;
    /// Removes every block above `number` (an L1 reorg moved the safe head back).
    async fn truncate_above(&self, number: BlockNumber) -> Result<(), StorageError>;
    /// Removes the oldest blocks so at most `retain` remain. Returns how many were removed.
    async fn trim(&self, retain: u64) -> Result<u64, StorageError>;
}
```

- `ArchivedBlock { header: Bytes, body: Bytes, receipts: Option<Bytes> }`, decompressed RLP,
  ready to be put on the wire by a caller that knows the protocol version.
- `append` runs the shared block validation, then checks that the RLP header it is about to
  store hashes to `block.hash` (`InvalidBlock`), so the archive never holds a block whose bytes
  do not match its hash. A block that does not extend the range is `NotContiguous { expected,
  got }` (severity Expected), where `expected` is the archive's tip and `got` is the parent the
  block claims, so a wrong parent at the right height is distinguishable from a gap. The caller
  decides whether to `truncate_above` or start over.
  Appending the block already at the tip (same number and hash) is a no-op.
- `set_receipts` requires one receipt per transaction and the stored number to match
  (`InvalidBlock`), as in the unsafe store. A block appended with receipts stores them at once.
- Every write is one fjall batch across the keyspaces it touches: one journal record, applied
  entirely or not at all, so a crash or a dropped future leaves the range contiguous. Every
  write is acknowledged only after the journal is synced to disk. `trim` and `truncate_above`
  remove keys in bounded batches, releasing the writer between batches so appends are not
  held up, and stop at an overall deadline with a timeout error; they are finished by calling
  them again. Dropping the future does not cancel a call already running on its blocking
  thread; the space comes back
  when fjall's background compaction runs.
- The held range comes from the first and last keys of `headers`, never from a count.
- Writers are serialized (a fjall batch has no conflict detection, so two appends must not
  read the same tip). The lock is granted in arrival order (tokio's mutex, taken on the
  blocking thread): with an unfair lock a removal re-took it for every batch and no append got
  in until the removal was done. A block numbered 0 is accepted only into an empty archive. A read takes one snapshot, so header, body and receipts, and both ends
  of the range, come from one point in time even during a trim.
- Space overhead is roughly constant, not a multiple of the window: a journal of at most
  128 MiB plus a few 64 MiB blob files that are dropped only once wholly stale. With a tiny
  window the archive therefore looks many times its live data (measured: 250 to 400 MB for
  37 MB live); at 20 GB it was 1.02x. The archive's disk use, stale blob bytes and running
  compactions are recorded as gauges (exported once the binary installs a recorder); the blocks
  removed by `trim` and `truncate_above` are counted, also when the call ends in a timeout.
- fjall is synchronous: the implementation (`FjallArchive`, cheap to clone) runs each call on
  a blocking thread, so the trait is async like the other two.
- On open: an archive written with another `schema_version` is emptied, with a warning. The
  archive can be rebuilt from the committed store.

### 9.3 Retention and configuration

Retention is by block count, or unlimited. With a window, the caller invokes `trim(window)`
after appending; with unlimited retention it never trims.

| Variable | Default | Meaning |
|---|---|---|
| `OP_INDEXER_ARCHIVE_RETENTION_BLOCKS` | `1296000` (30 days of 2-second blocks) | Blocks kept for serving. `all` keeps every block. `0` disables the archive: nothing is opened or written. |

The archive lives at `{OP_INDEXER_DATA_DIR}/archive/`. The binary opens it at startup when
enabled; nothing writes to it until the pipeline exists.

**Sizing.** Real blocks (264 consecutive OP Mainnet blocks from live gossip, 2026-10-03
22:32-22:42 UTC, a Saturday): compressed header plus body averages 17.8 KB per block (median
6.7 KB, p95 76.9 KB, max 83.7 KB), bimodal, about one block in six carrying roughly 94 KB of
poorly compressible data. That is about 23 GB of values for the 30-day window, **without
receipts** (gossip carries none, so they are unmeasured) and before the engine's overhead. The
whole history is plausibly 300 to 500 GB today and grows by roughly 200 GB a year; a full OP
Mainnet node, state included, is about 700 GB. One short weekend sample: repeat at a weekday
peak before relying on it. Unlimited retention only keeps what the indexer has seen; filling
in older history needs the execution-p2p crate.
