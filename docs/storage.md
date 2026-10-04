# Storage specification

Scope: the `storage` crate only, plus the shared types it needs in `primitives`. See
[roadmap.md](roadmap.md) for why storage is built first and what feeds it later.

| Store | Holds | Why |
|---|---|---|
| **Redis** (unsafe store) | Unsafe blocks: live, not yet committed to L1. Decoded, readable by other services. | Small, changes shape on reorgs. |
| **fjall** (archive, the committed store) | Every committed block (or a window of the newest) in its consensus encoding, with each transaction's sender, and the committed L1 heads. | Embedded, needs no service; serves peers and the stream by number or hash (section 9). |

There was a third store, ClickHouse, for committed blocks as rows; it was removed on 2026-10-04
when the node became a data source streamed over gRPC (see the roadmap). Sections 4 and 5.1
say so; the numbering is kept.

**Input.** Storage takes a block that is already decoded, with or without its receipts. (The
type is named `DecodedBlock` from an earlier plan; receipts are now expected to come from
execution-network peers, see the roadmap. "Executed" below means "has receipts".) It does not
decode gossip payloads, execute transactions or know about L1. Whoever calls it supplies:

- the block (header and transactions), and its receipts if they are known;
- the L1 safe and finalized heads, when known.

**Out of scope here:** decoding gossip payloads, execution, the promotion loop that moves blocks
from Redis to the archive (that is `pipeline`). Storage provides the operations promotion needs.

## 1. Shared types (`crates/primitives`)

Use alloy and op-alloy types; do not redefine blocks, transactions or receipts.

| Type | Meaning |
|---|---|
| `DecodedBlock` | `block: op_alloy_consensus::OpBlock`, `hash: BlockHash`, `senders: Vec<Address>` (one per transaction, recovered by the caller), `receipts: Option<Vec<OpReceiptEnvelope>>` (`None` until known; when present, one per transaction), `source: BlockSource`. |
| `BlockSource { Gossip, Sync }` | Where the block came from: gossip, or range sync from execution peers. (Imported blocks go straight to the archive and need none.) |
| `BlockRef { number: BlockNumber, hash: BlockHash }` | A block identified by height and hash. Used for heads. |
| `Reorg { common_ancestor: Option<BlockRef>, old_head: BlockRef, new_head: BlockRef, replaced: Vec<BlockHash> }` | Canonical entries were replaced or removed. `replaced` is newest first. `common_ancestor` is `None` when it is not known (the replaced range ends in a gap). `old_head == new_head` when only entries below the head changed. |
| `UnsafeEvent { NewHead { head: BlockRef, gap: bool }, Reorg(Reorg), Filled(BlockRef), Receipts(BlockRef), Pruned { up_to: BlockRef } }` | What an unsafe-store write did. Also published to readers (section 3.3). |
| `L1Heads { safe: Option<BlockRef>, finalized: Option<BlockRef> }` | `None` until the L1 side has published that head (no L1 configured, or no matching dispute game yet). |
| `EncodedBlock { hash, header: Bytes, body: Bytes, receipts: Option<Bytes> }` | A block in its consensus encoding. `From<&DecodedBlock>` encodes a gossip block. |
| `ArchivedBlock { encoded: EncodedBlock, senders: Vec<Address> }` | What the archive takes and gives back (section 9.2): the encoding and one sender per transaction. `From<&DecodedBlock>` for a promoted block. |
| `ReadLimits { items, bytes, lowest }` | Where an archive read ends even if more is held; blocks below `lowest` count as not held. |
| `ChainIdentity { chain_id, genesis_hash }` | The chain the archive (and the p2p node store) records and checks on open. |
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
    /// The canonical block at height `number`, or None if none is stored there.
    async fn canonical(&self, number: BlockNumber) -> Result<Option<DecodedBlock>, StorageError>;
    async fn set_l1_heads(&self, heads: L1Heads) -> Result<(), StorageError>;
}

```

The committed store is the archive's trait, `ArchiveStore` (section 9.2).

- `RedisStore` and `FjallArchive` are the implementations. Both are cheap to clone.
- `StorageError` is one `thiserror` enum with `severity(&self) -> Severity`:
  - **Transient** (retry can help): connection lost, timeout, a local disk I/O failure in the
    archive, and a busy server. Busy is
    recognised by the server's error code: Redis `BUSY`, `LOADING`, `READONLY`, `TRYAGAIN`,
    `CLUSTERDOWN`, `MASTERDOWN`.
  - **Expected** (the caller handles it, nothing is wrong with the store): `MissingAncestor`,
    `AncestryTooLong`, and the archive's `NotContiguous`.
  - **Fatal** (needs an operator): everything else, including a schema or chain mismatch, bad
    credentials, undecodable stored data, and a block that does not fit the schema.
  Variants carry what failed: the operation for driver errors, the block for decode errors.
  The stores do not retry: each call is one attempt with a timeout. The crate exports
  `retry(cancel, store, operation, budget, call)` and `RetryError`, which repeat a call while
  its error is transient, with exponential backoff and jitter, until `cancel` fires or the
  optional `budget` of time is spent; the pipeline and the importer use it.
- **Retries and lost replies.** A write that times out may still have been applied. Every write
  is idempotent, so retrying is safe, but a retried `insert` returns `stored = false` and no
  events: the events of the first attempt are on the stream only. A caller that needs them
  re-reads `head()`.
- **Cancel safety.** Dropping a future never corrupts a store. A multi-step operation that is
  dropped part-way (`prune`, `ancestry`, `trim`, `truncate_above`) is finished by calling it
  again.
- Module layout: `storage::unsafe_store` (Redis; keys and every limit of that store in its
  `layout` module), `storage::archive_store` (fjall), `storage::metrics`; the traits, the
  configuration types, `StorageError`, `Severity`, `InvalidBlockReason` and `Store` are exported
  from the crate root.

## 3. Redis (unsafe store)

### 3.1 Key layout

All keys are prefixed `opidx:{chain_id}:` (shown as `…:`). Hashes are lowercase `0x` hex.
Values are JSON in Ethereum JSON-RPC field naming (alloy's `serde` output), so readers can use
any Ethereum library to parse them. Block-scoped keys get `UNSAFE_TTL` (24 hours) as a backstop
for when nothing prunes them. The sorted sets do not expire, so `insert` also enforces
**retention**: the lowest heights whose set has expired are removed from `heights` and
`canonical`, a bounded number per call, without an event. Their block keys expired no later,
since every insert at a height renews its set. The horizon is `UNSAFE_TTL`, a time, so it is
the same on every chain whatever its block time. Readers must not expect blocks older than that.

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
  and emits a `reorg`. This is reachable when the L1 side publishes a safe head on a branch the
  unsafe store does not follow; the fix is not done, the gap is open.
- **When the unsafe head is below the safe head**, a block directly above the safe height
  becomes the head only if its parent is the safe head; `gap` is then `0`, because the heights
  in between are committed, not missing.
- **A reorg can be long.** One `reorg` event lists up to `MAX_REORG_DEPTH` replaced hashes.
- **Every height a fill writes emits `Filled`**, whether the height was empty or held another
  branch's block, so a reader learns the new canonical hash at each height it changed.
- **The head can outlive its block.** Prune or retention may remove the block the `head` key
  points at; the key stays, because fork choice only compares hashes against it.
- **Retention follows key expiry**, not the head: a height leaves the index once its set has
  expired, `UNSAFE_TTL` after the last block stored at it. A day of blocks is 43,200 heights
  on OP Mainnet and 86,400 on Unichain, so a Unichain store holds about twice the keys and
  memory of an OP Mainnet one.
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
archive's `set_heads`. Deleting `safe_head` would silently switch off step 2.

`ancestry(head, stop_at)` returns the complete range or an error, never a partial one:
`MissingAncestor` if a parent on the way down to `stop_at + 1` is not stored (pruned, expired,
or never received), and `AncestryTooLong` if the range exceeds `MAX_ANCESTRY_BLOCKS`, checked
before any read. Callers ask for bounded ranges.

Only the header and transactions are stored. `insert` rejects a block with ommers or
non-empty withdrawals (`InvalidBlock`), so nothing is dropped silently. The checks
(`validate_block`) run before any write: one sender (and, with receipts, one receipt) per
transaction, and a transaction type the JSON layout has no place for is
`UnsupportedTransaction`. Block numbers above 2^53 are rejected before a script is called,
because Lua numbers are doubles.

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

## 4. Committed store

The archive (section 9) is the committed store. The ClickHouse committed store that was here
(tables `blocks`, `transactions`, `receipts`, `logs`, `chain_state`, `imported_ranges`) was
removed with its client, migrations and configuration. A database an earlier build wrote is
not read; drop it when convenient.

## 5. Migrations

### 5.1 Archive

The archive's layout has a version in `meta` (`schema_version`, section 9.1). An archive of
another version is refused on open and left as it is; there is no migration in place. Version
2 added the `senders` keyspace and the heads in `meta`; an archive of version 1 is loaded again
from the importer's verified chunks (`op-indexer-import load`, no download needed) into a new
directory.

### 5.2 Redis

`…:schema_version` holds the key-layout version the data was written with. On connect: absent
means write the current version; equal means continue; different means **delete every key under
the prefix and start empty**, with a warning. Unsafe blocks are disposable.

Lua scripts are embedded with `include_str!` from `crates/storage/scripts/` and run by hash.

## 6. Connectors and configuration

| Store | Crate | Notes |
|---|---|---|
| Redis | `redis` 1.x with `tokio-comp`, `connection-manager`, `script` | One multiplexed connection that reconnects on its own. |
| Archive | `fjall` 3.x | Embedded; a directory. |

Both keep `default-features = false` and get a justification comment in the root `Cargo.toml`.
Redis has no TLS feature. Every network call has a timeout.

`storage::config` defines `StorageConfig { redis: RedisConfig, archive: ArchiveConfig, chain:
ChainIdentity }` as plain data, with `ArchiveConfig { path, retention: ArchiveRetention }` and
`ArchiveRetention { Blocks(u64), All }`. The archive cannot be disabled. The binary fills it
from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `OP_INDEXER_REDIS_URL` | `redis://127.0.0.1:6379` | Unsafe store. Never logged with its credentials. |
| `OP_INDEXER_ARCHIVE_RETENTION_BLOCKS` | `all` | Section 9.3. |

The binary opens the archive, then connects to Redis and checks its schema, and fails fast if
either fails. The pipeline then writes the blocks.

### Several instances on one host

Several indexers can run on one server: several chains (OP Mainnet and Unichain), or several
builds of one chain. Each needs:

- **Its own data directory** (`OP_INDEXER_DATA_DIR`). The archive and the node store record
  their chain and refuse another's, so two instances never share one.
- **Its own ports**: `OP_INDEXER_LISTEN_ADDR`, `OP_INDEXER_EL_LISTEN_ADDR`,
  `OP_INDEXER_L1_LISTEN_ADDR`, `OP_INDEXER_L1_BEACON_LISTEN_ADDR` and
  `OP_INDEXER_STREAM_LISTEN_ADDR`, plus the advertised addresses on a public host.
- **Its own Redis keys.** Keys are prefixed by chain id (`opidx:{chain_id}:`), so two chains
  can share a Redis database. Two instances of the same chain must use different databases:
  the database index is part of the URL, `redis://host:6379/1` (the `redis` crate selects it
  on every connection).
- **Its own L1 side**, if L1 is enabled: each instance runs its own beacon light client and
  its own L1 execution peers. Two instances do not share them; nothing on L1 is
  per-chain except the dispute game factory.

With docker compose, every host port comes from a variable with today's value as its default
(`OP_INDEXER_P2P_PORT`, `OP_INDEXER_EL_PORT`, `OP_INDEXER_L1_PORT`, `OP_INDEXER_L1_BEACON_PORT`,
`OP_INDEXER_STREAM_PORT`, and `OP_INDEXER_STREAM_HOST_BIND`). The listen addresses follow the
same variables, so a port is the same inside the container and on the host and the node
records advertise it. A project name gives a second instance its own container and data
volume. `--no-deps` keeps it on the first project's Redis, reached through the host's
published port:

```bash
docker compose up -d
docker compose -p unichain --env-file unichain.env.example up -d --no-deps indexer
```

`unichain.env.example` shifts every port by 100 and sets
`OP_INDEXER_REDIS_URL=redis://host.docker.internal:6379`. A second instance of the same chain
would set `…:6379/1` instead.

`storage::metrics` follows `crates/p2p/src/metrics.rs` (the binary calls its `describe()` at
startup): operation counts and durations by store, operation and outcome (`ok`, `transient`,
`expected`, `fatal`), blocks inserted, reorgs and their depth, receipts attached, blocks pruned,
blocks removed from the archive, and the archive's disk gauges.

## 7. Open points

- **Senders.** Promoted and synced blocks get senders the pipeline recovered; imported ones
  are recovered and checked by the importer's `load` (`docs/import.md`). Unproven: the zero
  address of a pre-Bedrock legacy transaction signed with all zeros, which has no signer.
- **Encoding runs on the calling task.** JSON encoding for Redis is not moved to a blocking
  thread; blocks are small.
- **Tests.** `CLAUDE.md` says no tests for now. Fork choice (3.2) is a state machine that live
  runs will rarely exercise. Until the rule changes, verify it by driving the script against the
  compose Redis with hand-made block sequences, and report the sequences and results.

## 8. Constants

| Constant | Value | Where |
|---|---|---|
| `UNSAFE_TTL` | 24 h | block and height keys |
| `RETENTION_HEIGHTS_PER_INSERT` | 16 | expired heights trimmed per insert |
| `MAX_REORG_DEPTH` | 256 | jump and fill walks |
| `MAX_ANCESTRY_BLOCKS` | 1024 | one `ancestry` call |
| `PRUNE_HEIGHTS_PER_CALL` | 1024 | one prune script call |
| `REMOVE_BLOCKS_PER_STEP` | 64 | blocks removed from one height per step |
| `EVENTS_MAXLEN` | 10000 | stream cap |
| Archive cache / journal cap / memtable | 64 MiB / 128 MiB / 16 MiB per keyspace | fjall archive |
| Archive background threads / delete batch | 2 / 1024 blocks | fjall archive |
| `REMOVE_DEADLINE` | 60 s | overall limit for one archive `trim` or `truncate_above`, checked between batches |
| Redis connect / request timeout | 5 s / 10 s | every request |
| `OPERATION_DEADLINE` | 60 s | overall limit for `ancestry`, `prune` and the schema wipe, checked between requests |

## 9. Local block archive (fjall)

**Why.** The archive is the committed store: every block committed to L1 (or a window of the
newest), kept in the encoding peers ask for, with each transaction's sender, and the committed
L1 heads. Promotion (pipeline) appends to it and records the heads, the importer and range
sync fill it, serving (`el`) and the stream (`stream`) read it.

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
| `senders` | number | the sender of each transaction, 20 bytes each, in block order, uncompressed |
| `meta` | name | `schema_version` (2); `chain`, the chain the archive holds: its id (8 bytes, big-endian), then its genesis hash (32 bytes); `safe_head` and `finalized_head`, the committed heads: number (8 bytes, big-endian) then hash, absent until promotion records one |

`bodies` and `receipts` use fjall's key-value separation, which keeps large values out of the
index tree; its own blob compression is off, because the values are already compressed.

Senders are stored because recovering them costs one signature recovery per transaction, which
a reader of the history would otherwise pay on every read. A peer does not ask for them.

### 9.2 Trait

```rust
pub trait ArchiveStore {
    /// Appends consecutive blocks, oldest first, in their original encoding, unchanged, with
    /// their senders.
    async fn append_batch(&self, blocks: Vec<ArchivedBlock>) -> Result<(), StorageError>;
    /// Attaches receipts to an archived block. Ok(false) if it is not archived.
    async fn set_receipts(&self, block: BlockRef, receipts: &[OpReceiptEnvelope]) -> Result<bool, StorageError>;
    /// A run of headers, bodies or receipts, read in one call on one snapshot, up to `limits`.
    async fn read(&self, read: BlockRead, limits: ReadLimits, convert: Option<ItemConvert>) -> Result<Vec<Bytes>, StorageError>;
    /// Whole blocks (header, body, receipts if set, senders) from `from` upwards, on one snapshot.
    async fn blocks(&self, from: BlockNumber, limits: ReadLimits) -> Result<Vec<ArchivedBlock>, StorageError>;
    /// The committed L1 heads; `set_heads` records them, a `None` head leaving the recorded one.
    async fn heads(&self) -> Result<L1Heads, StorageError>;
    async fn set_heads(&self, heads: L1Heads) -> Result<(), StorageError>;
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

- `append_batch` is the write path of the trait: `ArchivedBlock { encoded: EncodedBlock { hash,
  header, body, receipts }, senders }` (primitives). **The bytes are stored unchanged**, never encoded
  again from a decoded value, so what is served later is what was verified: a legacy
  transaction with an all-zero signature does not survive a decode and re-encode. Import and
  range sync hand over the bytes they verified; promotion encodes its gossip blocks once
  (`ArchivedBlock::from(&DecodedBlock)`), which gives their original bytes because their
  transactions are signed. Promotion and range sync pass the senders the pipeline recovered;
  the importer passes those of its verified chunks.
- The archive checks keccak(header) = `hash`, so it never holds a block whose bytes do not
  match its hash, reads the number and parent hash from that header, and checks that each
  block is the child of the one before and that the list extends the tip (or the archive is
  empty), and that there is one sender per transaction (the body is cut, not decoded:
  `InvalidBlock` with `SenderCount` otherwise). It does not decode bodies or receipts: the
  caller has verified the transactions root and receipts root over these bytes, and recovered
  the senders. Receipts are in the form
  `op_indexer_primitives::encode_receipts` gives.
- Blocks already held are skipped, so a resumed import or sync can resend any amount: the
  leading blocks when the tip is among the list, and the whole list when it ends at or below
  the tip. "Held" is checked on one block, the one at the tip's height or the list's last:
  it must be stored under its hash at its number; the archive is one chain, so the blocks
  before it are then held too. Otherwise the call is `NotContiguous { expected, got }`
  (severity Expected): `expected` is the archive's tip and `got` the parent the first block
  claims (a gap, or a wrong parent at the right height) or the block the archive holds another
  one in place of. A block numbered 0 is accepted only into an empty archive.
- The whole list is checked before the first write, then written in batches of at most 16 MiB
  of RLP (no limit on the number of blocks: a batch is one synced commit), one turn at the writer lock each; a failure leaves the earlier
  batches in place and `range` says where to resume.
- Outside the trait, `FjallArchive::bulk_append(Vec<PreparedBlock>)` is the importer's bulk
  write: blocks are checked and compressed off the writer (`PreparedBlock::new`, one per
  core) and written straight into new table and blob files, without the journal, several
  times faster than `append_batch` for long lists and as durable when it returns. The blocks
  must extend the tip; a crash during a call leaves the held range as it was, and the next
  open removes files a failed call left behind. Lists of hundreds of megabytes, not a few
  blocks.
- `read` answers one peer request in one blocking call on one snapshot: a run of headers
  (from a number or a hash, every `step`-th block, rising or falling; consecutive headers are
  one range scan), or the bodies or receipts of a list of hashes. The run ends at the first
  block not held, at the first block below `limits.lowest`, and at the limits (items, and
  bytes after the item that crosses them). With
  `convert`, each item is passed through it inside the same call (serving strips the receipts'
  blooms there); an item it refuses ends the run.
- `blocks(from, limits)` reads whole blocks from `from` upwards in one blocking call on one
  snapshot: header, body, receipts (`None` if not set) and senders, each decompressed. It ends
  at the first block not held, below `limits.lowest`, or at the limits (header, body and
  receipt bytes count). It is what the stream reads history with; a block by hash is
  `number_of`, then `blocks(number, one item)`.
- `heads()` / `set_heads(heads)`: the committed safe and finalized heads, in `meta`, written
  by promotion as its marker that the blocks up to the safe head are committed (one durable
  batch; a `None` head leaves the recorded one). Promotion never records a head the archive
  does not hold (`docs/pipeline.md` section 4, step 5), so a reader of the archive finds every
  block up to the recorded safe head.
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
- **Bulk path, importer only** (`FjallArchive::bulk_append(Vec<PreparedBlock>)`). Blocks are
  prepared off the writer (`PreparedBlock::new`: the header decoded for its number and
  parent, its keccak checked against the block's hash, the three values compressed as
  `append_batch` compresses them, into the buffers the ingestion takes, so the writer copies
  nothing; the senders checked one per transaction). Under the writer lock the list is checked to extend the
  tip block by block (the same parent and number rule as `append_batch`; held leading
  blocks are not skipped: the importer starts after the tip). Each keyspace then gets one
  fjall ingestion, on its own thread: entries in ascending key order (`numbers` sorted by
  hash) written straight into new table and blob files, without journal or memtable.
  `bodies`, `receipts`, `senders` and `numbers` are finished first; `headers` is written alongside and
  finished only once they all are. Finishing an ingestion syncs its files and then registers
  them in the keyspace's version atomically, so when the call returns the blocks are durable.
  A crash before `headers` is finished leaves the held range as it was. The other
  keyspaces may then hold blocks of the unfinished list above the tip: reads by number stop
  at the tip, but a read of bodies or receipts by hash, `number_of` and `set_receipts` find
  them, and `trim` / `truncate_above` (which walk `headers`) do not remove them. They are
  verified blocks of the chain the archive holds, and the next load writes them again with
  the same values and then their headers; the shadowed copies go with compaction and blob
  GC. A failed call is not retried by the importer: unregistered files it leaves are removed
  the next time the archive is opened. Nothing else may write meanwhile (ingestion is not safe with
  concurrent writes to a keyspace): the importer holds the archive's directory lock and
  the call holds the writer lock. Same keyspaces, same values: no format or schema change.
- `set_receipts` counts the stored body's transactions over its RLP, without decoding them,
  so it also works for a body holding a transaction the typed decoder refuses.
- Writers are serialized (a fjall batch has no conflict detection, so two appends must not
  read the same tip). The lock is granted in arrival order (tokio's mutex, taken on the
  blocking thread): with an unfair lock a removal re-took it for every batch and no append got
  in until the removal was done. `range` takes one snapshot, so both ends come from one point
  in time even during a trim.
- Space overhead is roughly constant, not a multiple of the window: a journal of at most
  128 MiB plus a few 64 MiB blob files that are dropped only once wholly stale. With a tiny
  window the archive therefore looks many times its live data (measured: 250 to 400 MB for
  37 MB live); at 20 GB it was 1.02x. The archive's disk use, stale blob bytes and running
  compactions are recorded as gauges (exported once the binary installs a recorder); the blocks
  removed by `trim` and `truncate_above` are counted, also when the call ends in a timeout.
- fjall is synchronous: the implementation (`FjallArchive`, cheap to clone) runs each call on
  a blocking thread, so the trait is async like the other two.
- On open: a directory holding an archive of another `schema_version` (or blocks and no
  version) is refused with `StorageError::ArchiveSchema`, naming the directory and both
  versions and telling the operator to load a new archive with `op-indexer-import load` from
  the verified chunks. Nothing is deleted: an archive can hold an import of the whole chain,
  so removing it is the operator's decision.
- On open, the chain: `open` takes the node's `ChainIdentity` (chain id and genesis hash). An
  archive recording another chain is refused with `StorageError::ArchiveChain`, naming the
  directory and both chains, and left as it is; one whose record does not decode is refused
  with `StorageError::ArchiveChainUnreadable`. One with no record is given one first. If it
  holds blocks, a build before the record wrote it, and every such build ran OP Mainnet only,
  so it is recorded as OP Mainnet's (`ChainIdentity::BEFORE_RECORD`) and then compared as
  usual: an imported OP Mainnet archive opened by a Unichain node is refused, not relabelled.
  If it is empty, it is recorded as the node's chain. The importer's `load` opens the archive the same way, so it refuses
  another chain's archive before appending anything. The p2p node store (`node/`, next to
  `archive/`) records and checks its chain the same way (`StoreError::WrongChain`,
  `StoreError::UnreadableChain`); for it, holding data means an identity, saved peers or sync
  progress. The binary opens the archive before the node store, so a refused archive leaves
  the node store without a record.
- **What removes blocks.** `trim` (the oldest, down to a window) and `truncate_above` (the
  newest, above a number), nothing else. Their callers bound them: see `docs/pipeline.md`
  section 4.

### 9.3 Retention and configuration

Retention is by block count, or unlimited. With a window, the caller invokes `trim(window)`
after appending; with unlimited retention it never trims.

| Variable | Default | Meaning |
|---|---|---|
| `OP_INDEXER_ARCHIVE_RETENTION_BLOCKS` | `all` | `all` keeps every block: the archive is the history the node serves and streams. A block count keeps a window of the newest. The archive cannot be disabled. |

The archive lives at `{OP_INDEXER_DATA_DIR}/archive/`. The binary opens it at startup;
promotion and range sync write to it, the importer's `load` too (with the indexer stopped).

**Sizing.** Real blocks (264 consecutive OP Mainnet blocks from live gossip, 2026-10-03
22:32-22:42 UTC, a Saturday): compressed header plus body averages 17.8 KB per block (median
6.7 KB, p95 76.9 KB, max 83.7 KB), bimodal, about one block in six carrying roughly 94 KB of
poorly compressible data. That is about 23 GB of values for 30 days, **without
receipts** (gossip carries none, so they are unmeasured) and before the engine's overhead. The
whole history is plausibly 300 to 500 GB today and grows by roughly 200 GB a year; a full OP
Mainnet node, state included, is about 700 GB. One short weekend sample: repeat at a weekday
peak before relying on it. Unlimited retention only keeps what the indexer has seen; filling
in older history needs the execution-p2p crate.
