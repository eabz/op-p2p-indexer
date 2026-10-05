# Storage specification

Scope: the `storage` crate only, plus the shared types it needs in `primitives`. See
[roadmap.md](roadmap.md) for why storage is built first and what feeds it later.

| Store | Holds | Why |
|---|---|---|
| **Unsafe chain** (in memory, journaled to fjall) | Unsafe blocks: live, not yet committed to L1, with fork choice. | Small, changes shape on reorgs; in the node process, so no service. |
| **fjall** (archive, the committed store) | Every committed block in its consensus encoding, with each transaction's sender, and the committed L1 heads. | Embedded, needs no service; serves peers and the stream by number or hash (section 9). |

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
from the unsafe store to the archive (that is `pipeline`). Storage provides the operations
promotion needs.

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

- `MemoryStore` and `FjallArchive` are the implementations. Both are cheap to clone.
- `StorageError` is one `thiserror` enum with `severity(&self) -> Severity`:
  - **Transient** (retry can help): a local disk I/O failure of the archive or the journal.
  - **Expected** (the caller handles it, nothing is wrong with the store): `MissingAncestor`,
    `AncestryTooLong`, and the archive's `NotContiguous`.
  - **Fatal** (needs an operator): everything else, including a schema or chain mismatch,
    undecodable stored data, and a block that does not fit the schema.
  Variants carry what failed: the operation for engine errors, the block for decode errors.
  The stores do not retry. The crate exports `retry(cancel, store, operation, budget, call)`
  and `RetryError`, which repeat a call while its error is transient, with exponential backoff
  and jitter, until `cancel` fires or the optional `budget` of time is spent; the pipeline and
  the importer use it.
- **Retries.** Every write is idempotent. An insert whose journal write failed was applied in
  memory: a retry returns `stored = false` and no events (the readers got them the first
  time). A caller that needs them re-reads `head()`.
- **Cancel safety.** Dropping a future never corrupts a store: the unsafe store's writes run to
  their end on a blocking thread, its reads are immediate.
- Module layout: `storage::unsafe_store` (`MemoryStore`; `chain` holds the state and fork
  choice, `journal` the fjall journal, `layout` every limit), `storage::archive_store` (fjall);
  the traits, the configuration types, `StorageError`, `Severity`,
  `InvalidBlockReason` and `Store` are exported from the crate root.

## 3. The unsafe chain (unsafe store)

Decision D0 (`docs/serving.md`, 2026-10-04): no Redis. The unsafe chain lives in memory in the
node process, and every change to it is journaled to a small local fjall database, so a
restart replays it without the network.

### 3.1 In memory, and the journal

In memory (`unsafe_store::chain`, behind one lock):

| What | Content |
|---|---|
| blocks | hash → the block in its consensus encoding (header, body, receipts once attached), its senders, its source, and its number, parent and timestamp read from the header. |
| heights | number → the hashes of every block seen at that height, canonical and side blocks. |
| canonical | number → hash: the canonical unsafe chain, at most one block per height. A missing height is a gap. |
| head | the unsafe head (it can outlive its block, which a prune may remove). |
| L1 heads | the safe and finalized heads, as `set_l1_heads` gave them. |
| events | the newest 10,000 events with sequence ids (section 3.3). |

Blocks are held encoded, as gossip and the archive carry them, and decoded only when read
whole (`block`, `canonical`, `ancestry`); serving reads (headers, bodies, receipts) return the
held bytes. When a block is stored its transactions root, and its receipts root once it has
receipts (by the rules at its time, hence `UnsafeConfig::canyon_time`), are checked against its
header over exactly those bytes; a mismatch is `InvalidBlock` (`TransactionsRoot`,
`ReceiptsRoot`) and nothing is stored, so what is served is what the header commits to.

**Bounds.** Blocks leave when the caller prunes; when their height's blocks are more than a
day (`RETENTION_SECS`) older than the newest block, a few lowest heights per insert, the
backstop for when nothing prunes (no L1); and when the blocks take more than
`UnsafeConfig::max_bytes` (`OP_INDEXER_UNSAFE_MAX_BYTES`, default 2 GiB), lowest heights first,
never the head's. Evictions are logged. Readers must not expect blocks older than that.

**Measured** (2026-10-04, synthetic, release build, Apple M-series): 3,600 linked blocks of 20
EIP-1559 transactions with two logs each, 31 KB per block encoded (header, body and receipts;
OP Mainnet averages 17.8 KB without receipts, section 9.3): about 51 KB of process memory per
block held (the encoding, the maps and the allocator), inserts at about 0.27 ms each, and a
replay of the 3,600 blocks from the journal in about 75 ms. So 2 GiB holds about a day of OP
Mainnet blocks, and a few hours of Base's, whose blocks with receipts are ten times larger:
raise the cap there, or run with L1, whose promotion prunes every game (about 20 minutes).

**The journal** (`unsafe/` in the data directory, `unsafe_store::journal`): a fjall database of
its own with two keyspaces:

| Keyspace | Key | Value |
|---|---|---|
| `blocks` | insertion order (big-endian `u64`) | the block: hash, source, senders, then header, body and receipts each with its length |
| `meta` | `schema_version`, `chain`, `safe_head`, `finalized_head` | the layout version, the chain (as the archive records it), the L1 heads |

A block is written when it is stored, written again when its receipts arrive, and deleted when a
prune, retention or the cap removes it, so the journal never holds more than the chain does.
Writes are not synced one by one (blocks come again over gossip); a crash loses at most the
last moments. `open` replays every block in insertion order through fork choice with the
recorded L1 heads, which rebuilds the same chain; a block it no longer takes (at or below the
safe head) leaves the journal. A journal of another chain is refused (`UnsafeChain`); one of
another layout version is emptied, since unsafe blocks are disposable. Its memtable and cache
are small (8 MiB each): the chain is in memory, the journal is only read on open.

### 3.2 Fork choice and reorgs

Blocks reaching the unsafe store are assumed valid (signature and hash checked upstream). The
unsafe store decides which are canonical. The whole decision for one block runs under the
chain's lock, so readers never see a half-applied reorg. The rules are the ones the Redis
store's Lua scripts had, ported step for step (`unsafe_store::chain`).

The rule follows what an op-node follower does with gossiped unsafe payloads (verified against
optimism `develop` @ c8e4ba855d79, `op-node/rollup/engine/payloads_queue.go` and
`engine_controller.go`): the unsafe head only ever moves to a **strictly higher** number. A block
at or below the head never replaces it, and neither does a block at `head + 1` on a different
parent. A block two or more heights above the head becomes the head whatever chain it is on.
There is no "latest wins" and no chain-length comparison.

For a new block `B` (number `n`, hash `h`, parent `p`), with `H` the current head:

1. `h` already stored: no-op, no events.
2. `n` at or below the safe head: ignored. Live input never changes what L1 has committed.
3. Store the block and add `h` to the hashes at height `n`.
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
at once and the gap is repaired by step 7 when the missing block arrives. Fork choice is one
function (`Chain::fork_choice`).

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
  situation. The fix is for `set_l1_heads` to become a write that, when the safe block
  contradicts `canonical`, removes the contradicted entries, moves the head to the safe block
  and emits a `reorg`. This is reachable when the L1 side publishes a safe head on a branch the
  unsafe store does not follow; the fix is not done, the gap is open.
- **When the unsafe head is below the safe head**, a block directly above the safe height
  becomes the head only if its parent is the safe head; `gap` is then `0`, because the heights
  in between are committed, not missing.
- **A reorg can be long.** One `reorg` event lists up to `MAX_REORG_DEPTH` replaced hashes.
- **Every height a fill writes emits `Filled`**, whether the height was empty or held another
  branch's block, so a reader learns the new canonical hash at each height it changed.
- **The head can outlive its block.** Prune, retention or the cap may remove the block the head
  points at; the head stays, because fork choice only compares hashes against it.
- **Retention follows block time**, not the head: a height leaves once its blocks are a day
  older than the newest block stored. A day is 43,200 heights on OP Mainnet and Base and 86,400
  on Unichain.

`set_receipts` attaches the receipts only if the block is still stored, and emits `Receipts`.
The receipts must be one per transaction, hash to the header's receipts root, and the number
must match the stored block, otherwise `InvalidBlock`. `insert` checks the same for a block
that arrives with receipts.

`prune(up_to)` removes every block at or below `up_to.number` (side blocks included) and its
canonical entry, from memory and the journal, then emits `Pruned`. Call
`set_l1_heads` with the new safe head **before** pruning up to it: otherwise a block gossiped
again at a pruned height would be stored and could be filled back in.

`set_l1_heads`: a `None` head means "unknown" and leaves the recorded one untouched, as in
the archive's `set_heads`. Forgetting the safe head would silently switch off step 2.

Reads for serving execution peers, from memory, returning the held bytes:

- `canonical_number(hash)`: the block's height if it is canonical.
- `canonical_headers(from, count, rising)`: consecutive canonical headers as RLP, ending at
  the first gap, missing block or broken parent link.
- `canonical_items(hashes, Body | Receipts)`: the bodies or receipts of the leading hashes
  that are canonical and stored (receipts attached), as RLP; consecutive ones must link. Their
  roots were checked when they were stored.
- `canonical_run(above, max)`: the last block of the unbroken canonical run above `above`
  (whose first block names `above` as its parent) whose blocks all have receipts, looking at
  most `max` heights: what serving advertises.

`ancestry(head, stop_at)` returns the complete range or an error, never a partial one:
`MissingAncestor` if a parent on the way down to `stop_at + 1` is not stored (pruned, expired,
or never received), and `AncestryTooLong` if the range exceeds `MAX_ANCESTRY_BLOCKS`, checked
before any read. Callers ask for bounded ranges.

`insert` rejects a block with ommers or non-empty withdrawals (`InvalidBlock`), so nothing is
dropped silently. The checks (`validate_block`) run before any write: one sender (and, with
receipts, one receipt) per transaction, and a transaction type the store has no place for is
`UnsupportedTransaction`.

### 3.3 Events for live readers

Every write's events go to an in-memory ring of the newest 10,000 (`EVENTS_KEPT`), each with
a sequence id (`EventId`, from 1, restarting with the process), and a `watch` of the newest id
wakes readers. `last_event_id()` and `events(after, count, wait)` read them; `events` waits up
to `wait` for one after `after`, and sets `missed` when events after `after` have left the ring
(the reader then reads the state again). The events are the `UnsafeEvent`s of section 1:

| Event | Meaning |
|---|---|
| `NewHead { head, gap }` | the head moved; `gap` when heights between the old head and this one are missing |
| `Reorg(Reorg)` | canonical entries were replaced or removed: `common_ancestor` (absent when not known), old and new head, `replaced` (newest first) |
| `Filled(BlockRef)` | a block below the head became canonical (a gap was repaired) |
| `Receipts(BlockRef)` | receipts were attached |
| `Pruned { up_to }` | everything at or below this is gone |

A `Reorg` that moves the head is always followed by the `NewHead` of the new head, from the
same write. Readers are in the node process (the stream); a restart restarts them too.

## 4. Committed store

The archive (section 9) is the committed store. The ClickHouse committed store that was here
(tables `blocks`, `transactions`, `receipts`, `logs`, `chain_state`, `imported_ranges`) was
removed with its client, migrations and configuration. A database an earlier build wrote is
not read; drop it when convenient.

## 5. Migrations

### 5.1 Archive

The archive's layout has a version in `meta` (`schema_version`, section 9.1). An archive of
another version is refused on open and left as it is; there is no migration in place. Version
2 added the `senders` keyspace and the heads in `meta`; an archive of version 1 is started
again in a new directory.

### 5.2 Unsafe chain

The journal's `meta` holds its layout version. On open: absent means write the current
version; equal means continue; different means **empty the journal and start empty**. Unsafe
blocks are disposable. A Redis store an earlier build wrote is not read; stop the service.

## 6. Connectors and configuration

| Store | Crate | Notes |
|---|---|---|
| Unsafe chain's journal | `fjall` 3.x | Embedded; `unsafe/` in the data directory. |
| Archive | `fjall` 3.x | Embedded; `archive/` in the data directory. |

Neither needs a service. `storage::config` defines `StorageConfig { unsafe_chain:
UnsafeConfig, archive: ArchiveConfig, chain: ChainIdentity }` as plain data, with
`UnsafeConfig { path, canyon_time, max_bytes }` (the journal's directory, the chain's Canyon
time for receipts roots, the memory cap) and `ArchiveConfig { path }`. The archive cannot be
disabled and keeps every block (section 9.3). The binary fills it from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `OP_INDEXER_UNSAFE_MAX_BYTES` | 2 GiB | Memory the unsafe chain's blocks may take (section 3.1). |

The binary opens the archive, then opens the unsafe chain and replays its journal, and fails
fast if either is another chain's. The pipeline then writes the blocks.

### Several instances on one host

Several indexers can run on one server: several chains (OP Mainnet, Unichain, Base), or several
builds of one chain. Each needs:

- **Its own data directory** (`OP_INDEXER_DATA_DIR`). By default it is named after the chain,
  `data-op`, `data-unichain` or `data-base` (`ChainSpec::default_data_dir`), so two chains on one host
  never share a directory unless told to. Two builds of the same chain need an explicit one each. The archive, the
  unsafe chain's journal and the node store all live in it, record their chain and refuse
  another's: nothing else is shared, so two instances of one chain need nothing more.
- **Its own ports**: `OP_INDEXER_P2P_LISTEN_ADDR`, `OP_INDEXER_EL_LISTEN_ADDR`,
  `OP_INDEXER_L1_LISTEN_ADDR`, `OP_INDEXER_L1_BEACON_LISTEN_ADDR` and
  `OP_INDEXER_STREAM_LISTEN_ADDR`, plus the advertised addresses on a public host.
- **Its own L1 side**, if L1 is enabled: each instance runs its own beacon light client and
  its own L1 execution peers. Two instances do not share them; nothing on L1 is
  per-chain except the dispute game factory.
- **Its own `.env`**: the settings above, in a file per instance. Run each instance from its own
  directory, where it reads `.env`, or point it at its file with `--env-file <path>`. The end of
  `.env.example` shows a Unichain and a Base instance next to an OP Mainnet one (their ports
  shifted by 100 and by 200):

```bash
./target/release/indexer --env-file unichain.env
```

Storage has no metrics (removed 2026-10-04, to be re-added later where needed): retries,
reorgs, evictions and failed operations are logged.

## 7. Open points

- **Senders.** Promoted and synced blocks get senders the pipeline recovered. Unproven: the
  zero address of a pre-Bedrock legacy transaction signed with all zeros, which has no signer.
- **Encoding runs on the calling task.** A block's encoding and root check before it is stored
  run on the calling task; blocks are small. The write and the journal run on a blocking thread.
- **Tests.** `CLAUDE.md` says no tests for now. Fork choice (3.2) is a state machine that live
  runs will rarely exercise. It is a port of the Lua scripts step for step; until the rule
  changes, verify it by driving `MemoryStore` with hand-made block sequences in a scratch
  program, and report the sequences and results.

## 8. Constants

| Constant | Value | Where |
|---|---|---|
| `RETENTION_SECS` | 24 h | heights older than this below the newest block leave |
| `RETENTION_HEIGHTS_PER_INSERT` | 16 | expired heights removed per insert |
| `MAX_REORG_DEPTH` | 256 | jump and fill walks |
| `MAX_ANCESTRY_BLOCKS` | 1024 | one `ancestry` call |
| `EVENTS_KEPT` | 10000 | events kept for readers |
| `OP_INDEXER_UNSAFE_MAX_BYTES` | 2 GiB | the unsafe chain's memory cap |
| Journal cache / journal cap / memtable | 8 MiB / 64 MiB / 8 MiB | the unsafe chain's journal |
| Archive cache / journal cap / memtable | 64 MiB / 128 MiB / 16 MiB per keyspace | fjall archive |
| Archive background threads | 2 | fjall archive |

## 9. Local block archive (fjall)

**Why.** The archive is the committed store: every block committed to L1, kept in the encoding peers ask for, with each transaction's sender, and the committed
L1 heads. Promotion (pipeline) appends to it and records the heads, the importer and range
sync fill it, serving (`el`) and the stream (`stream`) read it. Serving also reads the
canonical unsafe blocks above the archive's tip from the unsafe store (`docs/el.md`
section 11), with the unsafe store's `canonical_*` reads (section 3.2).

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
| `pending_receipts` | number | block hash, for each archived block without receipts: written in the same batch as the block, removed in the same batch as its receipts (`set_receipts`). Added after version 2 without a new version: a version-2 archive gains it empty on open, which is right for an import (every imported block has receipts) |
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
    /// The archived blocks without receipts from `from` up, then from the lowest (wrapping),
    /// at most `limit`, and how many in all.
    async fn pending_receipts(
        &self,
        from: BlockNumber,
        limit: usize,
    ) -> Result<(Vec<BlockRef>, u64), StorageError>;
    async fn set_heads(&self, heads: L1Heads) -> Result<(), StorageError>;
    /// The number of the archived block with this hash.
    async fn number_of(&self, hash: BlockHash) -> Result<Option<BlockNumber>, StorageError>;
    /// The first and last archived block, or None if empty.
    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError>;
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
  batches in place and `range` says where to resume. Under the lock each batch is checked again
  against the tip: blocks another writer appended in the meantime (promotion and range sync
  both append) are skipped, not refused.
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
  write is acknowledged only after the journal is synced to disk. Dropping the future does
  not cancel a call already running on its blocking thread.
- The held range comes from the first and last keys of `headers`, never from a count.
- `set_receipts` counts the stored body's transactions over its RLP, without decoding them,
  so it also works for a body holding a transaction the typed decoder refuses.
- Writers are serialized (a fjall batch has no conflict detection, so two appends must not
  read the same tip). The lock is granted in arrival order (tokio's mutex, taken on the
  blocking thread). `range` takes one snapshot, so both ends come from one point in time.
- Space overhead is roughly constant: a journal of at most 128 MiB plus a few 64 MiB blob
  files that are dropped only once wholly stale. A small archive therefore looks many times
  its live data (measured: 250 to 400 MB for 37 MB live); at 20 GB it was 1.02x.
- fjall is synchronous: the implementation (`FjallArchive`, cheap to clone) runs each call on
  a blocking thread, so the trait is async like the other two.
- On open: a directory holding an archive of another `schema_version` (or blocks and no
  version) is refused with `StorageError::ArchiveSchema`, naming the directory and both
  versions and telling the operator to start a new one. Nothing is deleted: an archive can hold
  the whole chain, so removing it is the operator's decision.
- On open, the chain: `open` takes the node's `ChainIdentity` (chain id and genesis hash). An
  archive recording another chain is refused with `StorageError::ArchiveChain`, naming the
  directory and both chains, and left as it is; one whose record does not decode is refused
  with `StorageError::ArchiveChainUnreadable`. One with no record is given one first. If it
  holds blocks, a build before the record wrote it, and every such build ran OP Mainnet only,
  so it is recorded as OP Mainnet's (`ChainIdentity::BEFORE_RECORD`) and then compared as
  usual: an imported OP Mainnet archive opened by a Unichain node is refused, not relabelled.
  If it is empty, it is recorded as the node's chain. The p2p node store (`node/`, next to
  `archive/`) records and checks its chain the same way (`StoreError::WrongChain`,
  `StoreError::UnreadableChain`); for it, holding data means an identity, saved peers or sync
  progress. The binary opens the archive before the node store, so a refused archive leaves
  the node store without a record.
- **Nothing removes blocks.** The archive only grows (section 9.3); a block is replaced only
  by an import or append of the same block.

### 9.3 Retention and configuration

The archive keeps every block: it is the history the node serves and streams. There is no
window and no setting; the archive cannot be disabled.

With the range sync on (`OP_INDEXER_EL_SYNC`) and no L1 side, the sync anchors on a block 64
below the gossiped head, so the archive holds blocks L1 has not committed (the stream marks
them unsafe), and an unsafe reorg deeper than 64 blocks leaves it on a dead branch, which only
rebuilding the archive repairs. With the L1 side every archived block is committed.

The archive lives at `{OP_INDEXER_DATA_DIR}/archive/`. The binary opens it at startup;
promotion and range sync write to it.

**Sizing.** Real blocks (264 consecutive OP Mainnet blocks from live gossip, 2026-10-03
22:32-22:42 UTC, a Saturday): compressed header plus body averages 17.8 KB per block (median
6.7 KB, p95 76.9 KB, max 83.7 KB), bimodal, about one block in six carrying roughly 94 KB of
poorly compressible data. That is about 23 GB of values for 30 days, **without
receipts** (gossip carries none, so they are unmeasured) and before the engine's overhead. The
whole history is plausibly 300 to 500 GB today and grows by roughly 200 GB a year; a full OP
Mainnet node, state included, is about 700 GB. One short weekend sample: repeat at a weekday
peak before relying on it. The archive holds only what the node has seen; older history comes
from the importer or the range sync.
