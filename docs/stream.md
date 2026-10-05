# Stream spec (`crates/stream`)

Status: **implemented**, including Arrow Flight and optional API-key authentication.
See [roadmap.md](roadmap.md) for current validation status. The original design decision is
in [decisions.md](decisions.md), 2026-10-04, "The node is a source of data, streamed out".

The `stream` crate serves the chain to consumers over gRPC: history from the block archive,
then the live chain from the unsafe store, each block with its status, and reorgs and status
changes as their own messages. It only reads: the archive, the unsafe store, and the unsafe
store's event stream. It reads through `primitives` and `storage`; `api` holds shared authentication, tickets and schemas.

**API keys are optional; TLS is not built in.** When keys are configured, gRPC and Flight
require `authorization: Bearer <key>`. With no keys configured, access is unauthenticated.
The binary listens on `127.0.0.1` by default (section 5).

## 1. Service

Protobuf package `opindexer.v1` (`crates/stream/proto/opindexer/v1/stream.proto`), compiled at
build time by `protox` (a protobuf compiler in Rust) and `tonic-prost-build`: no system `protoc`
is needed, and no generated code is checked in to drift from the `.proto`. `bytes` fields are
generated as `Bytes`, so the archive's bytes are sent without a copy. The generated module is
the crate's one lint exception (`#[expect]` with its reason, in `lib.rs`): the lints that
generated code trips, and only those.

| RPC | What |
|---|---|
| `Subscribe(SubscribeRequest) returns (stream Event)` | From a block number, or from the head; the payload, `DECODED` or `RAW`, chosen per subscription. A number the stores do not hold and never will is refused with `OUT_OF_RANGE`: below the archive's first block (the archive keeps all it has), or, with range sync off, between the archive's tip and the unsafe store's lowest block (the unsafe store expires blocks nothing promoted after `UNSAFE_TTL`, 24 h). A subscription whose next height becomes such ends with `OUT_OF_RANGE` too. With range sync on (`OP_INDEXER_EL_SYNC`) that gap is being filled: a subscription in it waits. |
| `GetHeads(GetHeadsRequest) returns (Heads)` | Unsafe, safe and finalized heads, and whether receipts are fetched. |
| `GetBlock(GetBlockRequest) returns (Block)` | One canonical block, by number or hash, in either payload. `NOT_FOUND` if not held, or a side block. |

A message can be up to 64 MiB (a full block, decoded; the server's limit). gRPC clients accept
4 MiB by default: raise the client's maximum receive size.

`Event` is one of:

- `Block`: number, hash, parent hash, status (`UNSAFE`, `SAFE`, `FINALIZED`) at sending time,
  and the payload:
  - `DECODED`: header fields, transactions (with sender, and each in its consensus encoding),
    receipts and their logs;
  - `RAW`: header, body and receipts in their consensus encoding: the archive's bytes as
    stored. The unsafe store keeps blocks decoded, so a block from it is encoded again, to the
    same bytes (a legacy transaction signed with zeros keeps that signature).

  Receipts are a message field with presence. Absent means "not known yet", not "none".
- `Receipts`: the receipts of a block sent earlier without them (section 3).
- `Reorg`: the blocks from height `from` up are no longer canonical, with their hashes. The new
  canonical blocks from `from` follow as `Block`s.
- `Heads`: the first event of every subscription, and again whenever the committed safe or
  finalized head moves. Every block sent at or below a head has that status from then on. The
  heads are the archive's (`ArchiveStore::heads`), which promotion records once the blocks up
  to the safe head are in the archive: a `SAFE` block is a committed one. The unsafe store has
  no events for these heads.

Per subscription, blocks come in chain order, each height once per canonical chain: after a
`Reorg` from `from`, the blocks from `from` are sent again, as their new versions.

## 2. Sources and the hand-over

- **History**: `ArchiveStore::blocks` (whole blocks with senders, one snapshot per batch) up to
  the archive's tip; above it, the unsafe store's canonical chain, a batch at a time (`ancestry`
  from the block a batch up), or one height at a time across a gap.
- **Live**: one follower task for all subscriptions (`follower.rs`) reads the unsafe store's
  event stream (`docs/storage.md` section 3.3: `UnsafeStore::events`, from where it left off,
  waiting up to 5 s). The events are facts: a new head, a reorg with its common ancestor and
  the blocks it replaced, a gap filled, receipts attached. For each, the follower publishes
  what it means for the canonical chain: the blocks on top of the last one it published, a
  `Reorg` of the published blocks a reorg removed, the `Receipts` of a published block. After
  each read it reads the archive's heads and publishes `Heads` when they changed.

  It reads the stores' state only where the events do not say enough: **at start** (the
  stream's position first, then the heads; the head is its first block), **when the stream
  dropped events it had not read** (the unsafe store keeps the newest ten thousand: the
  follower reads the state again and walks up from its last block), **across a gap** (heights not held stop the walk until a `fill`
  event), and when a block does not build on its last one (it finds the newest published block
  still canonical). A reorg event removes the published blocks from the first one it replaced:
  blocks of the new chain an earlier event of the same read already published stay. When it
  reads the state again and the head is at or below its last block (and not it), the store
  went back under it: it finds the newest published block still canonical and publishes the
  `Reorg` of those above.

  It keeps the last 128 events, numbered (about two minutes of blocks at one a second), and the
  last 256 blocks it published (the deepest reorg handled without starting over). A reorg that
  removes all 256 is published, and the follower then publishes again from the first removed
  height, not from the head, so its chain has no gap.
- **Hand-over** (`subscription.rs`): a subscription reads (history, then the unsafe store),
  checking each block's parent against the last one it sent; one that does not match is a
  reorg, found by reading which of its blocks are still canonical. Before each batch it sends
  `Heads` if the follower's heads moved (heads sent never go back), and tries to join the window: when the follower's last
  block is its own, the follower's chain is its own, so from the window's next event every event
  applies as it is. Receipts in the window, for blocks it sent without them, are sent at the
  join. A block in the window that does not build on its last one, or a window that moved past
  it, sends it back to reading. So each height is sent once per canonical chain, in order, and no
  event is applied twice: a subscription that found a reorg itself joins only once the follower
  has seen it too. A subscription from the head starts at the block after the follower's last
  (a reorg of blocks before it is not its own), and while it has sent nothing (or a reorg
  removed all it sent) it takes a block from the window only at the height it expects next.
- Each block is prepared once (number, hash and parent read once) and each of its messages
  converted the first time a subscription asks for it, then shared: a live block is converted
  once per payload, not once per subscription. Conversion runs in `spawn_blocking`; archive
  calls run on blocking threads. Every store call is retried while it fails with a transient
  error (`storage::retry`).

A gap in the unsafe chain (heights not received) holds live delivery at the block below it until
the gap is filled or the blocks reach the archive: the stream is contiguous. This is logged.

## 3. Receipts

Receipts are fetched after the block arrives (the `el` crate). A block is sent as soon as it is
canonical, with its receipts if they are attached and without them otherwise. Then:

- The follower publishes `Receipts` for a block it published without them when the unsafe store
  records them (its `receipts` event), or, for a block at or below the safe head, when the
  archive has them (the pipeline attaches late ones there; the archive has no events, so after
  each read of the event stream the follower reads the archive's list of blocks still without
  receipts, and reads only the blocks that left it).
- A subscription looks its blocks without receipts up again every 5 s, in both stores, while
  reading and while following: those it read from the stores, and those it got from the window
  but stopped following since (the follower publishes receipts only for blocks it published,
  and looks those up again after it reads the state again).
- A subscription waiting in a gap range sync is filling watches only the archive's tip until
  the sync reaches it.

What a consumer can rely on: a block sent without receipts gets one `Receipts` event when they
arrive while it is among the last 256 blocks the subscription sent, and on the same
subscription. Past that, or after subscribing again, it asks with `GetBlock`. With the
execution network disabled no receipts come, and `Heads.receipts` says so.

## 4. Backpressure and limits

- Each subscription has a queue of 64 events. HTTP/2 flow control slows the server when the
  consumer reads slowly. A subscription whose queue stays full for 30 s is ended with
  `RESOURCE_EXHAUSTED`, never buffered without bound: one slot of the queue is held back for
  that status, so a full queue still tells the consumer why it ends.
- At most `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS` subscriptions (default 64); one more is refused
  with `RESOURCE_EXHAUSTED`.
- History is read in batches of at most 64 blocks or 16 MiB, each sent before the next is read.
- At most 16 `GetHeads` and `GetBlock` calls are served at once; one more is refused with
  `RESOURCE_EXHAUSTED`.
- HTTP/2 keepalive: the server pings a connection every 30 s and drops it if the answer takes
  more than 10 s, so a consumer that vanished does not hold a subscription.
- One reader of the unsafe store's events per node (the follower); it waits on the store's
  watch of its newest event, in the process, so it holds up nothing.
- On shutdown every subscription and Flight stream ends with `UNAVAILABLE`. The server stops
  with the networks, waits up to 5 s for open connections (a consumer that stops reading holds
  one), then up to 5 s for its tasks, then gives up on them; the pipeline is stopped at the same
  time, not after.

## 5. Configuration

| Variable | Default | What |
|---|---|---|
| `OP_INDEXER_STREAM_LISTEN_ADDR` | `127.0.0.1:50051` | Shared gRPC/Flight listen address; local by default. |
| `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS` | `64` | Concurrent subscriptions (at most `Semaphore::MAX_PERMITS`; more is lowered to it). |
| `OP_INDEXER_STREAM_MAX_FLIGHTS` | `8` | Concurrent Arrow Flight `DoGet` streams (section 6), with the same ceiling. |
| `OP_INDEXER_STREAM_FLIGHT_QUEUE_MS` | `2000` | How long a `DoGet` waits for a free stream before `RESOURCE_EXHAUSTED` (section 6); `0` refuses at once. |
| `OP_INDEXER_STREAM_API_KEYS` | unset | Comma-separated bearer keys; unset disables authentication. |

Use API keys or a suitable proxy when exposing the listener beyond localhost.

## 6. Arrow Flight: bulk history

Bulk history for data pipelines and analytics (DuckDB, Polars, Spark, warehouses), as columnar
record batches over Arrow Flight. It is served by the same server, on the same port, as a
second gRPC service (`crates/stream/src/flight.rs`). The live tail with reorgs stays on the
subscription; Flight serves ranges. The same API-key configuration applies to both services;
TLS termination remains external.

**Crates.** `arrow-flight` 60 (Apache, Apache-2.0), with no default features: no Flight SQL,
no TLS, no CLI. It uses tonic 0.14 and prost 0.14, the stream's, so the binary holds one tonic.
It also needs `arrow-array`, `arrow-schema` and `arrow-ipc` (also direct dependencies, to build
the batches and the schemas) and `arrow-cast`. A separate crate would move that compile weight,
not remove it, and would duplicate the server's wiring.

**Requests.**

- `DoGet(ticket)`: the ticket is text, `table:from:to[:cap]`, for example
  `logs:120000000:120010000:finalized`.
  - The range `[from, to]` is inclusive; `to` below `from` is `INVALID_ARGUMENT`.
  - A range is cut to 100 000 blocks: the response's `op-indexer-range-to` header gives the
    last block it covers; ask again from the one after it.
  - `cap` is one of:
    - `finalized`: up to the finalized head;
    - `safe`: up to the safe head;
    - `any` (the default): up to the unsafe head. Blocks above the archive's tip come from the
      unsafe store and may still be reorged; the `status` column says which they are.
  - `finalized` and `safe` never leave the archive.
  - A `to` above what the cap allows is lowered to it.
  - `OUT_OF_RANGE` when the stores do not hold `from` and never will (as for `Subscribe`), or
    the cap's head is below `from` (or not known yet).
  - With range sync on, a gap between the archive's tip and the unsafe store's lowest block is
    being filled: a range that starts below it stops below it (`to` lowered), and one that
    starts in it is `UNAVAILABLE`, naming the gap.
- `GetFlightInfo(descriptor)` and `ListFlights`: the descriptor is a path of one table (its
  whole range, from the lowest block held, cap `any`) or a ticket's text as the command. The info's endpoint carries the
  ticket for the range as it resolves now, `to` lowered. Total records and bytes are unknown
  (`-1`).
- `GetSchema(descriptor)`: the table's schema.
- Everything else (`Handshake`, `DoPut`, `DoExchange`, `DoAction`, `ListActions`,
  `PollFlightInfo`) is `UNIMPLEMENTED`. Flight SQL is not served.

**Reads.** A `DoGet` reads the range as the subscription's history does: archive batches of at
most 64 blocks or 16 MiB, senders from the archive. `blocks` reads the headers alone; the other
tables decode each block once. A task reads the stores in order and hands each read to a
blocking thread, which converts it to one record batch and encodes it as Flight messages:
arrow-flight's encoder, which cuts it into pieces of about 2 MiB for gRPC. Up to
`PARALLEL_BUILDS` reads are built at once, so one stream uses that many cores, and their
messages are sent in order, `MESSAGES_AHEAD` of them queued ahead of the consumer
(`crates/stream/src/flight.rs`). A large range is never held whole: a build runs to its end
even while the consumer is slow, so a stream holds up to that many reads with their batches
and encoded messages (about 100 MiB at most); HTTP/2 flow control does the rest. A consumer
that does not read for 30 s is ended with `RESOURCE_EXHAUSTED`; one slot of the queue is held
back for that error. On shutdown a `DoGet` ends with `UNAVAILABLE` at once, not after its
next batch.

**Compression.** A `DoGet` may ask for the record batches' IPC buffers compressed, with the
request metadata `op-indexer-compression: lz4` (LZ4 frame) or `zstd` (`none`, the default,
sends them uncompressed; anything else is `INVALID_ARGUMENT`). Compression is Arrow's own
(the IPC format's buffer compression): readers that support it, such as pyarrow, undo it
transparently; the schema message is never compressed. It costs server CPU in the build
threads above, never on the async runtime. gRPC's own message compression would serve every
client, not only Arrow readers, but it compresses each message again on the async runtime
and gzip alone is common to all gRPC clients; Arrow's lets the build threads do it. In
pyarrow:
`flight.FlightCallOptions(headers=[(b"op-indexer-compression", b"zstd"), ...])`.

A range ends early with an error in two cases:

- `UNAVAILABLE`: a block is not held: a gap in the unsafe chain, or a block above the archive
  that the unsafe store expired (`UNSAFE_TTL`) before the read reached it.
- `ABORTED`: a block does not build on the one before it (a reorg during the read; only with
  `any`).

At most `OP_INDEXER_STREAM_MAX_FLIGHTS` `DoGet`s run at once (default 8). One more waits for
a place up to `OP_INDEXER_STREAM_FLIGHT_QUEUE_MS` (default 2000; 0 refuses at once), among at
most as many waiters as there are places, before it reads anything; past that, or with the
waiters full, it is refused with `RESOURCE_EXHAUSTED`, and a client backs off and asks again
(or asks the next location). A `DoGet` waiting counts as a stream in use in the load a `server`
reports to its balancer, so a server with waiters looks full and gets new jobs last.

**Tables.** Types are Arrow's. `FSB(n)` is `FixedSizeBinary(n)`. A `?` marks a nullable
column. Hashes are `FSB(32)`, addresses `FSB(20)`. Wei amounts (value, mint, gas prices) are
`FSB(32)`, a 256-bit big-endian integer: `Decimal256` holds at most 76 digits, and 2^256 has
78, so it cannot hold every value. To read one as a number, take the bytes as a big-endian
unsigned integer, e.g. in Python `int.from_bytes(value, "big")` (in Polars, through
`map_elements` with that function). Timestamps are `Timestamp(Second, "UTC")`. There is no
chain id column: one node serves one chain. A block without receipts adds no rows to
`receipts` and `logs`, and `blocks.has_receipts` says so.

`blocks`, one row per block:

| Column | Type |
|---|---|
| `number` | `UInt64` |
| `hash`, `parent_hash` | `FSB(32)` |
| `timestamp` | `Timestamp(s, UTC)` |
| `fee_recipient` | `FSB(20)` |
| `state_root`, `transactions_root`, `receipts_root` | `FSB(32)` |
| `logs_bloom` | `FSB(256)` |
| `prev_randao` | `FSB(32)` |
| `gas_limit`, `gas_used` | `UInt64` |
| `base_fee_per_gas` | `UInt64?` |
| `extra_data` | `Binary` |
| `tx_count` | `UInt32` |
| `withdrawals_root` | `FSB(32)?` |
| `blob_gas_used`, `excess_blob_gas` | `UInt64?` |
| `parent_beacon_block_root`, `requests_hash` | `FSB(32)?` |
| `has_receipts` | `Boolean` |
| `status` | `Utf8`: `unsafe`, `safe` or `finalized` when read |

`transactions`, `receipts` and `logs` start with the block's columns: `block_number` `UInt64`,
`block_hash` `FSB(32)`, `block_timestamp` `Timestamp(s, UTC)`.

`transactions`, one row per transaction:

| Column | Type |
|---|---|
| `tx_index` | `UInt32` |
| `hash` | `FSB(32)` |
| `tx_type` | `UInt8` (0, 1, 2, 4, 126 for a deposit) |
| `sender` | `FSB(20)` |
| `to` | `FSB(20)?`: null for a contract creation |
| `nonce` | `UInt64?`: null for a deposit |
| `value` | `FSB(32)` |
| `gas_limit` | `UInt64` |
| `gas_price` | `FSB(32)?`: legacy and access-list transactions |
| `max_fee_per_gas`, `max_priority_fee_per_gas` | `FSB(32)?`: dynamic-fee and set-code transactions |
| `input` | `Binary` |
| `source_hash`, `mint` | `FSB(32)?`: deposits only |
| `is_system_tx` | `Boolean?`: deposits only |
| `encoded` | `Binary`: the consensus encoding (EIP-2718), as the block holds it |

`receipts`, one row per receipt:

| Column | Type |
|---|---|
| `tx_index` | `UInt32` |
| `tx_hash` | `FSB(32)` |
| `success` | `Boolean` |
| `cumulative_gas_used` | `UInt64` |
| `gas_used` | `UInt64`: this transaction's, `cumulative_gas_used` minus the previous receipt's |
| `logs_count` | `UInt32` |
| `deposit_nonce`, `deposit_receipt_version` | `UInt64?`: deposits from Regolith and Canyon on |

`logs`, one row per log:

| Column | Type |
|---|---|
| `log_index` | `UInt32`, within the block |
| `tx_index` | `UInt32` |
| `tx_hash` | `FSB(32)` |
| `address` | `FSB(20)` |
| `topic0` … `topic3` | `FSB(32)?` |
| `data` | `Binary` |

**Validation:** local Flight benchmarks are recorded in [serving.md](serving.md); production
fleet validation remains in [roadmap.md](roadmap.md).

## 7. Not built

Log filters by address and topic, built-in TLS, compression, and for Flight:
`DoPut`, `DoExchange`, Flight SQL, and total row counts in `FlightInfo`.
