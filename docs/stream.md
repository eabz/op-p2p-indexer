# Stream spec (`crates/stream`)

Status: **built, not run**. It compiles and passes the lints; no consumer has subscribed yet,
and the unsafe store's event stream has not been read against a live Redis. Decision:
[roadmap.md](roadmap.md), 2026-10-04, "The node is a source of data, streamed out".

The `stream` crate serves the chain to consumers over gRPC: history from the block archive,
then the live chain from the unsafe store, each block with its status, and reorgs and status
changes as their own messages. It only reads: the archive, the unsafe store, and the unsafe
store's event stream. It depends on `primitives` and `storage` only.

**There is no authentication and no TLS.** Anyone who can reach the port can subscribe. The
binary listens on `127.0.0.1` by default, and docker compose publishes the port on the host's
loopback unless told otherwise (section 5).

## 1. Service

Protobuf package `opindexer.v1` (`crates/stream/proto/opindexer/v1/stream.proto`), compiled at
build time by `protox` (a protobuf compiler in Rust) and `tonic-prost-build`: no system `protoc`
is needed, and no generated code is checked in to drift from the `.proto`. `bytes` fields are
generated as `Bytes`, so the archive's bytes are sent without a copy. The generated module is
the crate's one lint exception (`#[expect]` with its reason, in `lib.rs`): the lints that
generated code trips, and only those.

| RPC | What |
|---|---|
| `Subscribe(SubscribeRequest) returns (stream Event)` | From a block number, or from the head; the payload, `DECODED` or `RAW`, chosen per subscription. A number below the archive's first block is refused with `OUT_OF_RANGE`. |
| `GetHeads(GetHeadsRequest) returns (Heads)` | Unsafe, safe and finalized heads, and whether receipts are fetched. |
| `GetBlock(GetBlockRequest) returns (Block)` | One block by number (canonical) or hash, in either payload. `NOT_FOUND` if not held. |

`Event` is one of:

- `Block`: number, hash, parent hash, status (`UNSAFE`, `SAFE`, `FINALIZED`) at sending time,
  and the payload:
  - `DECODED`: header fields, transactions (with sender, and each in its consensus encoding),
    receipts and their logs;
  - `RAW`: header, body and receipts in their consensus encoding, as stored.

  Receipts are a message field with presence. Absent means "not known yet", not "none".
- `Receipts`: the receipts of a block sent earlier without them (section 3).
- `Reorg`: the blocks from height `from` up are no longer canonical, with their hashes. The new
  canonical blocks from `from` follow as `Block`s.
- `Heads`: the first event of every subscription, and again whenever the committed safe or
  finalized head moves. Every block sent at or below a head has that status from then on. The
  heads are the archive's (`ArchiveStore::heads`), which promotion records once the blocks up
  to the safe head are in the archive: a `SAFE` block is a committed one. Redis has no events
  for these heads.

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
  trimmed events it had not read** (it keeps about the newest ten thousand: the follower reads
  the state again and walks up from its last block), **across a gap** (heights not held stop
  the walk until a `fill` event), and for a reorg whose ancestor it does not know (it finds the
  newest published block still canonical).

  It keeps the last 128 events, numbered (about two minutes of blocks at one a second), and the
  last 256 blocks it published (the deepest reorg handled without starting over from the head).
- **Hand-over** (`subscription.rs`): a subscription reads (history, then the unsafe store),
  checking each block's parent against the last one it sent; one that does not match is a
  reorg, found by reading which of its blocks are still canonical. Before each batch it tries to
  join the window: right after the follower's newest publication of its last block, where the
  follower's chain is its own, so from there every event applies as it is. Receipts published
  before that point, for blocks it sent without them, are sent at the join. A block in the
  window that does not build on its last one, or a window that moved past it, sends it back to
  reading. So each height is sent once per canonical chain, in order, and no event is replayed.
- Each block is prepared once (number, hash and parent read once) and each of its messages
  converted the first time a subscription asks for it, then shared: a live block is converted
  once per payload, not once per subscription. Conversion runs in `spawn_blocking`; archive
  calls run on blocking threads. Every store call is retried while it fails with a transient
  error (`storage::retry`).

A gap in the unsafe chain (heights not received) holds live delivery at the block below it until
the gap is filled or the blocks reach the archive: the stream is contiguous. This is logged.

## 3. Receipts

Receipts are fetched after the block arrives (the `el` crate). A block is sent as soon as it is
canonical, with its receipts if they are attached and without them otherwise. When the unsafe
store records them (its `receipts` event), a `Receipts` event follows for a block among the 256
the follower published last. With the execution network disabled no receipts come;
`Heads.receipts` says so. Receipts attached only after the block was promoted to the archive
(the pipeline attaches late ones there) are not streamed: the archive has no events. A consumer
can ask for the block again with `GetBlock`.

## 4. Backpressure and limits

- Each subscription has a queue of 64 events. HTTP/2 flow control slows the server when the
  consumer reads slowly. A subscription whose queue stays full for 30 s is ended with
  `RESOURCE_EXHAUSTED`, never buffered without bound: one slot of the queue is held back for
  that status, so a full queue still tells the consumer why it ends.
- At most `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS` subscriptions (default 64); one more is refused
  with `RESOURCE_EXHAUSTED`.
- History is read in batches of at most 64 blocks or 16 MiB, each sent before the next is read.
- One reader of the event stream per node (the follower); it waits on a Redis connection of its
  own, so it does not hold up the pipeline's writes.
- On shutdown every subscription ends with `UNAVAILABLE`; the server stops with the networks.

## 5. Configuration

| Variable | Default | What |
|---|---|---|
| `OP_INDEXER_STREAM_LISTEN_ADDR` | `127.0.0.1:50051` | gRPC listen address: local only, since there is no authentication. The image sets `0.0.0.0:50051` inside the container. |
| `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS` | `64` | Concurrent subscriptions. |
| `OP_INDEXER_STREAM_MAX_FLIGHTS` | `8` | Concurrent Arrow Flight `DoGet` streams (section 6). |

docker compose: `OP_INDEXER_STREAM_PORT` (default `50051`) is the port inside and outside the
container; `OP_INDEXER_STREAM_HOST_BIND` (default `127.0.0.1`) is the host address it is
published on. Set it to `0.0.0.0` to expose the stream, knowingly: there is no authentication.

## 6. Arrow Flight: bulk history

Bulk history for data pipelines and analytics (DuckDB, Polars, Spark, warehouses), as columnar
record batches over Arrow Flight. It is served by the same server, on the same port, as a
second gRPC service (`crates/stream/src/flight.rs`). The live tail with reorgs stays on the
subscription; Flight serves ranges. Same rules: no authentication, no TLS.

**Crates.** `arrow-flight` 60 (Apache, Apache-2.0), with no default features: no Flight SQL,
no TLS, no CLI. It uses tonic 0.14 and prost 0.14, the stream's, so the binary holds one tonic.
It also needs `arrow-array`, `arrow-schema` and `arrow-ipc` (also direct dependencies, to build
the batches and the schemas) and `arrow-cast`. A separate crate would move that compile weight,
not remove it, and would duplicate the server's wiring.

**Requests.**

- `DoGet(ticket)`: the ticket is text, `table:from:to[:cap]`, for example
  `logs:120000000:120010000:finalized`.
  - The range `[from, to]` is inclusive.
  - `cap` is one of:
    - `finalized`: up to the finalized head;
    - `safe`: up to the safe head;
    - `any` (the default): up to the unsafe head. Blocks above the archive's tip come from the
      unsafe store and may still be reorged; the `status` column says which they are.
  - `finalized` and `safe` never leave the archive.
  - A `to` above what the cap allows is lowered to it.
  - `OUT_OF_RANGE` when `from` is below the archive's first block, or nothing is held from
    `from` under the cap.
- `GetFlightInfo(descriptor)` and `ListFlights`: the descriptor is a path of one table (its
  whole range, from the lowest block held, cap `any`) or a ticket's text as the command. The info's endpoint carries the
  ticket for the range as it resolves now, `to` lowered. Total records and bytes are unknown
  (`-1`).
- `GetSchema(descriptor)`: the table's schema.
- Everything else (`Handshake`, `DoPut`, `DoExchange`, `DoAction`, `ListActions`,
  `PollFlightInfo`) is `UNIMPLEMENTED`. Flight SQL is not served.

**Reads.** A `DoGet` reads the range as the subscription's history does: archive batches of at
most 64 blocks or 16 MiB, senders from the archive. `blocks` reads the headers alone; the other
tables decode each block once. Each read becomes one record batch, built off the async runtime
while the next is read, so a large range is never held whole. A task produces the batches at
most two ahead of the consumer; HTTP/2 flow control does the rest. On shutdown a `DoGet` ends
with `UNAVAILABLE`.

A range ends early with an error in two cases:

- `UNAVAILABLE`: a block is no longer held (trimmed by retention, or a gap in the unsafe chain).
- `ABORTED`: a block does not build on the one before it (a reorg during the read; only with
  `any`).

At most `OP_INDEXER_STREAM_MAX_FLIGHTS` `DoGet`s run at once (default 8); one more is refused
with `RESOURCE_EXHAUSTED`.

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

**Not run.** No Flight client has read from a node yet.

## 7. Not built

Log filters by address and topic, authentication and TLS, compression, and for Flight:
`DoPut`, `DoExchange`, Flight SQL, and total row counts in `FlightInfo`.
