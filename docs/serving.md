# Serving at scale: chunks, `server` and `balancer`

The fleet: sealed chunks in R2, the `server` that reads them, the exporter that seals new
ones and the `balancer` that spreads requests. Status 2026-10-05: all built; Unichain is
served from R2 (1,694 chunks) by three servers and a balancer; the exporter runs there but has
not yet been seen sealing a chunk ([roadmap](roadmap.md) #2). Design choices are marked
**D**; their history is in [decisions.md](decisions.md) (2026-10-04, "Four binaries").

The programs are in [architecture.md](architecture.md#programs). R2 holds only sealed,
immutable history; live and recent data never go through it: every server follows the chain
itself, as `indexer` does, with its unsafe chain in memory and journaled to fjall
([storage §3](storage.md#3-the-unsafe-chain-unsafe-store), **D0**). Servers keep no block
data: any server can serve any chunk, and adding one adds R2 read bandwidth and nothing to
fill.

## 1. The chunk

A chunk is an immutable object holding a contiguous range of committed blocks of one chain,
every one of them finalized on L1 and with its receipts.

### 1.1 Contents

**D1.** Per block, the bytes that are already verified and that the archive and peers use:
- the block hash;
- header and body in their consensus encoding (RLP);
- the receipts in their consensus encoding **without blooms** (eth/69's receipt form);
- the senders (20 bytes per transaction).

A record is built from the archive's `ArchivedBlock` (`ChunkRecord::new`), each receipt's
bloom checked against its logs and then dropped. A receipt's bloom is recomputed from its logs when something needs it:
an eth/68 peer, a receipts root check, the stream's decoded receipts. `el` already does this
for eth/69 (`docs/el.md`).

Measured on 2,000 post-Isthmus OP Mainnet blocks (140,000,063 to 140,002,062, 50,368
transactions): 73 KB per block uncompressed, of which receipts 65.6 %, bodies 32.8 %, headers
0.8 %, senders 0.7 %.

The blooms are about 13 % of the uncompressed receipts bytes on this sample (256 B × 25
transactions per block). The compressed saving was not measured.

### 1.2 Layout and compression

Built (`crates/chunks/src/format.rs`):

```text
chunk    = segment* | index | footer
segment  = one zstd frame (level 1) of about 1 MiB of block records (closed once it reaches 1 MiB)
record   = hash (32) | sender count u32 | senders (20 each) | header length u32 | header RLP
         | body length u32 | body RLP | receipts length u32 | receipts RLP without blooms
index    = zstd frame: version u16 | chain id u64 | first u64 | last u64 | first parent (32)
         | last hash (32) | level i32 | segment count u32
         | per segment: offset u64, length u32, first block u64, blocks u32, sha256 (32)
         | per block, in block order: hash (32)
footer   = index offset u64 | index length u32 | magic "OPXC"
root     = sha256 of the index frame: the manifest records it, the object's name carries it
```

Integers are little-endian. **D2.**
- **Segmented zstd frames.** One block is read by decompressing one ~1 MiB segment, not the
  chunk. A ranged GET of one segment, or of a run of them, works on R2.
- **A per-chunk index** with every block's hash, in the chunk itself: by-hash lookups within
  a chunk, and the input of the global index (section 2.4).
- **Index and footer at the end**: the manifest records where the index starts
  (`footer_offset`), so one ranged GET gives the index without downloading the chunk.
- **A two-level root**: the manifest holds the sha256 of the index, and the index holds each
  segment's sha256. Any ranged read is checked before it is decompressed, for a whole chunk or
  one segment alike. There is no hash over the whole object, which a single-segment read could
  not check.

**D3. Light compression: zstd level 1, in 1 MiB segments** (6.6× on that sample; level 3 is
8.3× and decompresses as fast, but seals slower). CPU is weighed above R2 storage. The format
reads any level, so this can change per chunk without touching readers. The benchmark behind
it is in [decisions.md](decisions.md).

### 1.3 Size of a chunk

**D4.** Variable ranges cut by a deterministic rule:
- a chunk ends at the first block that brings its uncompressed records to the size target, or
  at **100,000 blocks**, or at the block before the chain's Bedrock block, whichever comes
  first;
- a chain's boundaries are then the same whoever cuts them (section 4, idempotence).

The manifest (section 2) lists every chunk's range, so a block's chunk is a binary search, not
arithmetic.

**What sizes a chunk**: with no cache, every read comes from R2, so the size follows R2's
time to first byte (TTFB) and its streaming throughput: large enough that each Class B
operation buys many bytes (section 6.6), small enough to be one Flight job a server streams in
seconds, so the balancer can spread a range over many servers. A server opens the next
chunk's stream before the reader reaches it (5.2), so the boundary costs no round trip.

The target is **256 MiB uncompressed**: about 3,600 OP Mainnet blocks and about 39 MB at
level 1. Legacy blocks (0.5 KB) give 100,000-block chunks of a few MB.

### 1.4 Whole-chain size

- **OP Mainnet**: about 500 GB of chunks (estimate, from the level-3 to level-1 ratio on the
  sample), about 50,000 objects; its fjall archive is 914 GB (measured).
- **Unichain**: 1,694 chunks for blocks 0 to 60,422,316 (in R2).
- **Base**: 2.57 TB of downloaded answers; the archive was estimated at 2 to 3.5 TB
  ([base.md](base.md) §6), so roughly a TB of chunks.

### 1.5 Integrity: how a server checks a fetched chunk

**D5.** Three checks, no root recomputation (CPU is the dearer resource):
1. **the chunk's root**: the index frame hashes to the manifest's sha256, and every segment
   read hashes to the index's sha256 for it before it is decompressed: transport or storage
   damage, or tampering, before a byte is used (one flipped byte fails "a segment does not
   hash to the index's sha256"). zstd's own frame checksum is not checked again;
2. **the manifest chain**: the segments' hash chain, the chunk's first parent hash and last
   hash equal to the manifest's, and its first parent equal to the previous chunk's last hash.
   The chain of chunks ends at a block the server verified itself (its own committed head,
   through the `l1` crate), so one chunk cannot be swapped without breaking the hashes back to
   a block L1 committed;
3. **header hashes and parent links**, only for a reader that does not trust the index
   (`import fetch`, the importer's anchor check): every header hashes to its record's hash and
   names the previous block as its parent. A server reading its own deployment's chunks does
   not: checks 1 and 2 tie each segment's bytes to the manifest, and every block was verified
   before it was sealed.

A failed check is an error for that read; it is not retried. Transient store errors are
retried by the R2 client (up to 10 times, 200 ms to 30 s, at most 3 minutes) and then, for
stream readers, by the storage retry policy until the reader is cancelled.

A read decodes only what its reader needs (`ReadParts`): the receipts are left out for the
headers and transactions of Flight, and their blooms (a keccak per log address and topic,
most of what a read costs) for the receipts and logs of Flight and the decoded payload of
gRPC, which carries no receipt blooms; the raw payload and the follower read whole blocks.
CPU per GB of block data read: whole 4.1 s (about 245 MB/s a core), without blooms 1.85 s
(about 540 MB/s), without receipts 0.94 s (about 1,060 MB/s). Raw headers and bodies are
slices of the decompressed segment, sent as they are; raw receipts need their blooms rebuilt,
which the chunk does not store.

Transactions and receipts roots are not recomputed on fetch: the header hash binds them, and
they were recomputed when the chunk was sealed (`verify`, or the node's promotion check).
Senders were proven at seal time (recovered, or hashed for deposits) and are not recovered
again.

## 2. R2 layout and the manifest

### 2.1 Layout

```text
<bucket>/<prefix>/
    chunks/<first:012>-<last:012>-<root, 16 hex>.opxc       immutable
    manifest/<sequence:010>.json                            immutable, append-only
    index/<generation:06>/<shard:03x>.idx                   immutable (section 2.4)
```

The bucket defaults to `<chain>-snapshot` (`op-snapshot`, `unichain-snapshot`,
`base-snapshot`) and the prefix to `archive` (`r2.bucket`, `r2.prefix`).

**D6.**
- Every manifest segment records the chain id and genesis hash, and a reader of another chain
  refuses it, so a bucket can never mix two chains.
- A chunk's name carries its content hash: an object is never overwritten, and two writers of
  the same chunk write the same name and bytes.

### 2.2 The manifest

**D7.** The manifest is a sequence of immutable segments.
- Each segment lists the chunks it adds, in order, each with:
  - range;
  - first parent hash and last hash;
  - sha256 and size;
  - footer offset;
  - compression level.
- It also records:
  - its format version, the chain id and genesis hash, and its sequence number;
  - the sha256 of the previous segment, so segments form a chain;
  - its exporter's id and the time;
  - the hash index generation, when it starts one (section 2.4).
- Segment 0 starts at the chain's first chunk.

A reader lists `manifest/` (R2 listings are strongly consistent), reads the segments after the
last one it had, and checks their chain.

### 2.3 Appending atomically

**D8.** One writer per chain (the exporting server, D11, or `import verify` while it runs, never
both). A chunk becomes visible only after its object is complete:
1. Upload the chunk object (multipart above 64 MiB; R2 has no partial objects).
2. Write the next manifest segment.

With one writer nothing races. As a guard against a second writer started by mistake, every
single-part PUT is create-only (`If-None-Match: *`), falling back to a plain PUT only where
the store does not support it; multipart uploads have no guard. No object is ever modified. A
chunk uploaded but never listed is unread garbage; no sweep removes it.

### 2.4 By-hash lookups: a global index in R2

With no local state, a hash (an eth request by hash, `GetBlock` by hash) needs a global
hash → number index the servers read from R2.

**D9.** Hash-prefix shards, immutable, in generations:
- **Shards**: 4,096 files per generation, split by the hash's first 12 bits. Each holds its
  entries sorted: hash bytes 2 to 10 and the block number (u64), 16 bytes an entry. A fan-out
  table heads each shard: 257 u32s, where each value of the hash's byte 2 starts, then the
  count (1 KB). Built: `crates/chunks/src/hash_index.rs`.
- **Sized for OP Mainnet**: 157.7 M blocks give about 38,500 entries per shard. That is 616 KB a
  shard and 2.5 GB a generation, about 150 entries (2.4 KB) per fan-out bucket. An 8-byte
  remainder after a 12-bit prefix leaves a false match below one in 10^9 over the whole chain;
  a match is confirmed by the block's own hash anyway.
- **Lookup**: two ranged GETs, the shard's fan-out table at offset 0 (fixed size, so no HEAD)
  and then its bucket (about 3 KB), then the block's own segment.
- **Generations**: shards are never modified. Each new generation merges the previous one's
  shards with the hashes of the chunks sealed since; names carry the generation number
  (`index/<gen>/<prefix:03x>.idx`). `import verify` writes one at the end of each run, the
  exporter one every 50,000 blocks. A generation is 2.5 GB and 4,096 PUTs for OP Mainnet, and
  old generations are not deleted.
  - Between generations the hashes of the chunks sealed since the last generation are in those
    chunks' indexes (section 1.2). That is at most about 14 chunks at OP Mainnet's rate, read
    with one ranged GET each, newest first, only when the generation's shard misses.
- **No generation yet** (the manifest names none): every by-hash lookup of sealed history is
  not found, with one warning.
- **Recent blocks** (the unsealed tail and the unsafe chain) are found locally.

No index service: by-hash reads of old blocks are rare (eth peers ask by number, except for
the block a range sync starts from).

## 3. Sealing history: the importer

`import verify` seals a downloaded range into these chunks, uploads them and lists them once
the range matches its anchor ([import §3.2](import.md#32-verify-check-seal-and-upload)). The
`import export` step and the verified copy it read are gone ([decisions.md](decisions.md),
2026-10-05).

## 4. Sealing new chunks: the exporter

**D11.** Sealing rule: a chunk is sealed when **every** block in it is at or below the L1
**finalized** head and **has its receipts**. The chunk's last block is the D4 boundary, so
sealing waits for the whole range; the newest partial range stays in the servers' tails.

**D12.** The exporter is a **mode of the `server` binary** (`server --export`): exactly one
server in a deployment runs it, and it holds the only R2 write key; every other server has a
read-only key. It exports a chunk as soon as that chunk's range has been promoted into its tail
and meets the sealing rule:
- every 10 s it reads the committed blocks after the manifest's last chunk from its tail,
  256 blocks or 64 MiB at a time;
- it adds each to the chunk being written while the block is at or below the tail's finalized
  head and has its receipts;
- it seals the chunk when the writer ends it, uploads it and appends the manifest segment (D8).

The other servers see the new segment, so their tails drop the sealed blocks. The tail must
hold the block after the manifest's last chunk, which range sync fetches; until it does, the
exporter warns "the tail does not hold the next block to seal".

**Status**: running on the Unichain fleet; not yet seen sealing a chunk.

Idempotence:
- boundaries are deterministic (D4);
- names are content-addressed (D6);
- on start, the exporter reads the manifest and continues after its last chunk.

So a crash re-seals the same chunk with the same name and bytes, and an upload repeated after
a crash is harmless.

## 5. The server

### 5.1 State

**D13.** A server keeps no history. Its local state:
- the node store (identity, peers);
- the unsafe chain in memory with its fjall journal (D0);
- a **tail**: a fjall archive (the existing `FjallArchive`) of the committed blocks above the
  last sealed chunk. Promotion appends here as in `indexer`; blocks leave it once a sealed
  chunk listed in the manifest covers them. This is bounded by finality plus one chunk: hours
  of blocks.

An `R2Archive` implements the existing `ArchiveStore` trait, so `el` serving, the stream,
Flight and promotion are unchanged:
- reads at or below the manifest's last block go to R2;
- reads above go to the tail;
- writes (`append_batch`, `set_receipts`, `set_heads`) go to the tail only.

### 5.2 Reads

- **Streaming**: Flight and subscription catch-up own an archive range stream. Each stream
  owns its read-ahead task, and dropping it cancels remote reads. The read path is a pipeline: GET the chunk
  (or the part of it the range needs, by its segment offsets), decompress segment by segment, check (D5), hand the records out.
  - The next chunk is fetched while this one is served (D4's rule).
  - Read-ahead is bounded: 16 MiB of decoded blocks per reader, the next chunk's stream, and a
    global byte budget in memory.
  - Nothing is written to disk and nothing is kept after the reader moves on: this is
    read-ahead, not a cache.
- **A single block by number**: the chunk's index (one ranged GET, located by the manifest,
  unless it is among the 256 indexes read last), then ranged GETs of about 1 MiB from the
  block's segment on, at least two at once.
- **By hash**: the index (D9), then the same as by number.
- **Check on fetch** (D5) runs per segment as the bytes stream: its sha256 against the index,
  which is the manifest's, then that the frame decompresses and its records decode; headers
  are not hashed again.
- **Failures**: transient errors are retried (1.5); anything else reaches the client as
  `INTERNAL` ("the node cannot read its stores"), which clients do not fail over on.

### 5.3 What it advertises to p2p peers

eth/69 lets a node advertise one contiguous range `[earliest, latest]` (`BlockRangeUpdate`).

**D14.** A server advertises the whole sealed range plus its tail: it can serve any sealed block from R2. Peers' history requests are read from R2 on
demand, under the existing per-peer serving limits ([el §6](el.md#6-being-a-polite-peer)) and a
global R2 budget; a request beyond them gets an empty answer, which eth/69 allows. Pre-Bedrock
blocks stay served only to op-p2p-indexer peers (`opidx`, as today).

### 5.4 gRPC and Flight

Unchanged code over `R2Archive`:
- **Subscriptions** stream history from R2, then join the live window (the server follows
  the chain itself).
- **Flight `DoGet`** streams a ticket's range from R2. A ticket the balancer clipped to one
  chunk (section 6.4) is one chunk stream: its index, then about 1 MiB per GET, 2 to 12 in
  flight.

### 5.5 Implementation

- `crates/node` (`op-indexer-node`) holds the node's wiring, generic over the committed
  store: `indexer` runs it on its fjall archive, `server` on an `R2Archive`. A binary adds
  tasks of its own (`op_indexer_node::Task`).
- `crates/server` (`op-indexer-server`):
  - `ChunkSource`: what the server needs of the sealed chunks (listing, refresh, a chunk's
    blocks from a number, hash lookup, publish). `R2Chunks` implements it over the chunk
    store and holds the manifest; publishing rolls the hash-index generation every 50,000
    blocks.
  - `R2Archive<S: ChunkSource>`: the routing of 5.1, the tail (`<data dir>/tail`; the
    server refuses a data directory holding an indexer's `archive/`, which the pruning would
    empty) pruned with
    `FjallArchive::prune_below` as the manifest grows (read every 30 s), and an empty tail
    accepting only the block after the last sealed one.
  - Its `heads()` raise the finalized (and safe) head to the last sealed block: a chunk is
    sealed only once finalized, so blocks served from R2 report `FINALIZED` in `GetBlock`,
    subscriptions and Flight caps even with the L1 side off.
  - Read-ahead (5.2): each consumer owns a range stream that reads chunk after chunk ahead
    of it, at most 16 MiB per reader; dropping the reader cancels its producer. No shared
    next-block lookup, idle expiry, disk writes or chunk cache.
    The read budget (`server.read_budget_mb`, default an eighth of the memory,
    256 MiB to 16 GiB, [configuration.md](configuration.md)) bounds them all,
    whatever the number of readers: half for decoded blocks read ahead, half for the chunk
    streams open at once (26 MiB each, with the server's reads of about a segment, two
    in flight). Measured on a local chunk (2,000 OP Mainnet blocks), peak RSS for
    1/8/16/32/64 concurrent Flight `DoGet`s of the whole chunk: 100/420/717/790/878 MB with
    a 1 GiB budget, about 300 MB at 64 with 256 MiB.
  - Peers (5.3): a read that needs R2 takes one of 16 places and counts against 4 GiB a
    minute, for all peers together; without one the peer gets the empty answer.
    - Read in runs, never a GET per block: a downward header request (a peer walking the
      chain from a hash) is read upwards as one stream and answered from the top; bodies and
      receipts by hash try the block after the previous one before the hash index, so a
      run of hashes costs one index lookup; a stream continues into the next chunk; a
      spaced header request (a skeleton) answers at most 64 headers, read 16 at once, one
      segment each.
    - Each reads only what its answer needs (`ReadParts`): headers and bodies without the
      receipts, eth/69 receipts without rebuilding their blooms (the bloom is dropped for
      eth/69 anyway); eth/68 receipts whole. Measured on a local chunk: 256 headers 5 ms,
      256 bodies 2 ms, eth/69 receipts 7 ms, a skeleton page 3 ms of server time.
  - `Exporter` (section 4).
- `bin/server`: `server.chunks_dir` selects local chunks instead of R2; `--export` or
  `server.export = true` enables export. Shared `[r2]` settings hold object-store credentials.
  `server.id` defaults to the host name and also names the exporter. See
  [configuration.md](configuration.md) for the remaining TOML settings.
- API keys (D18): `<role>.stream.api_keys` on `indexer` and `server` alike, an interceptor on
  the gRPC and Flight services (`crates/api/src/auth.rs`); empty means no check.

### 5.6 The indexer's history from chunks (design, not built)

A single-user `indexer` could get its history from the same chunks instead of only from
peers by range sync. Design:
- **Where**: a task the `indexer` binary adds through `op_indexer_node::Task`, so `crates/node`
  stays free of R2; it reads through `ChunkSource`, which moves to `crates/chunks` with the
  reader so the indexer does not depend on `crates/server`. Enabled by the R2 variables of
  `server` with a read-only key.
- **Trust**: the archive's first block is one the node verified itself (gossip and range sync,
  anchored on L1). The backfill fills `[0, first − 1]` and is accepted only if the chunks
  link (D5) down from a chunk whose last hash is the first block's parent hash; chunks are
  checked as they stream, nothing is stored before its chunk is.
- **Writes**: below the archive's first block, newest chunk first, so every stored block
  hangs off one already verified. That needs a `FjallArchive` prepend (the bulk path writing
  below the first block, durable per chunk, resumable from the archive's first block); today
  the archive only grows upwards.
- **Gap**: blocks between the last sealed chunk and the archive's first come from range sync,
  as they do now.

### 5.7 Memory

What a `server` holds, each part bounded. Most bounds are sized from the machine's memory and
cores ([configuration.md](configuration.md)); the sum must stay well under the machine's
memory, leaving room for the system, the allocator's slack and the parts below that are not
counted exactly.

| Part | Bound | Set by |
|---|---|---|
| Unsafe chain | its blocks' encoded bytes, an eighth of the memory by default (256 MiB to 2 GiB) | `<role>.unsafe_max_bytes` |
| Reads of sealed history (feeds) | half the budget for decoded blocks read ahead, half for open chunk streams (their GETs and decoded ranges); an eighth of the memory by default (256 MiB to 16 GiB) | `server.read_budget_mb` |
| Flight builds (converting and encoding reads to Arrow) | two per core server-wide, within an eighth of the memory, each up to about 100 MiB until its messages are sent: 800 MiB on 4 cores, whatever the number of `DoGet`s | `<role>.stream.max_builds` (`crates/node/src/sizing.rs`) |
| Flight messages queued per `DoGet` | 4 × about 2 MiB, × the `DoGet`s at once (as many as the builds by default, at least 8; 16: 128 MiB) | `<role>.stream.max_flights` |
| Subscriptions catching up | one history batch each (64 blocks or 16 MiB) and their messages, × the subscriptions | `<role>.stream.max_subscriptions` |
| Exporter (one server per deployment) | the chunk being sealed (256 MiB of records, about 40 MB compressed) and one read of 64 MiB | `crates/chunks` (`CHUNK_BYTES`), `crates/server/src/export.rs` |
| fjall tail and the unsafe chain's journal | caches of 64 and 8 MiB, memtables of at most 7 × 16 and 8 MiB | `crates/storage` |
| Peer reads from R2 | `MAX_PEER_READS` reads in flight, each one answer | `crates/server/src/budget.rs` |
| Chunk indexes read lately | 256 parsed indexes, 32 bytes a block: about 115 KiB for a 256 MiB OP chunk (some 30 MiB), up to about 3 MiB for a 100,000-block legacy chunk (up to about 800 MiB) | `crates/chunks/src/store.rs` (`MAX_CACHED_INDEXES`) |

A Flight build takes a place among the server's builds before it starts and keeps it until its
messages are sent; a stream without a free place sends what it has built first, so streams
never wait on each other's places.

### 5.8 Flight throughput: where a server's CPU goes (2026-10-05)

Profiled under 8 `DoGet`s of `blocks`, a server's CPU went to the sha256 of each segment read
(about half), zstd's decompression (a quarter), copies through a small buffer (an eighth) and
the frame's own checksum; Arrow building and IPC encoding a few per cent. Since then segments
are hashed with the CPU's SHA instructions (sha2 0.11, on aarch64 too), decompressed straight
into a buffer sized from the frame with one context per thread, and the frame's checksum is
skipped once the sha256 has passed: CPU per Arrow GB, blocks 481 → 114 s, transactions 8.7 →
3.7, receipts 136 → 83, logs 6.8 → 4.5 (local). A per-chunk job also reads the chunk's head
while its index arrives, and the last 256 indexes are kept, so its first batch comes after one
round trip instead of two. These landed after v0.1.8 and have not been benched on R2
(section 7).

## 6. The balancer

### 6.1 Table

Kept in memory and rebuilt from registrations after a restart. Per server, from its last
heartbeat:
- id, chain, and the `host:port` of its gRPC and Flight listener;
- health: an unhealthy server gets no work;
- heads: unsafe, safe, finalized, and the last sealed block it has read from the manifest;
- contiguity (`contiguous_through`): the highest block N such that it holds every block from
  the chain's first through N, each with its receipts. A server holds the sealed chunks (R2),
  its own tail (committed blocks above them; filled by range sync, `sync = true` in the role's `[el]` table)
  and its own unsafe chain (gossip since it started). Without range sync there is a gap
  between the last sealed block and the first block it gossiped: its head is above the gap
  but it cannot serve it. The server computes it at each heartbeat, cheaply, as the range
  it advertises to execution peers (`NodeView::contiguous_through`, `NodeProvider::range`):
  its committed store's blocks (contiguous by construction: every append links to the one
  before) up to the first still waiting for receipts, extended through its unsafe chain's
  canonical blocks that link to them and have theirs, each search continuing from the last;
  never below the last sealed block;
- load: requests in flight (subscriptions, Flight streams, lookups) and bytes sent per second
  (the stream's responses as encoded for the wire: subscription events, lookups and Flight
  data, counted with one atomic add each as they leave; the server turns the count into a
  rate over each heartbeat interval);
- stream limits: Flight `DoGet` streams and subscriptions at once (`max_flights`,
  `max_subscriptions`) and how many of each are taken now. A server with none free makes a
  `DoGet` wait up to `<role>.stream.flight_queue_ms` (2 s) for a place, then refuses
  with `RESOURCE_EXHAUSTED` ("too many Flight streams at once", "too many subscriptions"), so
  the picker counts them (6.4, 6.5). Unset from an older server, which is then taken to have
  room;
- peers: consensus gossip peers connected (what unsafe blocks arrive from); sessions with the
  chain's execution peers, in all and inbound (receipts, gap fill, range sync), unset without
  the execution network; sessions with L1 execution peers and the beacon light client's
  peers, unset without the L1 side. Read from what the node's networks publish as they run
  (`NodeView::peers`), nothing polled. For the status log only (6.3): routing does not use
  them;
- served in the last minute: blocks relayed on the consensus gossip (`blocks_forwarded`) and
  served there by number (`payloads_served`); on the chain's execution network the requests
  answered with blocks (headers, bodies, receipts), the items sent and the peers served,
  unset without the execution network. Each network counts as it serves and its own
  per-minute line (`consensus peers`, `serving execution peers`) publishes the minute
  (`NodeView::served`); [citizenship.md](citizenship.md), "Seeing the duties done", ties
  each count to the duty it shows. For the status log only.

The table holds no chunk ranges: every server is stateless
and reads the same bucket, so every server serves every sealed chunk. The balancer reads the
manifest itself, once per refresh (every 30 s), only for the chunk boundaries.

### 6.2 Registration

A server opens a `Register` stream to the balancer and sends a heartbeat every 5 s with its
health, heads, contiguity, load, stream limits, peers and what it served. Three missed
heartbeats (15 s) mark it down and remove it.

### 6.3 Health and failover

- A server is down after 3 missed heartbeats, and out of the table as soon as its `Register`
  call ends.
- Work above the sealed chunks goes only to a server that holds every block through it: its
  head covers the work and so does its `contiguous_through` (6.4, 6.5). A server that falls
  behind, or has a gap above the sealed chunks, gets none there, but still serves sealed
  chunks. A server that reports no `contiguous_through` gets no work above them.
- Failover is on the client side: every answer names more than one server when there are
  (a Flight endpoint up to three locations, `Locate` every server that can take the
  subscription), in the order to try them. A client moves to the next on `UNAVAILABLE` (the
  server is down or shutting down) or `RESOURCE_EXHAUSTED` (its limit is reached, or the
  client read too slowly); a Flight job resumes as the same ticket, a subscription from the
  last block received.
- Every 30 s the balancer logs one `server status` line per server: its heads,
  `contiguous_through` and how far that is behind the newest head any server has, requests in
  flight, bytes per second, Flight streams and subscriptions as `taken/max`, its peers, and
  what it served in the last minute (blocks forwarded, payloads served, execution requests,
  items and peers served). It
  is a warning when the server is unhealthy, more than 64 blocks behind, or has had no consensus
  peer or no execution session for more than a minute: gossip, or receipts and gap fill, have
  stalled. `no server registered` is a warning too.

### 6.4 Flight: per-chunk jobs

**D16.** The balancer implements `GetFlightInfo` and `ListFlights` only, and turns a range
into per-chunk jobs, so one big range runs in parallel over the servers:
- One `FlightEndpoint` per chunk the range touches, its ticket clipped to the chunk
  (`table:first:last:cap`, the stream's existing ticket); every healthy server can take it.
- The part above the last sealed chunk becomes jobs only a server can take whose head (under
  the ticket's cap) and `contiguous_through` both reach the job's last block.
- Every job is also cut to the servers' `DoGet` limit (100,000 blocks).
- Each job goes to the least loaded server that can take it, and its `location` lists up to
  three, least loaded first. A server whose free Flight places are used up (those it reported,
  less the jobs this plan already gave it) would refuse, so it comes after every server with
  room: first only when all are full, and otherwise a location to try next. The load counts the
  jobs already given out for the same range, so a big range spreads over every server while one
  busy with other requests gets fewer; ties go round-robin.
- A Flight client fetches the endpoints in parallel, straight from the servers, and moves to
  the next location if one fails. It may ask each server for compressed record batches
  (`op-indexer-compression: lz4` or `zstd`, [stream.md](stream.md) §6).
- The ticket code is `crates/api/src/ticket.rs` (`op_indexer_api::ticket`).

### 6.5 Locate

**D17.** `Locate(chain, from_block) → [server endpoints]` for gRPC subscriptions: the healthy
servers whose `contiguous_through` reaches the block before `from_block` (they hold every
block up to it), those with a free subscription place first, then least loaded first (a full
one after them, as in 6.4). The client
subscribes to the first and, on `UNAVAILABLE` or `RESOURCE_EXHAUSTED`, resubscribes from its
last block at the next.
Subscriptions already resume by number.

### 6.6 Cost of reads

R2 Standard, from Cloudflare's pricing page (read 2026-10-04):
- storage $0.015 per GB-month;
- Class A (PUT, LIST) $4.50 per million;
- Class B (GET, HEAD) $0.36 per million;
- egress free.

A ranged GET is a GET; the page does not say otherwise.

- **Streaming**: as built, a chunk stream of the server reads about a segment (1 MiB) per GET
  (`StreamReads::range_bytes`), two GETs always in flight and up to twelve while the read
  budget lends room (`crates/server/src/feed.rs`): a GET takes 100 to 200 ms, so a stream's
  speed is its bytes in flight per round trip. Each GET runs on its own task, and at most two
  ranges are decoded at once, in order, so what a stream holds stays small however many GETs
  it has out. So 1 TB served (compressed bytes read from R2) is about 1 million GETs, about
  **$0.36 per TB**. Near a chunk's end (its last 128 MiB decoded) the feed opens the next
  chunk's stream, if the budget has a place to spare, so its index and first ranges are read
  before the boundary. Measured locally with 150 ms added to every GET: one stream 247 MB/s
  decoded (about 35 MB/s compressed), 8 streams 177, 32 streams 78 (10 cores, CPU-bound);
  peak RSS stays the budget plus about 100 MB.
- **Random single blocks**: the index GET (unless cached) and two ranged GETs, about $1 per
  million blocks; by hash, two or three more.
- **Storage**: OP Mainnet's ~500 GB of chunks is about $7.50 a month, plus about $0.04 a month
  for each 2.5 GB index generation kept (old ones are not deleted).
- **Droplet egress** to the users is the real cost of serving, not R2.

**D18. API keys.** The balancer and the servers accept a request only with a key from their
configured list, sent as gRPC metadata (`authorization: Bearer <key>`). That covers gRPC,
Flight `GetFlightInfo` and Flight `DoGet`, which go straight to a server with the same key.
Servers register with the balancer using a server key of their own. Keys come from private TOML settings and are never logged. TLS is not part of this design.

### 6.7 As built

- `crates/balancer` (`op-indexer-balancer`): the `Balancer` component, its proto
  (`proto/opindexer/balancer/v1/balancer.proto`, package `opindexer.balancer.v1`) and the
  server's registration client (`register`). `bin/balancer`: the binary over it.
- **Registration** (6.2, 6.3): a bidirectional stream; the balancer answers the first
  heartbeat with `Registered`. A 15 s gap between heartbeats, a heartbeat of another chain or
  id, or a newer registration of the same id ends the call, and the server's entry with it.
  The client (`register::Registration::run`, wired into `server` by
  `server.balancer_url`) heartbeats every 5 s from a `watch` of the server's `Report`,
  and registers again after a backoff of 1 s doubling to 30 s with jitter. Only a bad
  configuration stops the server at startup (an invalid balancer URL or key, an invalid
  `server.address`, or none when the execution network is off). A server's address must be exactly `host:port` (`register::is_valid_address`).
- **Picking** (6.4, 6.5): servers with a free place of the kind the request takes (Flight or
  subscription; the jobs given out for the same request count against it) first, then by
  requests in flight plus those jobs, then bytes per second, ties round-robin
  (`table::Picker::pick`). Above the sealed chunks a server serves up to its
  reach, min(head under the cap, `contiguous_through`) (`table::Server::reach`): Flight jobs
  by the ticket's cap, `Locate` by the unsafe head. The heads and `contiguous_through` are the
  heartbeats'; no separate `GetHeads` probe.
- **Flight** (D16): the other Flight calls are `UNIMPLEMENTED` (no `GetSchema`: the schema is
  in each `FlightInfo`). A range is clipped by the best reach among the servers (the head of
  its cap, but no further than the server's `contiguous_through`), and may not start below the
  first sealed chunk (`OUT_OF_RANGE`). `ordered` is set.
- **Keys** (D18): `Locate` and Flight take a user key from `<role>.stream.api_keys`, the
  servers' own list; `Register` takes a server key from `balancer.server_keys`,
  which is required. Checked per call (`op_indexer_api::ApiKeys`: an interceptor on Flight,
`verify` in `Register` and `Locate`), never logged.
- **Load reporting**: servers report measured `bytes_per_second` from the change in encoded
  response bytes over each heartbeat interval. CPU is not part of the load score.

### 6.8 Raw chunk download (2026-10-05)

A client that wants whole history (a backfill, a mirror) can skip the servers and download the
sealed chunks straight from R2: no server CPU or egress, and R2 egress is free.

- **Plan.** `GetFlightInfo` on the balancer with the command `raw:from:to` (or the path `raw`
  for every sealed chunk) answers one `FlightEndpoint` per sealed chunk the range touches, at
  most 1,024 per plan (well under gRPC's 4 MB message limit; ask again from the block after the
  last for more). Each endpoint's location is a **presigned GET URL** of the chunk's object,
  good for 10 minutes; its app metadata is the chunk's manifest entry as JSON (`first`, `last`,
  `first_parent`, `last_hash`, `sha256` the root, `size`, `footer_offset`, `level`); its
  ticket `raw:first:last` is a label. `total_bytes` is the plan's size. Same user key as any
  Flight call. The part above the last sealed chunk is not in a raw plan: read it from a server.
- **Signing.** `ChunkSigner` (`crates/chunks/src/raw.rs`) signs with object_store's S3 signer,
  locally, no request made. The URL carries the key's id, never its secret. The key is the
  balancer's `r2.presign_access_key_id` / `r2.presign_secret_access_key`:
  give it a **read-only** R2 token (a URL is good for whoever holds it until it expires).
  Without them the balancer refuses raw plans (`FAILED_PRECONDITION`).
- **Client.** `import fetch --balancer <url> --from <n> --to <n> [--out fetched]
  [--downloads 8] [--api-key …] [--chain <id>]` asks for the plan, downloads the chunks in parallel (in
  order of use, `--downloads` at once), checks each and writes `<out>/<first>-<last>.rlp`: the
  range's blocks of the chunk, each an RLP list of its header, body and receipts (each as the
  eth protocol carries it). A file is written whole (through a temporary one); one already
  there is kept, so a stopped fetch goes on. A refused GET (403: the URL expired) asks the
  balancer for a new plan from that chunk on. Past the last sealed chunk it stops with a
  warning (the rest is a server's to stream). No state directory, archive service or R2 key.
- **Checks** (`decode_chunk`, then the importer): the object's size and footer against the
  entry, the index frame against the root, every segment against the index, every header
  against its hash, the parent links from the entry's first parent through its last hash;
  every block's transactions root and receipts root against its header; between chunks each
  first parent against the last hash before it. The entries are the balancer's, trusted as a
  server is for what it streams (it read them from the hash-chained manifest).
- Arrow IPC output is not built: for columns, use the servers' Flight `DoGet`.

### 6.9 Optional Cloudflare cache in front of R2 (2026-10-05)

R2 GETs cost $0.36 per million and every server read goes to R2. With
`r2.public_url` set (a custom domain on the bucket, behind Cloudflare's cache, e.g.
`https://chunks.example.com`), the servers read **sealed chunk** ranges from it over plain
HTTPS (`https://<domain>/<prefix>/chunks/…`, ranged GETs; Cloudflare serves ranges from the
cached object), and a cache hit costs no R2 operation. On any error (after one quick retry)
the read goes through the S3 API as before, logged at debug. The manifest and the hash index
are always read through the S3 API: the manifest changes, and the index is small.

Chunks are immutable (their names carry their root) and checked on read (1.5), so a cache can
only serve the right bytes or bytes that fail the check.

Setup, in the Cloudflare dashboard:
1. R2 → the chain's bucket → Settings → Custom Domains: connect a domain of a zone on the
   account (e.g. `chunks.example.com`). That is the bucket's only public access: leave the
   `r2.dev` subdomain disabled.
2. Only chunks should be public. Add a WAF custom rule on that hostname that blocks every
   path but the chunks: `http.host eq "chunks.example.com" and not starts_with(http.request.uri.path, "/archive/chunks/")` → Block.
3. Caching → Cache Rules: on that hostname with path starting `/archive/chunks/`, eligible
   for cache, edge TTL a long fixed time (e.g. a month; chunks never change), browser TTL
   respected or short. Objects above the plan's cacheable size limit (512 MB on Free/Pro) are
   not cached; a chunk is about 40 MB.
4. Set `public_url = "https://chunks.example.com"` in `[r2]` on the servers, the exporter
   included (it reads chunk indexes through it too). The importer and the balancer ignore it.

The public domain makes the chunks world-readable. They are the chain's public history, and
the raw download (6.8) hands them out anyway; the manifest and index stay private.

## 7. The bench (3 to 4 small droplets, one R2 bucket)

**Run 2026-10-05, v0.1.8**: three servers and a balancer over the Unichain bucket
(`unichain-snapshot/archive`), read from a client off the servers' network: `blocks` 129.6
MB/s on 48 streams (about 43 MB/s a server), `transactions` 406 MB/s, `logs` with lz4
329 MB/s. v0.1.8 predates the CPU changes of 5.8.

**Native client.** The release now includes `bench`, a Rust client with `--concurrency`,
`--repeat`, per-job JSON reports and `--heavy` for sequential blocks/transactions/receipts/logs
with longer read budgets. See [native benchmark usage](bench.md). Keep its performance series
separate from Python; both clients validate plan coverage and count logical decoded bytes.

**Python client.** `scripts/bench.py` reads a range of one table through the balancer, as a
Flight client would: it asks the balancer to plan the range (`GetFlightInfo`), then runs the
jobs across `--processes` processes with `--threads` threads each (default 4), so one Python
process is not the limit. `--per-server` (default 8) limits concurrent reads to each server
across all processes and threads of this run, including fallback attempts. Extra work waits
locally; a free fallback can take a job while its first server is busy. This is a client limit,
not discovered server capacity or a reservation against other clients. Set it no higher than
the smallest participating server's available Flight capacity, leaving room for other users.
Effective server limits are logged at startup; they are sized from the machine unless
overridden ([configuration.md](configuration.md)).

A job tries its locations in turn: on `UNAVAILABLE` or a read timeout it moves to the next;
after a round in which one answered `RESOURCE_EXHAUSTED` it backs off, 200 ms doubling to 5 s
with jitter. `--retry-for` (default 120 seconds) bounds a started job, including local slot
waits and retries. Jobs still in the work queue have not started that budget.
`--rpc-timeout` (default 120 seconds) bounds each complete read RPC, shortened to the job's
remaining budget; raise it for legitimately longer streams. `--plan-timeout` (default 30
seconds) bounds planning. A worker process crash aborts the run, since its shared permits
cannot safely be recovered. The client checks that plan tickets cover exactly the requested
range with no gaps or overlaps; a head-clipped plan fails before any reads start.

This comparison client needs PyArrow and Python 3.11 or newer (or `tomli` on older Python).
It reads `bench.api_key` and `bench.balancer_url` from `--config PATH`, defaulting to
`config.toml`. Native benchmark usage is in [bench.md](bench.md). Release archives include the script as
`bench.py`:

```bash
python3 scripts/bench.py --config /path/to/config.toml --balancer grpc://balancer.example:50060 --table blocks --from 40000000 --to 45000000 --processes 4 --threads 6 --per-server 8
```

Replace `balancer.example` with the deployment's balancer and put its client key in the
private TOML configuration.

**What the numbers mean.** Progress counts decoded Arrow bytes as batches arrive, including
bytes from attempts that subsequently fail. Final useful MB/s and rows/s count only
successful jobs; failed-attempt decoded bytes are reported separately. Neither is wire or
R2 traffic. Failed attempts are counted by status (exhausted, unavailable, timeout, other),
and an expired job reports its last error. An incomplete run exits unsuccessfully and labels
its throughput as a successful subset, not a full-range result. First-batch time and job latency start when work is enqueued,
so they include client startup, queueing, slot waits and retries before the first batch.
These times are observed in the parent and include inter-process message delivery. Empty
streams have no first-batch sample. Planning time is separate and included in the end-to-end
useful rate.
Per-stream rates describe successful attempts only. Compared with older script versions,
TTFB and progress therefore have different meanings.

**Local client to the live Unichain fleet, 2026-10-05.** These are checks of the new
benchmark from a macOS client (Python 3.9, PyArrow 21), not the earlier importer-hosted
benchmark. Server revision, resource utilization and cache state were not verified. Times
below are the reported read phase, including client startup and completion, excluding
planning (about 0.32–0.36 seconds). All runs completed with zero failed jobs and zero retries.

| Blocks, inclusive | Compression | Processes × threads | Per-server cap | Runs | Read seconds | Decoded MB/s |
|---|---|---|---|---|---|---|
| 50,000,000–50,300,000 | none | 2 × 4 | 4 | 2 | 32.2–55.4 | 3.6–6.1 |
| 50,000,000–50,300,000 | lz4 | 2 × 4 | 4 | 1 | 18.4 | 10.7 |
| 50,000,000–50,300,000 | zstd | 2 × 4 | 4 | 1 | 17.5 | 11.3 |
| 40,000,000–41,000,000 | zstd | 4 × 6 | 4 | 2 | 13.8–17.5 | 37.7–47.5 |
| 40,000,000–41,000,000 | zstd | 4 × 6 | 8 | 2 | 17.4–17.7 | 37.3–37.9 |

The small range returned 300,001 rows and 197.4 MB in every run; the large range returned
1,000,001 rows and 658.1 MB across 33 jobs. The large-range cap order was 4, 8, 8, 4. Compression
helped in these observations; doubling the cap did not consistently improve completion time.
The samples and uncontrolled network/cache conditions do not establish a fleet throughput
ceiling or a universal compression/concurrency default.

A logs check over 48,000,000–48,100,000 with Zstd returned 968,312 rows (314.3 decoded MB)
in 11.4 seconds with no failures or retries. Transactions over the same range, with a
90-second RPC deadline and 120-second job budget, completed only one of three jobs: four
timeout attempts consumed 830.4 MB of failed/incomplete decoded data. That run is invalid
as a throughput comparison. Deadlines must accommodate the client's transfer rate; short
deadlines can turn a slow but healthy stream into repeated work. The script's default read
deadline is 120 seconds; longer streams need both `--rpc-timeout` and `--retry-for` raised.
A smaller transaction check over 48,000,000–48,020,000, with a 120-second RPC deadline and
150-second job budget, completed its one job in 60.6 seconds: 179,890 rows, 158.6 decoded MB,
no failures or retries. This is a single-stream transfer check, not a fleet saturation run.

**Importer-hosted client, user-reported 2026-10-05.** On `op-ingestor`, `scripts/bench.py`
read finalized blocks 40,000,000–45,000,000 through the same three-server fleet, with
four processes, six threads each, Zstd, a 120-second RPC deadline and a 300-second job
budget. Both runs returned all 130 jobs, 5,000,001 rows and 3,290.6 decoded MB, with zero
failed jobs, retries or failed-attempt bytes. Server/client revisions, resource usage and
cache state were not captured in the supplied output; these are observations, not a
controlled before/after comparison with earlier runs.

| Per-server cap (run order) | Read seconds | End-to-end seconds | Decoded MB/s | End-to-end useful MB/s | Queue p95 seconds | Job latency p95 seconds | Aggregate slot/backoff wait seconds |
|---|---|---|---|---|---|---|---|
| 4 (first) | 29.0 | 29.4 | 113.3 | 112.0 | 22.084 | 27.864 | 299.5 |
| 8 (second) | 23.3 | 23.7 | 141.3 | 139.0 | 18.942 | 22.672 | 0.0 |

Cap 8 improved observed end-to-end throughput by 24.1% and reduced end-to-end time by
19.4%. Aggregate slot wait sums across workers, so it can exceed wall time. Zero slot wait
does not mean zero work-queue delay. Per-server throughput increased from 36.1–39.5 to
43.6–49.4 decoded MB/s, while per-stream throughput fell from 9.6–10.1 to 5.9–6.5 MB/s.
More overlapping reads helped this run, but do not identify whether CPU, R2, client or
network limits dominate. Repeat both caps in alternating order before choosing a default.

The immediate repeat sequence is five runs per cap, alternating `8,4` then `4,8` pairs,
on this same host, table, range, compression and worker count. Preserve each full output
and exit status; compare only complete runs, reporting median and range of end-to-end
throughput and any failures separately. Capture revisions, limits and resource usage first
as below. Do not increase admission above 8 until available server capacity is verified.

**Repeated macOS comparison, 2026-10-05.** The same five-million-block workload above
was subsequently run from macOS 26.6.2 arm64, Python 3.9.6 and PyArrow 21.0.0, using
`scripts/bench.py` at commit `8a2bcdb`. All settings matched the importer-hosted run.
Ten sequential runs used caps `8,4,4,8,8,4,4,8,8,4` (five per cap). Every run completed
130 jobs, returned 5,000,001 rows and 3,290.6 decoded MB, and exited 0 with no failures,
retries or failed/incomplete bytes. Full [run output](benchmarks/2026-10-05-macos-blocks.txt)
and [revision, script hash and per-run results](benchmarks/2026-10-05-macos-blocks.json)
are preserved without credentials.

| Per-server cap | End-to-end useful MB/s, in sample order | Median MB/s | Median end-to-end seconds |
|---|---|---|---|
| 4 | 28.2, 28.1, 27.8, 26.4, 27.0 | 27.8 | 118.3 |
| 8 | 43.3, 44.5, 44.0, 46.4, 43.5 | 44.0 | 74.7 |

Cap 8's median throughput was 58.3% higher, and median completion time 36.9% shorter,
than cap 4's on this client. All cap-8 runs reported zero local slot/backoff waiting;
cap-4 runs reported 963.2–1,126.8 aggregate worker-seconds of waiting. Both still queued
jobs behind busy workers. These repeats support retaining cap 8 for this workload;
they do not establish a server capacity limit or justify raising admission further.
Server revision, available capacity, utilization, competing traffic and index-cache state
remain unverified. No nodes were restarted. The same range was repeatedly read, without
controlled cold/warm classification. Client/network conditions differ from `op-ingestor`,
so the rates must not be pooled or used as evidence of a server regression. Repeated
importer-hosted measurements with server telemetry remain outstanding.

**Next measurements, after the CPU changes of 5.8:**

1. Record the server commit, benchmark revision, effective server limits, importer activity
   and CPU/RAM/network usage on the client and all three servers. Keep the range and table
   fixed. Check that all requested jobs succeed before comparing rates.
2. Keep four client processes and sweep 3, 6, 9 and 12 threads (12/24/36/48 workers), repeating
   each configuration five times. With `--per-server 8`, three servers admit at most 24 reads
   from this run; more workers test queueing, not higher server concurrency. Raise the cap
   only after checking available server capacity and resource usage.
3. Compare `--compression none`, `lz4` and `zstd` separately for each table. Measure NIC bytes
   alongside decoded output and CPU. Separate cold-index-cache and warmed runs; do not restart
   live nodes merely to clear caches.
4. Compare one, two and three serving nodes using equivalent covered ranges. A second client
   with disjoint ranges helps investigate an importer/client bottleneck, but improved aggregate
   throughput alone can also reflect more server concurrency or different cache behavior.

Larger Flight batches, work-weighted job scheduling and R2 read tuning remain experiments.
Measure batch sizes, per-job durations and GET latency/bytes first; existing adaptive reads,
prefetch and index caching should not be duplicated. No before/after fleet speedup attributable
to the new scheduler has been established.

**Not measured yet**: the exporter's lag from finalization to a listed chunk; failover after
killing a server mid-`DoGet` and mid-subscription; subscription catch-up from R2; R2
operations and stored GB per day against 6.6's estimate; level 1 against level 3 on the
droplets (D3); the chunk size target (D4).
