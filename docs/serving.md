# Serving at scale: chunks, `server` and `balancer`

Status: **built, except the balancer.** The storage core (`crates/chunks`, sections 1, 2 and
2.4) and the converter (`import export`, section 3) are built and checked end to end against a
local directory. The stateless `server` (`crates/server`, `bin/server`: history from R2 with no
cache, section 5; the exporter, `server --export`, section 4) is built and not yet run against
R2 (5.5). The `balancer` is in progress; the bench comes next. Decision: [roadmap.md](roadmap.md),
2026-10-04, "Four binaries". Decisions are marked **D**; the user's answers of 2026-10-04 settled
the open questions (they are recorded in the decisions). Measurements are from 2026-10-04.

Four binaries (`op-indexer` is renamed `indexer`; the importer keeps its `import` command):

| Binary | What | Data |
|---|---|---|
| `indexer` | Today's node: one user, every service in one process | fjall archive, unsafe chain in memory + fjall journal |
| `server` | A full node (p2p layers, its own unsafe chain, serving peers, gRPC and Flight) that holds no history: it reads sealed chunks from R2 on demand and streams them; one server per deployment also exports (section 4) | a small fjall tail of unsealed committed blocks, unsafe chain in memory + fjall journal; no chunk cache |
| `importer` (`import`) | Today's importer, plus the one-time converter of `verified/` to chunks (section 3) | state directory, R2 (write, during the conversion) |
| `balancer` | The single entry point: keeps the servers' health and load, splits each request into per-chunk jobs and spreads them over the servers; no block data passes through it | an in-memory table |

R2 holds only sealed, immutable history. Live and recent data never go through R2: every
server follows the chain itself, as `indexer` does.

**The flow** (user, 2026-10-04): balancer → distributes jobs → server → reads the chunk from R2
and serves it. Servers are stateless for history, so scaling out is a matter of network, not
storage: any server can serve any chunk, and adding one adds R2 read bandwidth and nothing
to fill.

**D0. No Redis.** The unsafe chain (gossiped blocks not yet committed, with fork choice) lives
in memory in the node process, `indexer` and `server` alike, and every change to it is
journaled to a local fjall keyspace, so a restart replays it without the network. Nothing
needs a separate service: no Redis in the binaries or in the docs. (This
replaced the Redis unsafe store of the `storage` crate: `MemoryStore`, `docs/storage.md`
section 3.)

## 1. The chunk

A chunk is an immutable object holding a contiguous range of committed blocks of one chain,
every one of them finalized on L1 and with its receipts.

### 1.1 Contents

**D1.** Per block, the bytes that are already verified and that the archive and peers use:
- the block hash;
- header and body in their consensus encoding (RLP);
- the receipts in their consensus encoding **without blooms** (eth/69's receipt form);
- the senders (20 bytes per transaction).

This is the importer's verified record (`bin/op-indexer-import/src/chunk.rs`, `ArchivedBlock`)
with the blooms left out, so `chunk.rs`'s writer and reader, `PreparedBlock` and the archive's
encodings are reused. A receipt's bloom is recomputed from its logs when something needs it:
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
segment  = one zstd frame (level 1, with its checksum) of up to ~1 MiB of block records
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

Compression on that sample, 146.9 MB of records (blooms still in), zstd 1.5.7's in-memory
benchmark (`zstd -b`), one thread, Apple M-series:

| | size | ratio | compress | decompress |
|---|---|---|---|---|
| none | 146.9 MB | 1× | | |
| `--fast=5` | 26.1 MB | 5.6× | 1.8 GB/s | 4.5 GB/s |
| `--fast=1` | 23.1 MB | 6.4× | 1.6 GB/s | 4.4 GB/s |
| **level 1** | **21.0 MB** | **7.0×** | **1.2 GB/s** | **3.4 GB/s** |
| level 1, 1 MiB segments | 22.3 MB | 6.6× | 1.1 GB/s | 3.3 GB/s |
| level 3 | 17.7 MB | 8.3× | 0.9 GB/s | 4.2 GB/s |
| level 9 (`zstd -9`, 100-block files) | 16.8 MB | 8.7× | about 0.2 GB/s | |
| level 19 (one 2,000-block file) | 15.0 MB | 9.8× | about 4 MB/s | |

Bigger chunks gain little: 1.5 % at level 3, 6 % at level 19.

**D3. Light compression: zstd level 1, in 1 MiB segments** (6.6×). The user weighs CPU above R2
storage.
- Reading a 256 MiB chunk costs about 80 ms of one core.
- Moving it uncompressed would put 6.6 times the bytes through R2, the network and the page
  cache, which costs far more than that.
- `--fast` levels save little CPU for 10 to 20 % more bytes.
- Decompression speed hardly depends on the level: level 3 reads as fast as level 1 here and
  is 16 % smaller. Only sealing costs more at level 3, once per chunk. The format reads any
  level, so this can change per chunk without touching readers.

### 1.3 Size of a chunk

**D4.** Variable ranges cut by a deterministic rule:
- a chunk ends at the first block that brings its uncompressed records to the size target, or
  at **100,000 blocks**, or at the chain's Bedrock block, whichever comes first;
- a chain's boundaries are then the same whoever cuts them (section 4, idempotence).

The manifest (section 2) lists every chunk's range, so a block's chunk is a binary search, not
arithmetic.

**What sizes a chunk**: with no cache, every read comes from R2, so the size follows R2's
time to first byte (TTFB) and its streaming throughput, not cache efficiency.
- Large enough that a chunk's GET is mostly transfer, not TTFB, and that each Class B
  operation buys many bytes (section 6.6).
- Small enough to be one Flight job (one endpoint) a server streams in seconds, so the
  balancer can spread a range over many servers.

The read-ahead keeps the old rule: a server reads the next chunk while it serves this one, so

> time to serve a chunk to a sequential reader ≥ time to fetch the next chunk from R2
> (TTFB + size / throughput).

On a sequential read (a Flight range, a subscription catching up, an eth range sync) the
server starts the next GET as soon as the reader enters a chunk. A chunk too small for the
rule leaves the reader waiting on TTFB.

Start at **256 MiB uncompressed**: about 3,600 OP Mainnet blocks and about 39 MB at level 1,
about 0.4 s at 100 MB/s plus TTFB. Settle the number in the bench (section 7). Legacy blocks
(0.5 KB) give 100,000-block chunks of a few MB.

### 1.4 Whole-chain size

OP Mainnet: the fjall archive is 914 GB (values snappy-compressed one by one). The importer's
`verified/` (zstd 3, 100-block chunks) was not measured whole: about 0.68 of the 589 GB
downloaded on this sample, so about 400 GB. At level 1 in segments the chunks would be about
500 GB (8.3 / 6.6 times the level-3 size, a little less without blooms), about 50,000 objects
at 256 MiB uncompressed each (estimate).

Base: about 2 to 3.5 TB of archive (`docs/base.md` section 6), so roughly a TB of chunks.

### 1.5 Integrity: how a server checks a fetched chunk

**D5.** Three checks, no root recomputation (CPU is the dearer resource). A chunk that fails
any of them is discarded and refetched once, then reported:
1. **the chunk's root**: the index frame hashes to the manifest's sha256, and every segment
   read hashes to the index's sha256 for it before it is decompressed (and zstd's frame
   checksum holds): transport or storage damage, or tampering, before a byte is used. Built:
   one flipped byte fails "a segment does not hash to the index's sha256";
2. **the manifest chain**: the segments' hash chain, the chunk's first parent hash and last
   hash equal to the manifest's, and its first parent equal to the previous chunk's last hash.
   The chain of chunks ends at a block the server verified itself (its own committed head,
   through the `l1` crate), so one chunk cannot be swapped without breaking the hashes back to
   a block L1 committed. This is the importer's anchor argument, applied continuously;
3. **header hashes and parent links**: every header hashes to its record's hash and names the
   previous block as its parent.

Transactions and receipts roots are not recomputed on fetch: the header hash binds them, and
they were recomputed when the chunk was sealed (`verify`, or the node's promotion check).
Senders were proven at seal time (recovered, or hashed for deposits) and are not recovered
again.

## 2. R2 layout and the manifest

### 2.1 Layout

```text
<bucket>/<chain id>-<genesis hash, 8 hex>/
    chunks/<first:012>-<last:012>-<root, 16 hex>.opxc       immutable
    manifest/<sequence:010>.json                            immutable, append-only
    index/<generation:06>/<shard:03x>.idx                   immutable (section 2.4)
```

As built: OP Mainnet's prefix is `10-7ca38a19`.

**D6.**
- The prefix carries the genesis hash, so a bucket can never mix two chains with the same id.
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
  - the sha256 of the previous segment, so segments form a chain;
  - its exporter's id and the time.
- Segment 0 starts at the chain's first chunk.

A reader lists `manifest/` (R2 listings are strongly consistent), reads the segments after the
last one it had, and checks their chain.

### 2.3 Appending atomically

**D8.** One writer per chain (the exporting server, D11, or the converter while it runs, never
both). A chunk becomes visible only after its object is complete:
1. Upload the chunk object (multipart for large ones; R2 has no partial objects).
2. Write the next manifest segment.

With one writer nothing races, so neither step needs a conditional write. Where R2 honours
`If-None-Match: *` on PUT, both writes send it as a guard against a second writer started by
mistake; whether R2 does is tested in the bench. No object is ever modified. A chunk uploaded
but never listed is garbage, removed by a sweep of objects older than a day that no segment
names.

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
- **Generations**: shards are never modified.
  - The exporter writes a new full generation every 50,000 blocks (`server --export`), and
    the converter writes the first. Names carry the generation number:
    `index/<gen>/<prefix:03x>.idx`.
  - A rewrite is 2.5 GB and 4,096 PUTs, about $0.02.
  - Between generations the hashes of the chunks sealed since the last generation are in those
    chunks' indexes (section 1.2). That is at most about 14 chunks at OP Mainnet's rate, read
    with one ranged GET each, newest first, only when the generation's shard misses.
- **Recent blocks** (the unsealed tail and the unsafe chain) are found locally, as today.

This keeps every object immutable and needs no index service. A separate service holding the
index in memory (about 2.5 GB) would cut the lookup to one round trip, but it is one more
moving part and it is not needed while by-hash reads of old blocks stay rare: eth peers ask by
number, except for the block a range sync starts from.

## 3. The converter: `verified/` to chunks (one-time)

**D10.** An `import` subcommand, `import export --state-dir <dir>`, not a separate tool, which
replaces `import load` (removed, 2026-10-04). It runs once per existing `verified/` folder (OP Mainnet's and Unichain's on the user's
server). It needs exactly what `importer` already has:
- the state directory and its accepted range (`verified.json`, so only what `verify` accepted
  is exported);
- the verified chunk reader (`chunk.rs`);
- the sender proof `load` did, now `export`'s (recover and compare; senders in `verified/` are
  the service's until then);
- progress and stop handling.

Built (`bin/op-indexer-import/src/export.rs`; `docs/import.md` section 3.3):
- **Flow**: the verified 100-block chunks are read, their senders proven (`docs/import.md` 3.2)
  and their records prepared, several chunks at once (`--threads`, one per CPU). The records
  are cut into D4 chunks in order (blooms dropped, each receipt's bloom first checked against
  its logs) and sealed at level 1 with their index.
- **Upload and listing**: up to `--uploads` chunks (4) are uploaded at once. A manifest
  segment is appended every 16 chunks, after their objects are stored.
- **Index**: once the range is done, the hash index's first generation is written from every
  exported block and recorded in a manifest segment of its own.
- **The last chunk** ends at the range's last block, wherever D4 would have cut it: boundaries
  are deterministic from the manifest's tip. The exporter continues after it.
- **Resumable**: on start it reads the manifest and continues after its last chunk,
  rebuilding the index input from the exported chunks' indexes. A chunk uploaded but not yet
  listed is sealed again, to the same name and bytes. A run with nothing left to do ends at
  once.
- **Targets**: R2, with credentials from the environment (`OP_INDEXER_R2_ACCOUNT_ID`,
  `_BUCKET`, `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY`, optional `_ENDPOINT`), never logged. Or
  `--to-dir`, a local directory with the same layout (`object_store`'s local backend).
- **Disk**: a chunk lives in memory until it is uploaded. The index input spills by shard
  under `<state>/export-index/` (about 2.5 GB for OP Mainnet) and is removed at the end.

**Measured** (2026-10-04, Apple M-series, 10 cores, release build, `--to-dir` on the internal
SSD): the 20 sample chunks (2,000 post-Isthmus OP Mainnet blocks, 50,368 transactions, 147 MB
of records):
- sealing took 0.37 s, about 5,400 blocks/s or 400 MB/s of records, with 48,357 senders
  recovered; this is CPU-bound and runs in parallel;
- the result is one chunk of 20.5 MB (7.2×);
- the index generation took 1.6 s, its 4,096 shard files mostly system time on the local
  disk; on R2 it is 4,096 PUTs.

Reading it back through `ChunkStore` (scratch program, local backend):
- the stream returned all 2,000 blocks byte for byte equal to the verified chunks, receipts
  with rebuilt blooms included, in 231 ms (8,600 blocks/s, 635 MB/s of records);
- a stream started mid-chunk begins at its block;
- an index read, three single blocks and four hash lookups (one a miss) took 13 ms;
- one flipped byte was refused.

Extrapolated to OP Mainnet's 157.7 M blocks, at the measured sealing rate of post-Bedrock
blocks, the whole conversion is bound by sender recovery (about 13 CPU-hours)
and by the upload. Not measured whole.

## 4. Sealing new chunks: the exporter

**D11.** Sealing rule: a chunk is sealed when **every** block in it is at or below the L1
**finalized** head and **has its receipts**. The chunk's last block is the D4 boundary, so
sealing waits for the whole range; the newest partial range stays in the servers' tails.

**D12.** The exporter is a **mode of the `server` binary** (`server --export`): exactly one
server in a deployment runs it, and it holds the only R2 write key; every other server has a
read-only key. It exports a chunk as soon as that chunk's range has been promoted into its tail
and meets the sealing rule:
- it reads the committed blocks after the manifest's last chunk from its tail (`ArchiveStore`);
- it checks the finalized head and the receipts it recorded (`ArchiveStore::heads`,
  `pending_receipts`);
- it seals the chunk, uploads it and appends the manifest segment (D8).

The other servers see the new segment, so their tails drop the sealed blocks.

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

- **Streaming**: a range read (`blocks(from, limits)`, Flight, a subscription catching up, an
  eth range) is a pipeline: GET the chunk (or the part of it the range needs, by its segment
  offsets), decompress segment by segment, check (D5), hand the records out.
  - The next chunk is fetched while this one is served (D4's rule).
  - Read-ahead is bounded: at most two chunks per reader and a global byte budget in memory.
  - Nothing is written to disk and nothing is kept after the reader moves on: this is
    read-ahead, not a cache.
- **A single block by number**: one ranged GET of the chunk's footer (its index, located by
  the manifest), then one ranged GET of the block's segment. Two GETs, about 1 MiB read.
- **By hash**: the index (D9), then the same as by number.
- **Check on fetch** (D5) runs per segment and per chunk as the bytes stream: the object's
  sha256 over the whole GET, the header hash and parent links per block. A single-segment read
  checks the segment's zstd checksum and the block's header hash; the object sha256 needs the
  whole chunk, so a chunk-wide check is not possible there.
- **Failures**: an R2 error or a failed check retries once on the same server; then the
  request fails with `UNAVAILABLE`, and the client goes to the next location the balancer
  gave it (6.3).

### 5.3 What it advertises to p2p peers

eth/69 lets a node advertise one contiguous range `[earliest, latest]` (`BlockRangeUpdate`).

**D14.** A server advertises the whole sealed range plus its tail (confirmed by the user,
2026-10-04): it can serve any sealed block from R2. Peers' history requests are read from R2 on
demand, under the existing per-peer serving limits (`docs/el.md`, "be a polite peer") and a
global R2 budget; a request beyond them gets an empty answer, which eth/69 allows. Pre-Bedrock
blocks stay served only to op-p2p-indexer peers (`opidx`, as today).

### 5.4 gRPC and Flight

Unchanged code over `R2Archive`:
- **Subscriptions** stream history from R2, then join the live window (the server follows
  the chain itself).
- **Flight `DoGet`** streams a ticket's range from R2. A ticket the balancer clipped to one
  chunk (section 6.4) is one GET.

### 5.5 As built (2026-10-04, not run)

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
  - Read-ahead (5.2): a consumer's `blocks` calls are served by a feed that streams chunk after
    chunk ahead of it, at most 64 MiB per reader and 512 MiB in all, dropped after a minute
    unread; no disk, no cache.
  - Peers (5.3): a read that needs R2 takes one of 4 places and counts against 512 MiB a
    minute; without one the peer gets the empty answer.
  - `Exporter` (section 4): adds a block to the chunk being written once it is finalized and
    has its receipts, and publishes the chunk when the writer ends it.
- `bin/server`: `OP_INDEXER_CHUNKS_DIR` reads the chunks from a local directory instead of R2 (local runs, the bench); `--export` or `OP_INDEXER_EXPORT=true`; `OP_INDEXER_R2_ACCOUNT_ID`, `_BUCKET`, `_ACCESS_KEY_ID`,
  `_SECRET_ACCESS_KEY`, `_ENDPOINT`, `OP_INDEXER_EXPORT_ID`; the node's variables otherwise.
- API keys (D18): `OP_INDEXER_STREAM_API_KEYS` on `indexer` and `server` alike, an interceptor on
  the gRPC and Flight services (`crates/stream/src/auth.rs`); empty means no check.
- Not built: retry-once-then-`UNAVAILABLE` on a failed chunk read (5.2) is left to the
  caller's own retry; the hash lookup is the chunk store's (no index cache on the server side).

### 5.6 The indexer's history from chunks (design, not built)

Without `import load`, a single-user `indexer` gets its history from the same chunks (or from
peers, by range sync, as today). Design:
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

## 6. The balancer

### 6.1 Table

Kept in memory and rebuilt from registrations after a restart. Per server, from its last
heartbeat:
- id, chain, and the `host:port` of its gRPC and Flight listener;
- health: an unhealthy server gets no work;
- heads: unsafe, safe, finalized, and the last sealed block it has read from the manifest;
- contiguity (`contiguous_through`): the highest block N such that it holds every block from
  the chain's first through N, each with its receipts. A server holds the sealed chunks (R2),
  its own tail (committed blocks above them; filled by range sync, `OP_INDEXER_EL_SYNC=true`)
  and its own unsafe chain (gossip since it started). Without range sync there is a gap
  between the last sealed block and the first block it gossiped: its head is above the gap
  but it cannot serve it (seen on the bench, 2026-10-04: one server answered `NOT_FOUND` for
  a block the other served). The server computes it at each heartbeat, cheaply, as the range
  it advertises to execution peers (`NodeView::contiguous_through`, `NodeProvider::range`):
  its committed store's blocks (contiguous by construction: every append links to the one
  before) up to the first still waiting for receipts, extended through its unsafe chain's
  canonical blocks that link to them and have theirs, each search continuing from the last;
  never below the last sealed block;
- load: requests in flight (subscriptions, Flight streams, lookups) and bytes sent per second.

The table holds no chunk ranges (corrected by the user, 2026-10-04): every server is stateless
and reads the same bucket, so every server serves every sealed chunk. The balancer reads the
manifest itself, once per refresh (every 30 s), only for the chunk boundaries.

### 6.2 Registration

A server opens a `Register` stream to the balancer and sends a heartbeat every 5 s with its
health, heads, contiguity and load. Three missed heartbeats (15 s) mark it down and remove it.

### 6.3 Health and failover

- A server is down after 3 missed heartbeats, and out of the table as soon as its `Register`
  call ends.
- Work above the sealed chunks goes only to a server that holds every block through it: its
  head covers the work and so does its `contiguous_through` (6.4, 6.5). A server that falls
  behind, or has a gap above the sealed chunks, gets none there, but still serves sealed
  chunks. A server that reports no `contiguous_through` gets no work above them.
- Failover is on the client side: every answer names more than one server when there are.

### 6.4 Flight: per-chunk jobs

**D16.** The balancer implements `GetFlightInfo` and `ListFlights` only, and turns a range
into per-chunk jobs, so one big range runs in parallel over the servers:
- One `FlightEndpoint` per chunk the range touches, its ticket clipped to the chunk
  (`table:first:last:cap`, the stream's existing ticket); every healthy server can take it.
- The part above the last sealed chunk becomes jobs only a server can take whose head (under
  the ticket's cap) and `contiguous_through` both reach the job's last block.
- Every job is also cut to the servers' `DoGet` limit (100,000 blocks).
- Each job goes to the least loaded server that can take it, and its `location` lists up to
  three, least loaded first. The load counts the jobs already given out for the same range, so
  a big range spreads over every server while one busy with other requests gets fewer; ties go
  round-robin. Pure round-robin would ignore that one request can be 1,000 times another.
- A Flight client fetches the endpoints in parallel, straight from the servers, and moves to
  the next location if one fails.
- The ticket code is `crates/stream/src/flight.rs`'s (`op_indexer_stream::ticket`).

### 6.5 Locate

**D17.** `Locate(chain, from_block) → [server endpoints]` for gRPC subscriptions: the healthy
servers whose `contiguous_through` reaches the block before `from_block` (they hold every
block up to it), least loaded first. The client
subscribes to the first and, on failure, resubscribes from its last block at the next.
Subscriptions already resume by number.

### 6.6 Cost of reads

R2 Standard, from Cloudflare's pricing page (read 2026-10-04):
- storage $0.015 per GB-month;
- Class A (PUT, LIST) $4.50 per million;
- Class B (GET, HEAD) $0.36 per million;
- egress free.

A ranged GET is a GET; the page does not say otherwise.

- **Streaming**: as built, a stream reads runs of segments of 8 MiB per GET
  (`ReadOptions::range_bytes`), four at once, for read-ahead and parallel transfer. So 1 TB
  served (compressed bytes read from R2) is about 125,000 GETs, about **$0.05 per TB**. Whole
  39 MB chunks per GET would be about 26,000 GETs, $0.01 per TB. Reading one segment (about
  150 KB compressed) per GET would be about 6.7 million GETs, $2.40 per TB, so streams never do
  that.
- **Random single blocks**: two GETs each (footer, segment), $0.72 per million blocks; by hash,
  two or three more.
- **Storage**: OP Mainnet's ~500 GB of chunks plus a 2.5 GB index generation is about
  $7.60 a month.
- **Droplet egress** to the users is the real cost of serving, not R2.

**D18. API keys.** The balancer and the servers accept a request only with a key from their
configured list, sent as gRPC metadata (`authorization: Bearer <key>`). That covers gRPC,
Flight `GetFlightInfo` and Flight `DoGet`, which go straight to a server with the same key.
Servers register with the balancer using a server key of their own. Keys come from the
environment and are never logged. TLS is not part of this design.

### 6.7 As built (2026-10-04)

- `crates/balancer` (`op-indexer-balancer`): the `Balancer` component, its proto
  (`proto/opindexer/balancer/v1/balancer.proto`, package `opindexer.balancer.v1`) and the
  server's registration client (`register`). `bin/balancer`: the binary over it.
- **Registration** (6.2, 6.3): a bidirectional stream; the balancer answers the first
  heartbeat with `Registered`. A 15 s gap between heartbeats, a heartbeat of another chain or
  id, or a newer registration of the same id ends the call, and the server's entry with it.
  The client (`register::Registration::run`, wired into `server` by
  `OP_INDEXER_BALANCER_URL`) heartbeats every 5 s from a `watch` of the server's `Report`,
  and registers again after a backoff of 1 s doubling to 30 s with jitter; it never stops the
  server. A server's address must be exactly `host:port` (`register::is_valid_address`).
- **Picking** (6.4, 6.5): by requests in flight plus the jobs given out for the same request,
  then bytes per second, ties round-robin. Above the sealed chunks a server serves up to its
  reach, min(head under the cap, `contiguous_through`) (`table::Server::reach`): Flight jobs
  by the ticket's cap, `Locate` by the unsafe head. The heads and `contiguous_through` are the
  heartbeats'; no separate `GetHeads` probe.
- **Flight** (D16): the other Flight calls are `UNIMPLEMENTED` (no `GetSchema`: the schema is
  in each `FlightInfo`). A range is clipped by the best reach among the servers (the head of
  its cap, but no further than the server's `contiguous_through`), and may not start below the
  first sealed chunk (`OUT_OF_RANGE`). `ordered` is set.
- **Keys** (D18): `Locate` and Flight take a user key from `OP_INDEXER_STREAM_API_KEYS`, the
  servers' own list; `Register` takes a server key from `OP_INDEXER_BALANCER_SERVER_KEYS`,
  which is required. Checked per call (`op_indexer_stream::ApiKeys::verify`), never logged.
- **Checked** locally with a throwaway client against the one-chunk export of 3.3 (local
  backend, fake servers): the order by load, a server behind the tip kept out of tip jobs but
  given sealed ones, an unhealthy server given nothing, both kinds of key, another chain, a
  server leaving (removed at once) and a server stalling (removed after 15.0 s).
- **Not built**: CPU in the load; servers report `bytes_per_second` as 0 for now.
- **Follow-up** (shipped as is for v0.1.1; [roadmap.md](roadmap.md), "Follow-ups"): the
  balancer builds two stacks it never runs. It builds storage (fjall) through
  `op-indexer-stream`, for the ticket types and `ApiKeys`: a light crate for those removes it.
  It builds the node through `op-indexer-node`, for `env_file` and `shutdown_signal`: a tiny
  crate for those removes it, and the importer's copy of `env_file` with it. The binaries also
  repeat their env helpers, R2 settings and tracing setup, which could go in the same crate.

## 7. The bench (3 to 4 small droplets, one R2 bucket)

Setup:
- 2 or 3 `server`s (no cache), one of them with `--export`;
- one `balancer`;
- a bucket filled by `import export` from OP Mainnet's `verified/`.

Measure:
- **Converter**: blocks/s and MB/s from `verified/` to R2; total time and objects; R2 Class A
  operations.
- **R2 from a droplet**:
  - time to first byte, and its spread;
  - streaming throughput per server, with 1, 4 and 16 GETs in flight;
  - random single-block latency by number (2 GETs) and by hash (index + 2 GETs).
- **The chunk size**: serve time of a chunk to a sequential reader against TTFB plus the
  transfer of the next (D4's rule), with read-ahead of one and two chunks; settles the size
  target.
- **R2 conditional PUT**: whether `If-None-Match: *` is honoured (D8).
- **Decompression**: CPU per served GB at level 1 against level 3 on the droplets (D3).
- **Flight**: `DoGet` MB/s per server; aggregate MB/s for a whole-chain `GetFlightInfo` with
  the endpoints fetched in parallel, for 1, 2 and 3 servers (does it scale?).
- **Subscriptions**: catch-up blocks/s from R2; time to join the live window.
- **Balancer**: `GetFlightInfo` and `Locate` latency; heartbeat load; failover time after
  killing a server mid-`DoGet` and mid-subscription.
- **Peers**: eth requests served, R2 reads triggered by peers, empty answers given (D14).
- **Exporter lag**: from a block's finalization to its chunk listed in the manifest.
- **Restart**: time to replay the unsafe chain's journal (D0) against a node that waits for
  gossip.
- **Cost**: R2 operations and stored GB per day against section 6.6's estimate; droplet
  egress.
- **Resources**: CPU and memory per server at rest and under load (the read-ahead budget);
  the tail's disk.

## 8. Reused, new, open

- **Reused**:
  - the verified chunk record and its reader/writer (`chunk.rs`);
  - `PreparedBlock` and the archive encodings;
  - the `ArchiveStore` trait and `FjallArchive` (tail);
  - the stream's `Source`, conversions and Flight tickets;
  - `el` serving and `BlockRangeUpdate`;
  - `l1`'s finalized head;
  - the importer's state, sender proof and progress.
- **New**:
  - the in-memory unsafe chain with its fjall journal, which replaces Redis (D0);
  - the chunk index and footer, `R2Archive` with its streaming read-ahead, and the hash index
    shards (D9);
  - the R2 client (an S3-compatible crate, chosen when built) and the manifest;
  - `import export` (the converter) and `server --export` (the exporter);
  - the `server` and `balancer` binaries, the registration protocol and the API keys.
- **Settled by the bench, not decided here**: the chunk size target (D4), whether R2 honours
  conditional PUT (D8), level 1 against level 3 on real servers (D3).
