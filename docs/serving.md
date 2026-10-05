# Serving at scale: chunks, `server` and `balancer`

Status: **design, nothing built.** Decision: [roadmap.md](roadmap.md), 2026-10-04, "Four
binaries". To be built after Base. Decisions are marked **D**; the user's answers of
2026-10-04 settled the open questions (they are recorded in the decisions). Measurements are
from 2026-10-04.

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
needs a separate service: no Redis in the binaries, in docker compose or in the docs. (This
replaces the Redis unsafe store of today's `storage` crate; `docs/storage.md` and the compose
file still describe Redis and change with it.)

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

```text
chunk  = magic "OPXC" | version u16 | chain id u64 | first number u64 | last number u64
       | first parent hash (32) | last hash (32)
       | segment* | index | footer
segment = one zstd frame (with its checksum) of up to 1 MiB of block records
record  = the verified chunk's block record (hash, senders, header, body, receipts)
index   = zstd frame: per segment (offset, length, first number);
          per block (hash, segment, offset in segment), sorted by hash in a second table
footer  = index offset u64 | index length u64 | sha256 of everything before the footer (32)
          | magic "OPXC"
```

Integers are little-endian. **D2.**
- **Segmented zstd frames.** One block is read by decompressing one ~1 MiB segment, not the
  chunk. A ranged GET of one segment works on R2.
- **A per-chunk index for by-hash lookups**, in the chunk itself: no global index object to keep
  up to date (section 2.4).
- **The footer at the end**, so one ranged GET of the last bytes (`Range: bytes=-N`) gives the
  index without downloading the chunk.

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
1. **the object's sha256** equals the manifest's (and the footer's): transport or storage
   damage;
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
    chunks/<first:012>-<last:012>-<sha256, 16 hex>.opxc     immutable
    manifest/<sequence:010>.json                            immutable, append-only
```

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
  entries sorted by hash: the next 8 bytes of the hash and the block number (u64), 16 bytes an
  entry. A 256-entry fan-out table on the next 8 bits (1 KB) heads each shard.
- **Sized for OP Mainnet**: 157.7 M blocks give about 38,500 entries per shard. That is 616 KB a
  shard and 2.5 GB a generation, about 150 entries (2.4 KB) per fan-out bucket. An 8-byte
  remainder after a 12-bit prefix leaves a false match below one in 10^9 over the whole chain;
  a match is confirmed by the block's own hash anyway.
- **Lookup**: two ranged GETs, the shard's fan-out table and then its bucket (about 3 KB), then
  the block's own segment. Shard sizes are in the generation's manifest entry, so no HEAD is
  needed.
- **Generations**: shards are never modified.
  - The exporter writes a new full generation every 50,000 blocks (`server --export`), and
    the converter writes the first. Names carry the generation number:
    `index/<gen>/<prefix:03x>.idx`.
  - A rewrite is 2.5 GB and 4,096 PUTs, about $0.02.
  - Between generations the hashes of the chunks sealed since the last generation are in those
    chunks' footers (section 1.2). That is at most about 14 chunks at OP Mainnet's rate, read
    with one ranged GET each, newest first, only when the generation's shard misses.
- **Recent blocks** (the unsealed tail and the unsafe chain) are found locally, as today.

This keeps every object immutable and needs no index service. A separate service holding the
index in memory (about 2.5 GB) would cut the lookup to one round trip, but it is one more
moving part and it is not needed while by-hash reads of old blocks stay rare: eth peers ask by
number, except for the block a range sync starts from.

## 3. The converter: `verified/` to chunks (one-time)

**D10.** An `import` subcommand, `import export --state-dir <dir> --bucket ...`, not a separate
tool. It runs once per existing `verified/` folder (OP Mainnet's and Unichain's on the user's
server). It needs exactly what `importer` already has:
- the state directory and its accepted range (`verified.json`, so only what `verify` accepted
  is exported);
- the verified chunk reader (`chunk.rs`);
- the sender proof `load` does (recover and compare; senders in `verified/` are the service's
  until then);
- progress and stop handling.

It reads the verified 100-block chunks in order, proves the senders as `load` does, re-cuts
the records into D4 chunks (dropping the blooms), writes them at level 1 with their index,
uploads them and appends
a manifest segment every N chunks. It is resumable: on start it reads the manifest and
continues after its last chunk. Uploads run several at once with backoff, bounded by bytes in
flight.

For the converter's own disk it writes each chunk to a temporary file, uploads it, and
deletes it; nothing else is kept.

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

## 6. The balancer

### 6.1 Table

Kept in memory and rebuilt from registrations after a restart. Per server:
- id and endpoints (gRPC, Flight), chain;
- health, live head and tail range;
- load: subscriptions and Flight streams open, R2 bytes per second, CPU;
- last heartbeat.

It reads the manifest like a server, to split ranges into chunks. It does not track which
server holds what: every server can serve every chunk.

### 6.2 Registration

A server opens a `Register` stream to the balancer and sends a heartbeat every 5 s with its
head and load. Three missed heartbeats mark it down.

### 6.3 Health and failover

- A server is down after 3 missed heartbeats (15 s); a `GetHeads` probe also checks that its
  head is no more than a few blocks behind the best one.
- A server that falls behind is not given live work until it catches up.
- Failover is on the client side: every answer names more than one server.

### 6.4 Flight: per-chunk jobs

**D16.** The balancer implements `GetFlightInfo` and `ListFlights` only, and turns a range
into per-chunk jobs:
- One `FlightEndpoint` per chunk the range touches, its ticket clipped to the chunk
  (`table:first:last:cap`, the stream's existing ticket).
- Each endpoint's `location` lists two or three servers, chosen by load: least loaded first,
  then round-robin over the rest, so a big range is spread over every server.
- The part above the last sealed chunk is one endpoint listing every healthy server.
- A Flight client fetches the endpoints in parallel, straight from the servers, and moves to
  the next location if one fails.
- The ticket and resolve code is `crates/stream/src/flight.rs`'s.

### 6.5 Locate

**D17.** `Locate(chain, from_block) → [server endpoints]` for gRPC subscriptions: the healthy
servers, least loaded first. The client subscribes to the first and, on failure, resubscribes
from its last block at the next. Subscriptions already resume by number.

### 6.6 Cost of reads

R2 Standard, from Cloudflare's pricing page (read 2026-10-04):
- storage $0.015 per GB-month;
- Class A (PUT, LIST) $4.50 per million;
- Class B (GET, HEAD) $0.36 per million;
- egress free.

A ranged GET is a GET; the page does not say otherwise.

- **Streaming**: one GET per chunk. At about 39 MB a chunk, 1 TB served (compressed bytes read
  from R2) is about 26,000 GETs, about **$0.01 per TB**. Reading by segment instead (about
  150 KB compressed each) would be about 6.7 million GETs, $2.40 per TB, so streams read whole
  chunks or large ranges of segments, never one segment at a time.
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
