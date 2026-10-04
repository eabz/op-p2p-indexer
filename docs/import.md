# Import spec (`bin/op-indexer-import`)

Status: **built on `feat/el`; the whole OP Mainnet chain (blocks 0 to 157,745,023) has been
downloaded and verified on the real service, and its load into the block archive was running
when this was written (section 6). The archive's layout has since changed (schema version 2:
senders and the committed heads), so that archive must be loaded again from the verified
chunks; no download is needed. ClickHouse is no longer part of the project.**

**Goal (user, 2026-10-04): sync the whole chain from HyperSync as a separate process, usable
for any chain, into a local store that the node serves over p2p; then test a normal p2p sync
against that node.**

OP Mainnet's blocks before the Bedrock upgrade (0 to 105,235,062) cannot be re-executed by a
modern EVM, and no execution peer we reached serves them (`docs/el-viability.md`). This binary
downloads a chain's blocks from Envio HyperSync, verifies every block against a trusted anchor,
and loads the verified bytes into the local block archive. It keeps what it downloaded, so
`verify` and `load` can be repeated without HyperSync.

It is a separate binary: the indexer never links it and never talks to an external service.
The HTTP client and the compression crates belong to this package only.

**Sources.** Envio HyperSync for the blocks, and, only for what HyperSync leaves out of some
rows, the chain's JSON-RPC endpoint, read-only (section 3.1a). Neither is trusted: every block
is rebuilt and proven by its header hash and the parent links up to the anchor.

## 1. What was measured (2026-10-04, 1,009 blocks)

- HyperSync has the whole range from block 0, with every header field. Each rebuilt header
  hashed to its block hash; the Bedrock block's parent hash equals the hash of 105,235,062.
- Every transaction re-encoded to its hash and every transactions root matched. Legacy blocks
  hold exactly one transaction, always type 0.
- Every receipts root matched with the plain legacy receipt encoding (status, cumulative gas
  used, bloom, logs).
- L1-to-L2 message transactions have a zero signature and HyperSync gives their sender as the
  zero address; the old client's metadata for them is not exposed.
- About 7 KB of JSON per block with all fields; a 1,000-block query took 1.3 s (73 ms of server
  time).

## 2. The constraint that shapes the design

The API token allows unlimited requests for **30 minutes from first use**; the window can be
reset. So nothing but downloading happens during the window, as few bytes as possible
travel, as little work as possible is done per byte, and the download resumes: one window is
not enough for the whole chain.

## 3. Phases

Each phase is a subcommand, works in a state directory (section 7), and can be stopped and run
again; a completed chunk is never redone.

### 3.1 `download`

- On its first run it decides the range and records it in `plan.json` (section 7).
- The range is cut into chunks: 1,000 blocks before the chain's Bedrock block, 100 from it on,
  where one block holds as much as many legacy ones (`--chunk-blocks`, and a tenth of it).
  Chunks are fetched with `--requests` requests in flight (default 64), each on its own
  HTTP/1.1 connection.
- A response may cover less than the range asked for; the chunk is complete only when every
  block of it has arrived.
- **Stored as it travels.** The response body is written to the chunk's file exactly as
  received, in the content encoding the service chose (zstd is asked for first, then gzip),
  after one byte naming that encoding. Nothing is decompressed to be compressed again. The
  only work per byte is one streaming decode, into nothing, to read the cursor
  (`next_block`) at the end of the body. Memory is about a megabyte per request in flight,
  whatever a chunk holds.
- **Fields requested**: what the header, the transactions and the receipts with their logs
  are rebuilt from. Not requested, because `verify` computes them and the header hash proves
  them: the bloom filters, the transactions root, the receipts root and the transaction
  hashes. The old client's L1 fee fields are not requested: no root covers them and nothing
  reads them.
- **Errors**: a chunk that fails for a reason that may pass (HTTP 429, 408, 5xx, a broken
  connection, an answer cut short) is fetched again from its start, up to six times, with
  capped, jittered backoff. A 400, 401 or 403 fails at once. When a chunk fails for good, or
  free disk space falls below 16 GiB, no new chunk is started, the requests in flight finish
  and are written, and the phase ends with a summary of what is missing. It never spins.
- **Open files**: a request in flight holds a connection and a chunk file. At start the
  soft limit is raised to what `--requests` needs if the hard limit allows; otherwise the step
  refuses to start and prints the `ulimit -n` command. Running out of files while writing a
  chunk is retried like a busy service.
- **Progress**, every 10 seconds: chunks done, blocks, blocks per second over the last
  minute, requests in flight, bytes per second on the wire, the processor time spent decoding
  (`decode_cpu_percent`, 100 = one core), bytes on disk, free disk space, and `bytes_left` /
  `secs_left`. The time left is estimated from bytes, not blocks: the bytes per block of each
  era (before the Bedrock block, and from it on, where blocks are ten to thirty times larger)
  times the blocks left in it, over the speed on the wire. It is absent until an era with
  blocks left has been sampled, and within the second era it still grows as blocks do.

### 3.1a What the service leaves out, from the chain's RPC

Two gaps are known, both on Unichain; OP Mainnet's whole chain verified without either.

- **Authorization lists.** HyperSync sends EIP-7702 transactions (type 4) without their
  `authorization_list`; every other field is there. Found at block 16,068,511: its type-4
  transaction rebuilt with an empty list hashes the header to another value. The list is not
  optional (EIP-7702 refuses an empty one), so such a row cannot be rebuilt from the download
  alone.
- **Holes: whole blocks without their rows.** At 55,142,810 to 55,142,819 the answer has the
  ten header rows, matching the chain field for field, but no transaction and no log rows,
  while the chunk's other 90 blocks have theirs and the answer was not cut short (its cursor
  is the chunk's end). The chain has 8 transactions in 55,142,810. Every block after the
  Bedrock block has at least the L1-attributes deposit, so a block from there on without
  transaction rows is a hole: the whole block is fetched.

- **Checked after every download.** Once the chunks are on disk, `download` reads every chunk
  not verified yet (one per core at a time, within `verify`'s 256 MiB of downloaded bytes in
  flight) and lists every field its rows lack that the
  rebuild needs, or fills with a default the header hash must then prove: header fields of
  the block's forks (`mix_hash` and `base_fee_per_gas` from Bedrock, `withdrawals_root` from
  Canyon, the blob gas fields and the parent beacon block root from Ecotone), the fields of
  each transaction type, a receipt's status, and a deposit receipt's nonce and version from
  Canyon. Each is logged once, at warn, with its count and its first block: one pass shows
  them all, where `verify` would stop at the first. Progress is logged every 10 seconds
  (chunks per second, time left). The pass costs about what reading the rows costs `verify`
  (decompress and parse, no hashing, nothing written): by section 6's measurements about a
  third of a `verify` pass over the same chunks. It runs again on every `download` over the
  chunks still not verified, and skips the verified ones.
- **Left out** means: a type-4 row with no `authorization_list`, no bytes, or a list of zero
  entries (the service writes an empty list as a count of zero, as it does `access_list`), and
  no fill. The scan and `verify` use the same rule (`TransactionRow::lacks_authorization_list`).
- **Fetched**: `eth_getBlockByNumber` with full transactions for each block that needs it
  (Unichain's public endpoint does not allow `eth_getTransactionByBlockNumberAndIndex`), and
  for a hole its receipts too, with `eth_getBlockReceipts` (else `eth_getTransactionReceipt`
  per transaction, for an endpoint without it), up to 10 calls per request as one JSON-RPC
  batch (the endpoint refuses larger batches, with one error for the whole batch, which is
  reported as a refusal with its message), four chunks' requests at a time. A request is
  retried up to six times with `download`'s capped, jittered backoff on a busy endpoint (408,
  5xx), a broken connection or a malformed answer; on a rate limit (429) it waits what
  `Retry-After` asks (up to 10 minutes), else 15 s doubling to 2 minutes, minutes in all.
  Ctrl-C drops the requests in flight at once. A refused call, a block the endpoint does not have, or a block hash that is not
  the downloaded one (an endpoint of another chain) fails at once, naming the endpoint.
- **Kept apart.** What is fetched goes to the chunk's fill, `raw/<from>-<to>.fill.json`,
  written atomically and durably, in the RPC's JSON form (a list per transaction; a hole's
  transactions and receipts with their logs); the downloaded chunk stays as received. A
  chunk downloaded again loses its old fill first. `verify` stays offline: it puts the fill
  into the rows before rebuilding, a list into its transaction, a hole's transactions and
  logs as rows like the service's (the receipt's fields with the transaction's, the access
  list in the service's layout). A type-4 row with no list, or a block after Bedrock with no
  transactions, and no fill, fails `verify` with a message to run `download`, not the generic
  hash mismatch.
- **Trust unchanged.** What is filled goes into the rebuilt block; the transactions and
  receipts roots, so the header hash, prove it. A hole's senders are the endpoint's `from`,
  which `load` recovers from the signatures and checks like every other. A wrong fill fails `verify` like a wrong row (checked: one
  changed signature byte gives a header hash mismatch).
- **Endpoint**: `--rpc-endpoint` (`OP_INDEXER_IMPORT_RPC_ENDPOINT`), by default
  `https://mainnet.unichain.org` for Unichain and none for OP Mainnet, whose rows need none so
  far; without one, `download` stops with a message when something is missing.
- **Counted**: `authorization_lists_to_fetch`, `holes_to_fetch` and `rpc_filled_transactions`
  in `download`'s summary, `rpc_filled_transactions` (lists filled and hole transactions
  added) in `verify`'s; each hole is also a `block`/`transactions` line of the missing-field
  report.
- Not done: asking HyperSync again for a hole's chunk before using the RPC; the RPC answer is
  proven the same way.

Checked on 2026-10-04, offline but for the endpoint: a chunk of block 16,068,511 built from
the endpoint's block and receipts in HyperSync's row format, without `authorization_list`.
`verify` failed with the message to run `download`; `download` fetched the list and wrote the
fill; `verify` then rebuilt the header to the block's hash 0xb8a5…c3e6 and accepted the range.
A second `download` fetched nothing. The same with the row's list written as a count of zero.
Holes, the same way: block 55,142,810 with its header row only (no transaction or log rows)
failed `verify` with the message to run `download`; `download` fetched the block and its
receipts in one batch (8 transactions, 6 logs) and `verify` rebuilt the header to its hash
0x8f43…ca26. Block 16,068,511 as a hole (its type-4 transaction, with its authorization list
and access list, coming from the endpoint) verified too. The receipt-by-receipt fallback has
not run: Unichain's endpoint has `eth_getBlockReceipts`. Not checked against HyperSync's own
Unichain answers.

### 3.2 `verify`

Offline; reads the downloaded chunks only, several at once (`--verify-threads`, default one
per core).

For every block it rebuilds the transactions and the receipts from the downloaded rows,
computes the transactions root over the transaction encodings and the receipts root over the
receipts, rebuilds the header with those roots and the bloom of the logs, and requires the
header to hash to the block's hash and to name the previous block as its parent. Once every
chunk is verified, the chunks are linked to each other and the last block is checked against
the anchor (section 11). The bytes that passed are written as verified chunks.

- **What is proven** for every block: its header hashes to its block hash and links to its
  parent up to the anchor; its transactions root is the root over the stored transaction
  encodings; its receipts root is the root over the stored receipts. That is what a peer
  checks when the bytes are served to it.
- **Header fields `verify` rebuilds.** The transactions root, the receipts root and the logs
  bloom are never downloaded: `verify` computes them from the transactions, receipts and logs,
  and the header hash proves them (they are not compared with anything the service says).
  `mix_hash` is downloaded, but the service leaves it out of some pre-Bedrock rows (seen on
  OP Mainnet in chunks around block 47,705,000, 47,740,000 and 47,745,000). Before Bedrock
  every header has a zero `mix_hash`, so a row without it is rebuilt with zero, and the
  header hash decides: a wrong guess is refused, never accepted. The blocks rebuilt this way
  are logged per chunk and counted in the summary (`rebuilt_header_fields`). From Bedrock on
  a row without `mix_hash` is refused ("the header lacks `mix_hash`").
- **Senders are proven by `load`, not by `verify`.** `verify` checks no signature: the sender
  it records in each verified chunk is the `from` HyperSync reports. `load` then proves it
  before anything is archived (user decision, 2026-10-04): for every signed transaction it
  recovers the sender from the signature and compares; for a deposit it compares with the
  `from` in the deposit's encoding, which the transactions root and so the block hash cover.
  A mismatch stops `load` before that block, naming the block, the transaction's index and
  both addresses. So every sender in the archive is proven (recovered, or hashed for
  deposits) except one kind: a legacy transaction signed with all zeros (an L1-to-L2 message
  of OP Mainnet's client before Bedrock) has no signer, keeps the recorded zero address, and
  is counted, by `verify` and again at the end of `load` (`zero_signature_transactions`).
  The recovery runs on libsecp256k1 (alloy's `secp256k1` backend), which does it in about a
  fifth of the time of k256.
- **The accepted range.** Only when every chunk is verified, every link holds and the anchor
  matches does `verify` write `verified.json` (range, anchor, hash of the last block, time).
  Every run of `verify` removes it first. `load` refuses to load without a record that matches
  the plan, so nothing `verify` did not accept as a whole is ever loaded.
- A chunk that fails stops the phase with the block number, the check and the file; delete
  the file and run `download` again. With the roots not downloaded, a wrong transaction,
  receipt or log shows as the header hash not matching, for its block.
- Memory: the chunks verified at once are limited to 256 MiB of downloaded bytes (one chunk is
  always allowed); a chunk takes about twenty times its downloaded size while it is verified.
- Progress every 10 seconds (chunks, blocks, transactions, their speed over the last minute,
  busy threads, bytes written, free disk space, and the time left, estimated from the
  downloaded bytes still to verify over those verified per second), a line at start saying
  how many chunks are already verified, how many are not downloaded, the downloaded bytes to
  read and the free space, and lines during the linking pass.
- Disk: a verified chunk is about as large as its downloaded one. `verify` refuses to start
  if free space is under half of the downloaded bytes it has to verify, warns under 64 GiB
  free and stops cleanly under 16 GiB.
- `--from-block N` verifies only the chunks from that block on and neither links nor accepts
  the range: a quick check of one part of the chain (for example the first blocks after
  Bedrock) without waiting for everything before it. The chunks it verifies are kept;
  `verify` without the flag must still run before `load`. `run` does not take it.

### 3.3 `load`

- **`load` writes to the archive only** (section 4): the fjall store that is the node's
  committed store, which it serves peers and the stream from. It needs no database. Each
  block goes in with its senders, recovered and checked first (section 3.2): the chunks on
  disk are checked again on every load, so an existing state directory needs no new
  download. Its last line reports `senders_recovered` and `zero_signature_transactions`.
- **Cost of the check** (20 chunks after Isthmus, blocks 140,000,063 to 140,002,062, 2,000
  blocks, 48,357 signed transactions, 146 MB of RLP; M1 Pro, 10 cores, release build):
  recovery is about 32 µs per transaction on one core, 37 µs with decoding. The load used
  2.0 s of CPU instead of 0.22 s, and took 0.9 s instead of 0.7 s of wall time (one bulk
  append, so mostly the write). Preparing a block after Isthmus now takes about 1 ms of CPU
  instead of 0.11 ms. On this 10-core laptop that makes the bulk load bound by the cores:
  about 9,900 blocks/s (720 MB/s of RLP) instead of the 32,000 blocks/s (2.35 GB/s) measured
  above. On a 32-core server it is about 31,000 blocks/s (2.2 GB/s) of preparation, above
  the 580 to 665 MB/s of RLP the full load wrote (bound by its disk, section 6), so that load
  should stay bound by the disk; an estimate, assuming cores as fast as an M1 Pro's.
  Extrapolated to the whole chain, about 1.3 billion transactions: about 13 CPU-hours of
  recovery, some 25 minutes on 32 cores, spread over the load next to the writes.
- **An archive written by an older build is refused** when its schema version differs
  (version 2 added the senders and the committed heads). Move it away and `load` into a new
  directory from the same verified chunks; nothing is downloaded again.
- It loads only the range `verify` accepted (`verified.json`), and once the archive holds the
  range it checks that the archive's block at the top of the range is the one `verify`
  accepted (`last_hash`), so what was loaded is bound to what was verified.
- **What `load` trusts:** the verified chunk files on disk, as `verify` wrote them. It
  checks each header's hash and the chain's links again, but does not recompute the
  transactions and receipts roots, so a body or receipts value changed on disk after
  `verify` would be loaded. Verified chunks are written with zstd's frame checksum, so a
  damaged file fails to decompress (files written before the checksum was added still read,
  without that check).

## 4. The local history store, for serving

`load` also appends every verified block, in order from the first block of the range, to the
local block archive (`ArchiveStore`, fjall) in its consensus encoding: the store the node
serves peers from. The archive holds one contiguous range, so the import builds it upward
from block 0 and the running indexer continues it at the tip once the two meet.

- The archive gets a bulk append (`FjallArchive::bulk_append`), an importer-only path next
  to the node's `append_batch`. Chunks are read and each block prepared
  (`PreparedBlock::new`: header decoded, its keccak checked against the block's hash,
  values snappy-compressed) on blocking threads, one chunk per core (4 to 32). Blocks are
  collected into appends of about 1 GiB of RLP; one append is written while the next is
  prepared. Each append writes the four keyspaces at once, straight into new table and blob
  files (fjall's ingestion: no journal, no memtable), each synced, with `headers` registered
  last; see `docs/storage.md` section 9. The format on disk is the one `append_batch`
  writes, so an archive can be filled by either and continued by the node.
- Measured on an Apple M-series laptop (10 cores, internal SSD), with 20 real post-Bedrock
  chunks and 10 legacy ones repeated into a long valid chain (headers renumbered and
  re-linked; 2026-10-04):

  | | before (`append_batch`, 16 MiB) | after (bulk, 1 GiB) |
  |---|---|---|
  | legacy (2.2 KB/block) | 82,000 blocks/s, 187 MB/s RLP | 650,000 blocks/s, 1.5 GB/s RLP |
  | post-Bedrock (71 KB/block) | 5,000 blocks/s, 365 MB/s RLP | 32,000 blocks/s, 2.35 GB/s RLP |
  | bytes written / RLP | 0.68 (post), 1.11 (legacy) | 0.31 (post), 0.55 (legacy) |

  Bytes written are what the files hold (about 1.0 to 1.07 of the final directory size):
  values are written once and the number-keyed trees are moved, not rewritten, by
  compaction; only `numbers` (hash to number, about 40 bytes a block) is merged. On a disk
  that writes 325 MB/s the bulk path is then limited by the disk for post-Bedrock blocks
  (about 1 GB/s of RLP), and by preparation on the cores for legacy ones. On the full chain
  (section 6) it ran at 190,000 to 235,000 blocks/s, 580 to 665 MB/s of RLP.
- Progress lines give `secs_left` from the bytes of the verified files still to read, not
  from blocks, and `mb_per_sec` of RLP appended.
- A crash or a kill leaves the archive holding a contiguous prefix: `load` resumes after its
  last block, writing again the blocks of the unfinished append. A failed append is not
  retried within the run (it can leave files only the next open removes): running `load`
  again resumes. On a stop (Ctrl-C), no new chunk is read; the chunks being read are
  appended, and `load` reports where it stopped. Checked by killing the
  load at random points (`kill -9`) and reading every block back.
- Serving itself (answering header, body and receipt requests, advertising the held range)
  and the syncing side (a node fetching a range from peers and verifying it) are `el` work:
  `docs/el.md` sections 11 and 12.

## 5. Running it

The importer is a self-contained command-line tool, meant to be built here and run on another
machine: `cargo build --release -p op-indexer-import` produces one file to copy.

- Subcommands: `download`, `verify`, `load`, and `run` for all three in order.
- `--api-token <TOKEN>` carries the HyperSync token; `ENVIO_API_TOKEN` in the environment is
  the fallback. A flag is visible in the process list and the shell history, the variable is
  not. The token is never logged and never written to the state directory.
- Every other setting is a flag with an environment fallback and a default: the state
  directory (`OP_INDEXER_IMPORT_STATE_DIR`), the chain (`OP_INDEXER_IMPORT_CHAIN`), the
  endpoints (`OP_INDEXER_IMPORT_ENDPOINT`, `OP_INDEXER_IMPORT_L1_ENDPOINT`), the range
  (`OP_INDEXER_IMPORT_FIRST_BLOCK`, `_LAST_BLOCK`, `_ANCHOR_HASH`, `_LEGACY_ONLY`), the chunk
  size (`OP_INDEXER_IMPORT_CHUNK_BLOCKS`), requests in flight (`OP_INDEXER_IMPORT_REQUESTS`),
  `verify`'s threads and start (`OP_INDEXER_IMPORT_VERIFY_THREADS`,
  `OP_INDEXER_IMPORT_VERIFY_FROM_BLOCK`), and for `load` the archive directory
  (`OP_INDEXER_IMPORT_ARCHIVE_DIR`).
- **The importer fills the block archive the node serves from, and needs no database.**
  `load` appends the verified bytes and senders to the archive directory (`--archive-dir`,
  default `data-<chain>/archive`: `data-op/archive` or `data-unichain/archive`, where the node looks by default) and contacts nothing else. Redis is never needed.
- `load` needs the range accepted by `verify` (`verified.json`) and refuses anything else.
  What the archive holds is asked of the archive: `load` continues after its last block, and
  refuses an archive that does not start at the range's first block or holds another chain.
  The archive also records the chain it is for (chain id and genesis hash) on first open: one
  recorded for another chain is refused before anything is read or appended (see
  `docs/storage.md` section 9.2 for an archive with no record).
- `load` exits with an error if it stops before the end of the range, and says what to run.

### How to run it

Build on any machine with the Rust toolchain and copy the one file:

```bash
cargo build --release -p op-indexer-import
```

The file is `target/release/import` (the package is `op-indexer-import`). On the machine that runs it, with the defaults
(OP Mainnet, from block 0 to the last block committed to L1, state in `./import-state`):

```bash
import download --api-token <TOKEN> --requests 64
```

```bash
import verify
```

```bash
import load
```

`import --help` and `<command> --help` list every flag, its environment variable
and its default. Only `--state-dir` is shared by the steps: the range is decided by the first
`download` and read from `plan.json` afterwards.

- **The range**: with no range flags, block 0 to the L2 block of the newest dispute game of
  the chain on L1 (section 11). `--legacy-only` ends at the last block before Bedrock with its
  known hash and needs no lookup; `--last-block <n> --anchor-hash <hash>` gives any end whose
  hash you trust; `--first-block` any start. These are read on the first `download` only; on a
  later run a range flag that disagrees with the recorded plan is refused. To import another
  range, or to extend to a newer game, use an empty state directory.
- **State directory**: section 7.
- **Stopping and restarting**: Ctrl-C or SIGTERM stops a step within a fraction of a second
  and exits with status 1 and a summary; `run` does not start the next step. Run the same
  command again: it continues with exactly the chunks that are missing. A killed process
  (SIGKILL, power loss) is as safe: a chunk file is written under a `.tmp` name, synced and
  renamed, so a file with a final name is always complete, and leftover `.tmp` files are
  removed at the next start. A rename lost to a power loss only loses that chunk, which the
  next run does again; `verified/` is synced once before `verified.json` is written, and the
  JSON files' directory after each of them.
- **A damaged verified chunk is not a verified one.** A file copied or downloaded short
  (seen on a server filled from a snapshot: three files under the 64-byte header) would
  otherwise block linking and be skipped by `download`. One check decides for both: a
  verified file counts only if it starts with the two hashes and then a zstd frame's magic
  number (68 bytes read). `verify` removes a file that fails it, with a warning per file and a
  `damaged_removed` count in its start line, and verifies the chunk again from `raw/`. If
  `raw/` no longer has it, it counts as not downloaded, and `download` fetches just those
  chunks. Damage past the first bytes shows when a chunk is read in full (linking a game
  anchor, or `load`): every such error names the file and says what is wrong and what to
  run.
- **One process per state directory**: the directory is locked while a process runs; a second
  one exits with "another `import` process is using this state directory". The lock
  is released by the system when the process ends, however it ends.
- **Resuming**: when the request window closes, `download` stops with a summary of how many
  chunks are missing. Reset the window and run the same command again: only missing chunks are
  fetched. Ctrl-C stops any step cleanly; run it again to continue.
- **A chunk that fails `verify`**: the error names the block, the check and the file. Delete
  that file from `raw/` and run `download` again.
- **Disk**: a downloaded legacy chunk is 0.7 to 1.0 KB per block (zstd or gzip, as the
  service sends it) and a verified one about 0.5 KB per block: roughly 75 to 105 GB and 52 GB
  for the legacy range if the sample of section 6 is typical. The whole OP Mainnet chain was
  589 GB downloaded (section 6). `raw/` can be deleted once `verify` has accepted the range:
  `download` counts a chunk with a verified file as done, so a later `download` or `run` on
  the same state directory does not fetch it again.

## 6. Measured

Offline, on 1,000 saved blocks (50,000,000 to 50,000,999, one transaction each), Apple M-series:

- Size: 4.7 MB of JSON with the fields requested; 0.75 MB as zstd, 1.0 MB as gzip.
- `verify`, one thread: 0.17 s of processor time per 1,000 blocks with sender recovery, about
  0.06 s without (what `verify` does now): reading and decoding 6 to 8 ms, parsing 10 to
  13 ms, receipts and their blooms 8 ms, the two tries 9 ms, header, body and receipts
  encoding 5 ms, transactions 1 ms, writing the verified chunk with its sync 17 to 22 ms (on
  macOS; a sync is cheaper on Linux).
- Projection, not a measurement: 105 million legacy blocks at 0.06 s per 1,000 are about 105
  core-minutes, 13 minutes on 8 cores if the disk keeps up.

On the real service:

- 2026-10-03, 8 cores, 1 Gbit, an earlier build: the legacy range downloaded at about
  185,000 blocks per second with 64 requests in flight.
- The whole chain, a 32-core server: `download` of blocks 0 to 157,745,023 (157,745,024
  blocks, 589 GB) in about 25 minutes at 300 to 380 MB/s; `verify` accepted the whole chain
  (630,336 chunks, linking 58 s), its top block matching the claim of the newest dispute game
  (a type 9 super game); the bulk load into the archive ran at 190,000 to 235,000 blocks/s,
  580 to 665 MB/s of RLP, on a volume `dd` measured at 325 MB/s of sequential writes.

## 7. The state directory

```text
<state>/plan.json                 the chain, the range, its anchor and the chunk size
<state>/verified.json             the range `verify` accepted; `load` requires it
<state>/raw/<from>-<to>.raw       downloaded chunk: the service's answers as they travelled
<state>/raw/<from>-<to>.fill.json fields the service left out, from the chain's RPC (3.1a)
<state>/verified/<from>-<to>.blk  verified chunk: the consensus encodings that passed
<state>/lock                      held by the one process working on the directory
```

- `plan.json` is written by the first `download` and never changed: chain id, first and last
  block, the anchor (a trusted hash, or the dispute game found on L1) and the chunk size. Every
  later run of any step reads it; `verify` and `load` take no range flags. Files already
  written were cut by it, which is why it cannot change.
- A directory written by a build with another layout is refused: it either has chunks and no
  `plan.json`, or a `plan.json` with another version. Delete it and download again.
- A file exists only when it is complete: it is written under a `.tmp` name, synced and
  renamed; leftover `.tmp` files are removed at the next start.
  How the three short files on the new server came about is not known: this code syncs the
  data before the rename, so a crash cannot leave a short file under the final name; the copy
  of the snapshot is the likelier cause.

## 8. Not built

The senders and L1 metadata of L1-to-L2 messages; any source other than HyperSync; the binary
(Arrow) format of HyperSync, which measured offline would save about 20% on the wire over the
compressed JSON and was put aside; reading the deposit contract's logs on L1.

## 9. From the Bedrock block onward

Every block from Bedrock on starts with a deposit, and bridged deposits follow. `verify`
rebuilds legacy, EIP-2930, EIP-1559, EIP-7702 and deposit transactions, their receipts, and
headers of every fork (base fee, withdrawals root, blob fields, beacon root, and from Isthmus
the hash of an empty requests list, which has no column in HyperSync).

- **Deposits are rebuilt from HyperSync's `source_hash` and `mint` columns.** A deposit
  without a reported source hash fails `verify` with a named check: the source hash comes from
  the deposit's event on L1, which this tool does not read. On OP Mainnet the endpoint fills
  them for every deposit: the whole chain verified (section 6).
- The system-transaction flag, which has no column, follows the protocol's rule: only the
  L1-attributes deposit before Regolith has it.
- A deposit receipt carries the sender's nonce and the receipt version from Canyon on, when
  they became part of the hashed receipt; before Canyon nothing the root does not cover is
  stored.
- Fork times and the Bedrock block come from `op-indexer-chainspec`.
- `access_list` and `authorization_list` arrive as the bytes of the service's binary column
  (a hex string in the JSON), decoded when the transaction is rebuilt (`verify/lists.rs`).
- Verified on real data: the whole OP Mainnet chain, so every transaction type, user deposits
  and every fork up to the newest dispute game (section 6). A block that cannot be rebuilt and
  verified is not imported.

## 10. Any chain

The chain is chosen with `--chain <id>` on the first `download` and recorded in `plan.json`;
a later run with another `--chain` is refused, and one without it continues the recorded
chain. Its parameters (fork times, the Bedrock block and its time, the hash of the last
legacy block if it has a legacy chain, the block time, the dispute-game factory) come from
`op-indexer-chainspec`, which knows OP Mainnet (10) and Unichain (130). The HyperSync
endpoint is the importer's concern, not the chain specification's: a table in the importer
gives `https://optimism.hypersync.xyz` for 10 and `https://unichain.hypersync.xyz` for 130,
`--endpoint` overrides it, and a chain without an entry needs `--endpoint`. The L1 endpoint
(`--l1-endpoint`, default Ethereum's) is where the dispute games are looked up.

### Unichain (chain 130)

```bash
import --state-dir unichain-state download --chain 130 --api-token <TOKEN>
```

```bash
import --state-dir unichain-state verify
```

```bash
import --state-dir unichain-state load
```

What differs from OP Mainnet:

- **No legacy chain.** Unichain began with Bedrock at its genesis (block 0, time
  1730748359): every chunk is a post-Bedrock chunk, a tenth of `--chunk-blocks` (100 blocks
  by default), and `--legacy-only` is refused. Block 0's parent hash is zero and its header
  already has the fields of every fork through Granite, which are all active at genesis; the
  transaction and receipt rules are chosen by each block's timestamp as for OP Mainnet.
- **One-second blocks**, so about 60 million blocks today and about 600,000 chunks at the
  default size, about as many as OP Mainnet's: the linking pass, `verified.json` and memory
  are sized by chunk, not by block, and stay as they are. Unichain blocks are small, so
  `--chunk-blocks 10000` (1,000 blocks per chunk) means fewer requests and files; it is
  recorded in the plan and cannot change later.
- **The endpoint** is `https://unichain.hypersync.xyz`, the host the service's naming gives.
  It has not been reached from here; if it is wrong, give the right one with `--endpoint`.
- **EIP-7702 authorization lists** are missing from HyperSync's rows; `download` fetches them
  from `https://mainnet.unichain.org` (section 3.1a). A state directory downloaded before
  this needs one more `download`, which downloads nothing and fills what is missing.
- **The top anchor** is the newest dispute game of Unichain's own factory
  (`0x2F12d621a16e2d3285929C9996f478508951dFe4` on Ethereum): super games (type 9), whose
  claim is read for chain 130, with the timestamp turned into a block at one block a second
  from genesis.

**Run so far** (by the user, reported 2026-10-04): a download of 604,009 chunks, and a
`verify` that stopped at block 16,068,511 on the missing authorization list (section 3.1a).
The fill, the rest of `verify` and `load` have not run on Unichain yet.

## 11. Where the import stops, and the top anchor

The archive service's newest blocks are unsafe: not yet committed to L1. And parent hashes
only prove that a range is one chain, not that it is the canonical one. The legacy range has a
trusted hash at its top (section 3.2); a range that reaches the present needs one too.

- **End of a range that reaches the present:** the L2 block of the newest dispute game the
  chain's `DisputeGameFactory` created on L1 (the factory's `DisputeGameCreated` logs of the
  last day, every game type; the claim is read from the calldata of the `create` call). The
  game is recorded with the plan in the state directory, so `verify` and `load` stay offline
  and a resumed download keeps the same end. The lookup uses the service's L1 endpoint, so it
  counts against the token's window like any download.
- **Two kinds of game, chosen by game type** from a table in `op-indexer-chainspec`
  (`claim_format`): a fault dispute game (types 0, 1, 2, 3, 8) names an L2 block number and
  its root claim is that block's output root; a super fault dispute game (types 4, 5, 7, 9;
  OP Mainnet creates type 9 as of 2026-10) carries the preimage of a super root, a timestamp
  and one output root per chain, and its root claim is the hash of that preimage. The claim
  for this chain is the output root next to its chain id, about its block at that timestamp
  (found from the Bedrock block, its time and the block time in the chain specification), and
  `verify` also requires the block to have that timestamp. A newest game of a type that is
  not in the table is refused by name, with the types the factory created.
- **The check is mandatory.** `verify` computes the output root of that block from its
  downloaded header, `keccak256(bytes32(0) ‖ state root ‖ withdrawals root ‖ block hash)`, and
  requires it to equal the game's root claim. A mismatch fails `verify` with both values, and
  nothing is loaded. The block's hash is then the top anchor, in addition to the link to the
  block below the range.
- **What it proves:** the whole range is the chain a game on L1 claims; the proposer proposes
  blocks that are already safe, so the range is committed to L1. The import ends up to about
  an hour behind the safe head (OP Mainnet creates a game about hourly).
- **What it does not prove:** that the claim is right. A new game is a bonded claim nobody
  has challenged yet.
- **It is a consistency check, not an independent proof.** The game is read through the same
  provider as the blocks (HyperSync's L1 endpoint). A provider that served a wrong chain could
  serve a matching wrong game. Against that, give `--last-block` with an `--anchor-hash` taken
  from a source you trust.
- **Before Isthmus** the header does not carry the message passer's storage root, so the
  output root cannot be computed from what is downloaded and a game cannot anchor such a
  block. A range that ends there needs the trusted hash of its last block instead.
- A game not created by a plain call of the factory's `create` with a one-word extra data
  (created through another contract, or another kind of game) is refused with a message
  rather than misread.
- From the chain specification: the factory (OP Mainnet:
  `0xe5965Ab5962eDc7477C8520243A95517CD252fA9`, superchain registry,
  `superchain/configs/mainnet/op.toml`, `DisputeGameFactoryProxy`), the Bedrock block and its
  time, and the block time. Which game type the chain's portal respects is stored on L1 and
  cannot be read through logs, so the newest game of any known type is used; the factory's
  recent games on OP Mainnet are all type 9 (seen 2026-10-03). The extra-data encodings are
  from `FaultDisputeGame.sol`, `SuperFaultDisputeGame.sol` and `Encoding.sol` in the Optimism
  monorepo.
- `--legacy-only` (0 to 105,235,062 on OP Mainnet) uses the trusted hash of the last legacy
  block from the chain specification and needs no lookup.
