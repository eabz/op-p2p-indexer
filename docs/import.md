# Import spec (`bin/op-indexer-import`)

Status: **built on `feat/el`; the legacy range has been downloaded once on the real service
(2026-10-03); everything from the Bedrock block on is untested against real data.**

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

The API token allows unlimited requests for **30 minutes from first use**; the user can reset
the window. So nothing but downloading happens during the window, as few bytes as possible
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
- **What is not proven: senders.** No signature is checked: one recovery per transaction was
  most of the work of `verify`, and the bytes served to peers contain no senders. The sender
  written to the optional ClickHouse rows is the `from` HyperSync reports, trusted as given. A
  transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client before
  Bedrock) has no signer and gets the zero address; they are counted.
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
  `verify` without the flag must still run before `load`.

### 3.3 `load`

- **By default `load` writes to the local archive only** (section 4): the fjall store the
  node serves peers from. It needs no database.
- Loading the committed store (ClickHouse) is optional and happens only when its settings are
  given: verified chunks become `DecodedBlock`s with `BlockSource::Import` and go through
  `CommittedStore::insert`, in batches. It can be done later from the same chunks, without
  HyperSync and without touching the archive again.
- It loads only the range `verify` accepted (`verified.json`).

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
- Measured on this machine (Apple M-series, 10 cores, internal SSD), with the user's sample
  chunks repeated into a long valid chain (2026-10-04):

  | | before (`append_batch`, 16 MiB) | after (bulk, 1 GiB) |
  |---|---|---|
  | legacy (2.2 KB/block) | 82,000 blocks/s, 187 MB/s RLP | 650,000 blocks/s, 1.5 GB/s RLP |
  | post-Bedrock (71 KB/block) | 5,000 blocks/s, 365 MB/s RLP | 32,000 blocks/s, 2.35 GB/s RLP |
  | bytes written / RLP | 0.68 (post), 1.11 (legacy) | 0.31 (post), 0.55 (legacy) |

  Bytes written are what the files hold (about 1.0 to 1.07 of the final directory size):
  values are written once and the number-keyed trees are moved, not rewritten, by
  compaction; only `numbers` (hash to number, about 40 bytes a block) is merged. On a disk
  that writes 325 MB/s the bulk path is then limited by the disk for post-Bedrock blocks
  (about 1 GB/s of RLP), and by preparation on the cores for legacy ones.
- Progress lines give `secs_left` from the bytes of the verified files still to read, not
  from blocks, and `mb_per_sec` of RLP appended.
- A crash or a kill leaves the archive holding a contiguous prefix: `load` resumes after its
  last block, writing again the blocks of the unfinished append. A failed append is not
  retried within the run (it can leave files only the next open removes): running `load`
  again resumes. On a stop (Ctrl-C), no new chunk is read; the chunks being read are
  appended, and `load` reports where it stopped. Checked by killing the
  load at random points (`kill -9`) and reading every block back.
- The archive's retention must be `all` on a node that serves history.
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
  directory, the chain, the endpoint, the block range, chunk size, requests in flight, and for
  `load` the ClickHouse settings and the archive directory.
- **By default the importer fills the local block archive the node serves from, and needs no
  database.** `load` appends the verified bytes to the archive directory (`--archive-dir`,
  default `data/archive`) and contacts nothing else.
- **ClickHouse is optional.** It is written only when `--clickhouse-url` is given
  (`OP_INDEXER_IMPORT_CLICKHOUSE_URL`); its migrations are then applied if missing, and
  `--clickhouse-database`, `--clickhouse-user` and `--clickhouse-password` apply. Redis is
  never needed.
- `load` needs the range accepted by `verify` (`verified.json`) and refuses anything else.
  What the archive holds is asked of the archive: `load` continues after its last block, and
  refuses an archive that does not start at the range's first block or holds another chain.
  ClickHouse keeps one marker per chunk (`<state>/loaded/<chunk>.clickhouse`), so it can be
  loaded on a later run from the same verified chunks without touching the archive.
- `load` exits with an error if it stops before the end of the range, and says what to run.

### How to run it

Build on any machine with the Rust toolchain and copy the one file:

```bash
cargo build --release -p op-indexer-import
```

The file is `target/release/op-indexer-import`. On the machine that runs it, with the defaults
(OP Mainnet, from block 0 to the last block committed to L1, state in `./import-state`):

```bash
op-indexer-import download --api-token <TOKEN> --requests 64
```

```bash
op-indexer-import verify
```

```bash
op-indexer-import load --archive-dir data/archive
```

`op-indexer-import --help` and `<command> --help` list every flag, its environment variable
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
  (SIGKILL, power loss) is as safe: a chunk file is written under a `.tmp` name and renamed,
  so a file with a final name is always complete, and leftover `.tmp` files are removed at
  the next start.
- **One process per state directory**: the directory is locked while a process runs; a second
  one exits with "another op-indexer-import process is using this state directory". The lock
  is released by the system when the process ends, however it ends.
- **Resuming**: when the request window closes, `download` stops with a summary of how many
  chunks are missing. Reset the window and run the same command again: only missing chunks are
  fetched. Ctrl-C stops any step cleanly; run it again to continue.
- **A chunk that fails `verify`**: the error names the block, the check and the file. Delete
  that file from `raw/` and run `download` again.
- **Disk**: a downloaded legacy chunk is 0.7 to 1.0 KB per block (zstd or gzip, as the
  service sends it) and a verified one about 0.5 KB per block: roughly 75 to 105 GB and 52 GB
  for the legacy range if the sample of section 6 is typical. Blocks from Bedrock on are many
  times larger and have not been measured. `raw/` can be deleted once `load` has finished.

## 6. Measured

Offline, on 1,000 saved blocks (50,000,000 to 50,000,999, one transaction each), Apple M-series:

- Size: 4.7 MB of JSON with the fields requested; 0.75 MB as zstd, 1.0 MB as gzip.
- `verify`, one thread: 0.17 s of processor time per 1,000 blocks with sender recovery, about
  0.06 s without (the build described here): reading and decoding 6 to 8 ms, parsing 10 to
  13 ms, receipts and their blooms 8 ms, the two tries 9 ms, header, body and receipts
  encoding 5 ms, transactions 1 ms, writing the verified chunk with its sync 17 to 22 ms (on
  macOS; a sync is cheaper on Linux).
- Projection, not a measurement: 105 million legacy blocks at 0.05 s per 1,000 are about 90
  core-minutes, 11 minutes on 8 cores if the disk keeps up.

On the real service (the user's run, 2026-10-03, 8 cores, 1 Gbit, the build before this one):
the legacy range downloaded at about 185,000 blocks per second with 64 requests in flight.
Nothing from the Bedrock block on has been measured.

## 7. The state directory

```text
<state>/plan.json                 the chain, the range, its anchor and the chunk size
<state>/verified.json             the range `verify` accepted; `load` requires it
<state>/raw/<from>-<to>.raw       downloaded chunk: the service's answers as they travelled
<state>/verified/<from>-<to>.blk  verified chunk: the consensus encodings that passed
<state>/loaded/<from>-<to>.clickhouse   marker: ClickHouse holds the chunk
<state>/lock                      held by the one process working on the directory
```

- `plan.json` is written by the first `download` and never changed: chain id, first and last
  block, the anchor (a trusted hash, or the dispute game found on L1) and the chunk size. Every
  later run of any step reads it; `verify` and `load` take no range flags. Files already
  written were cut by it, which is why it cannot change.
- A directory written by a build with another layout is refused: it either has chunks and no
  `plan.json`, or a `plan.json` with another version. Delete it and download again.
- A file exists only when it is complete: it is written under a `.tmp` name and renamed, and
  leftover `.tmp` files are removed at the next start.

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
  the deposit's event on L1, which this tool does not read. Whether the OP Mainnet endpoint
  fills these columns for every deposit is **not known**: no block from Bedrock on has been
  verified against real data.
- The system-transaction flag, which has no column, follows the protocol's rule: only the
  L1-attributes deposit before Regolith has it.
- A deposit receipt carries the sender's nonce and the receipt version from Canyon on, when
  they became part of the hashed receipt; before Canyon nothing the root does not cover is
  stored.
- Fork times and the Bedrock block come from `op-indexer-chainspec`.
- Verified offline on blocks 105,235,062 to 105,235,064 with an earlier build only. Not
  verified on any sample: typed transactions (the JSON form of `access_list` and
  `authorization_list` is assumed to be the Ethereum RPC's), user deposits, blocks from Canyon
  on. A block that cannot be rebuilt and verified is not imported.

## 10. Any chain

The chain is chosen with `--chain <id>` on the first `download`; its parameters (fork times,
the Bedrock block and its time, the hash of the last legacy block, the block time, the
dispute-game factory) come from `op-indexer-chainspec`, which today knows OP Mainnet. The
HyperSync endpoints of the chain and of its L1 are flags (`--endpoint`, `--l1-endpoint`).

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
