# Import spec (`bin/op-indexer-import`)

Status: **agreed 2026-10-04, being built** on `feat/el` (user: all of this work stays on one branch).

**Goal (user, 2026-10-04): sync the whole chain from HyperSync as a separate process, usable
for any chain, into a local store that the node serves over p2p; then test a normal p2p sync
against that node.** The legacy range (section 1) verifies today and is built first. The
range from the Bedrock block onward is section 9 and depends on a test that is running.

OP Mainnet's blocks before the Bedrock upgrade (0 to 105,235,062) cannot be re-executed by a
modern EVM, and no execution peer we reached serves them (`docs/el-viability.md`, on the `el`
branch). This binary fetches them once from Envio HyperSync, verifies every block, and loads
them into the committed store. It keeps what it downloaded, so later steps can be repeated
without HyperSync.

It is a separate binary: the indexer never links it and never talks to an external service.
The HyperSync dependency belongs to this package only.

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
the window. 105.2 million blocks in 30 minutes is about 58,000 blocks per second. On a 1 Gbit
line at most about 225 GB arrives in that time, so:

- the transfer must be compact (binary or compressed responses, only the fields needed);
- requests must run in parallel (the server work alone is about 130 minutes);
- nothing but downloading happens during the window;
- the download must resume, because one window may not be enough.

## 3. Phases

Each phase is a subcommand, reads and writes a state directory, and can be stopped and run
again; a completed step is never redone.

### 3.1 `download`

- Splits the range into fixed chunks of blocks (1,000 by default) and fetches them with a
  configurable number of requests in flight.
- A HyperSync response may cover less than the range asked for; the chunk is complete only
  when every block of it has arrived.
- Each complete chunk is written compressed to the state directory, atomically (temporary
  file, then rename), so a file that exists is complete. A rerun fetches only missing chunks.
- Fields requested: everything needed to rebuild the header, the transaction and the receipt
  with its logs, plus the old client's L1 fee fields. The bloom filters are not requested:
  they are recomputed from the logs, and the header hash check proves them.
- Errors: a failed request is retried with capped, jittered backoff; when the service starts
  refusing for rate limits the phase stops with a summary of what is missing, to be run again
  after the window is reset. It never spins.
- Prints progress (chunks done, blocks per second, bytes per second, time left at this rate)
  at a fixed interval.

### 3.2 `verify`

Offline; reads the downloaded chunks only.

- Rebuilds each header and checks that it hashes to its block hash and that each block's
  parent hash is the previous block's hash, from the last legacy block down to block 0.
- The anchor is the Bedrock block: 105,235,062's hash must equal the parent hash of block
  105,235,063 (hash `0xdbf6a80f…afd3`), a constant with its source documented.
- Rebuilds each transaction, checks its hash, and checks the transactions root.
- Rebuilds each receipt (bloom from its logs) and checks the receipts root.
- Recovers the sender of every signed transaction. A zero-signature transaction (an
  L1-to-L2 message) gets the zero address as sender; they are counted.
- CPU work runs in parallel, off the async runtime. A chunk that fails stops the phase with
  the block number and the check that failed; the chunk can be deleted and downloaded again.
- Records which chunks are verified.

### 3.3 `load`

- **By default `load` writes to the local archive only** (section 4): the fjall store the
  node serves peers from. It needs no database.
- Loading the committed store (ClickHouse) is optional and happens only when its settings are
  given: verified chunks become `DecodedBlock`s with `BlockSource::Import` and go through
  `CommittedStore::insert`, in batches. It can be done later from the same chunks, without
  HyperSync and without touching the archive again.
- Progress is recorded per target, so each resumes on its own.
- The old client's L1 fee fields are not loaded anywhere: no root covers them. They stay in
  the downloaded chunks.

## 4. The local history store, for serving

`load` also appends every verified block, in order from the first block of the range, to the
local block archive (`ArchiveStore`, fjall) in its consensus encoding: the store the node
serves peers from. The archive holds one contiguous range, so the import builds it upward
from block 0 and the running indexer continues it at the tip once the two meet.

- The archive gets a batch append (`ArchiveStore::append_batch`): one synced write for many
  blocks. One synced write per block would take days for a hundred million blocks.
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
  `--clickhouse-database`, `--clickhouse-user`, `--clickhouse-password` and `--chain-id`
  apply. Redis is never needed.
- The two targets keep separate records per chunk (`<state>/loaded/<chunk>.archive` and
  `<chunk>.clickhouse`), so ClickHouse can be loaded on a later run from the same verified
  chunks without redoing the archive.

### How to run it

Build on any machine with the Rust toolchain and copy the one file:

```bash
cargo build --release -p op-indexer-import
```

The file is `target/release/op-indexer-import`. On the machine that runs it, with the defaults
(OP Mainnet, blocks 0 to 105,235,062, state in `./import-state`):

```bash
op-indexer-import download --api-token <TOKEN> --requests 64
```

```bash
op-indexer-import verify
```

```bash
op-indexer-import load --clickhouse-url http://127.0.0.1:8123 --archive-dir data/archive
```

`op-indexer-import --help` and `<command> --help` list every flag, its environment variable
and its default. Range flags (`--state-dir`, `--first-block`, `--last-block`, `--anchor-hash`,
`--chunk-blocks`) must be the same for every step.

- **State directory**: `raw/` holds one compressed file per downloaded chunk (the service's
  answers as received), `verified/` one file per verified chunk (the consensus encodings that
  passed), `loaded/` one marker per loaded chunk. A file exists only when its chunk is
  complete; `*.tmp` files are leftovers of an interrupted write and are overwritten.
- **Resuming**: when the request window closes, `download` stops with a summary of how many
  chunks are missing. Reset the window and run the same command again: only missing chunks are
  fetched. Ctrl-C stops any step cleanly; run it again to continue.
- **A chunk that fails `verify`**: the error names the block, the check and the file. Delete
  that file from `raw/` and run `download` again.
- **Disk**: measured on 1,000 blocks around block 50,000,000, a downloaded chunk is about
  0.8 KB per block compressed and a verified one about 0.5 KB per block: roughly 85 GB and
  52 GB for the whole legacy range if that sample is typical, plus ClickHouse and the archive.
  `raw/` can be deleted once `verify` reports the range verified up to the anchor.

## 6. Sizing (estimates until the rehearsal measures them)

- Downloaded chunks: 100 to 160 GB compressed.
- ClickHouse: tens of GB.
- Verification: one signature recovery per signed transaction, about 105 million in total.

## 7. Rehearsal before the real window

A short run on a small range that measures: bytes per block in the chosen transfer format,
how many requests in flight the service accepts, blocks per second achieved, and the time
`verify` and `load` take per million blocks. Its numbers replace section 6 and decide the
concurrency for the real run. It uses up a window, which the user then resets.

## 8. Not in this PR

The senders and L1 metadata of L1-to-L2 messages, serving and backfill over p2p (`el`), any
other source than HyperSync (the source sits behind one small trait so an RPC or the published
legacy archive can be added).

## 9. From the Bedrock block onward

HyperSync does not return three fields of deposit transactions (source hash, mint,
system-transaction flag) or the deposit nonce of their receipts, and every block from Bedrock
on starts with a deposit. A block can only be imported if its deposits can be rebuilt so that
each transaction hashes to its reported hash and both roots match the header:

- *The L1-attributes deposit* (first transaction of every block): its missing fields follow
  from protocol rules and from its own calldata.
- *User deposits* (bridged from L1): the source hash needs the L1 block hash and log index of
  the deposit event, and the mint value is in that event; both would come from the bridge
  contract's logs on L1, which HyperSync also serves.
- *Deposit receipts*: the deposit nonce must be recoverable from the fields returned.

Status: being tested on saved responses. If a block cannot be rebuilt and verified, it is not
imported; the importer never stores data it could not verify.

## 10. Any chain

Everything chain-specific is configuration, not code: the HyperSync endpoint, the block
range, the anchor (a trusted block hash the range must link to), the fork activations that
change encodings, and for OP Stack chains the L1 endpoint and bridge contract. A chain without
a legacy era or deposits needs only the endpoint, the range and the anchor.
