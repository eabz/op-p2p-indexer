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
and its default. Range flags (`--state-dir`, `--first-block`, `--last-block`, `--anchor-hash`,
`--legacy-only`, `--chunk-blocks`) must be the same for every step.

- **The range's end**: with no range flags, `download` looks up the newest dispute game of
  the chain on L1 (through HyperSync's L1 endpoint, same token), ends the range at that game's
  L2 block, and records the game in `anchor.json` in the state directory. Every later run of
  any step uses the recorded game; `verify` checks the last block's output root against the
  game's claim. Delete `anchor.json` and run `download` again to extend the range to a newer
  game: chunks already there are kept. `--legacy-only` ends at block 105,235,062 with its
  known hash and needs no lookup; `--last-block <n> --anchor-hash <hash>` gives any end you
  trust; `--last-block <n> --allow-unanchored-top` goes without an anchor (also the fallback
  if the lookup fails).
- **A state directory filled by an earlier legacy run** is continued by a default run: chunks
  are files named by their block range, so every chunk already there is skipped. Only the
  short last chunk of the legacy range (`…105235000-…105235063`) has no counterpart in the
  longer range; its blocks are downloaded again as part of chunk `…105235000-…105236000`, and
  the short file is never read again (it can be deleted).
- **State directory**: `raw/` holds one compressed file per downloaded chunk (the service's
  answers as received), `verified/` one file per verified chunk (the consensus encodings that
  passed), `loaded/` one marker per loaded chunk. A file exists only when its chunk is
  complete; `*.tmp` files are leftovers of an interrupted write and are overwritten.
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

Status (2026-10-04): built, verified offline on blocks 105,235,063 and 105,235,064 only.

- `verify` rebuilds legacy, EIP-2930, EIP-1559, EIP-7702 and deposit transactions, their
  receipts, and headers of every fork (base fee, withdrawals root, blob fields, beacon root,
  and from Isthmus the hash of an empty requests list, which has no column in HyperSync).
- HyperSync's schema lists `source_hash`, `mint`, `deposit_nonce` and
  `deposit_receipt_version` columns; `download` requests them. Where a deposit's source hash
  is reported, the deposit is rebuilt from its row; the system-transaction flag is the one of
  the two values that gives the reported hash. Whether the OP Mainnet endpoint fills these
  columns is not known until the first request.
- Where it is not reported, the block's deposits are rebuilt by the protocol's rules
  (`deposit.rs`): the L1-attributes deposit from its own calldata, network upgrade deposits
  from their intents. User deposits then need the deposit contract's logs on L1, which are
  **not downloaded yet**: such a block fails `verify` with a named check.
- Before Canyon the deposit nonce is not part of the hashed receipt: the root is checked
  without it, and the stored receipt keeps the reported nonce, unproven.
- Fork times are flags (`--regolith-time`, `--canyon-time`, `--isthmus-time`), OP Mainnet by
  default. For a post-Bedrock range set `--first-block`, `--last-block` and `--anchor-hash`.
- Not verified on any sample: typed transactions (the JSON form of `access_list` and
  `authorization_list` is assumed to be the Ethereum RPC's), user deposits, blocks from Canyon
  on. A block that cannot be rebuilt and verified is not imported; the importer never stores
  data it could not verify.

## 10. Any chain

Everything chain-specific is configuration, not code: the HyperSync endpoint, the block
range, the anchor (a trusted block hash the range must link to), the fork activations that
change encodings, and for OP Stack chains the L1 endpoint and bridge contract. A chain without
a legacy era or deposits needs only the endpoint, the range and the anchor.

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
- The legacy-only default range (0 to 105,235,062) keeps its trusted hash and needs no lookup.
