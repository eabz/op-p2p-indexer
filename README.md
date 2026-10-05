# op-p2p-indexer

A self-hosted source of OP Stack chain data. It syncs blocks, transactions, receipts and logs from
the chains' own peer-to-peer networks, verifies everything against the block hashes the sequencer
signs and L1 commits to, keeps the history in a local archive, and streams it to consumers over
gRPC and Apache Arrow Flight. The running node calls no L1 or L2 RPC. The standalone `indexer` needs no external
database; fleet `server`s use object storage for sealed history.

It also tries to leave the networks it reads from better off: it serves the blocks it holds to
other nodes, follows each network's peer rules, and shares pre-Bedrock history with other
op-p2p-indexers.

Supported chains: **OP Mainnet** (10), **Unichain** (130) and **Base** (8453).

## Binaries

The project builds four programs. One machine serving one user needs only the first; serving
many users means running several servers behind one balancer.

| Binary | Role | Storage | Status |
|---|---|---|---|
| **`indexer`** | The full node for a single user: every service in one process. Takes part in the p2p networks, follows L1, keeps history and serves it over gRPC and Arrow Flight | Its own local block archive (fjall) | built |
| **`server`** | A full node for serving at scale: the same p2p participation and live data, but stateless for history: it keeps no archive and reads sealed, immutable block chunks from Cloudflare R2 on demand (no block cache; only the indexes of recently opened chunks are kept). One server per deployment runs with `--export` (or `OP_INDEXER_EXPORT=true`) and is the single exporter, which seals finalized blocks into new chunks | R2 (sealed chunks), a small local tail of unsealed blocks | built; run against R2 in a three-server bench ([serving.md](docs/serving.md)) |
| **`import`** (package `op-indexer-import`) | Fills history once from an external archive (Envio HyperSync), with an optional RPC for fields the archive leaves out: `download`, then `verify`, which checks every block and uploads it as sealed chunks to R2, deleting each downloaded chunk once uploaded (`run` does both); `fetch` downloads sealed chunks straight from R2 through URLs a balancer signs | Its state directory | built |
| **`balancer`** | The single entry point for users: tracks server health, contiguous coverage and load, directs clients to servers sharing the same sealed history, and signs R2 URLs for raw chunk downloads. No block data passes through it | In-memory registrations and the shared manifest | built; run in the bench |

R2 holds the sealed history the servers read. Live data always comes from the p2p networks,
and chunks are sealed only once all their blocks are finalized on L1, so they never change.

## How it works

| Part | What it does |
|---|---|
| Consensus p2p (`crates/p2p`) | Joins the OP Stack gossip network, validates sequencer-signed blocks, serves `payload_by_number` to older op-nodes |
| Execution p2p (`crates/el`) | Joins the execution network (devp2p, eth/68 and eth/69): fetches each block's receipts and verifies them against the header, serves headers, bodies and receipts, and syncs missing ranges |
| L1 (`crates/l1`, optional) | A beacon light client plus L1 execution peers: finds the chain's dispute games on Ethereum and marks blocks safe and finalized |
| Pipeline (`crates/pipeline`) | Gossiped blocks into the unsafe store, missed blocks and receipts attached, committed blocks promoted into the archive |
| Storage (`crates/storage`) | The unsafe tip in memory with a local journal, a fjall block archive for committed history |
| Serving at scale (`crates/server`, `crates/chunks`, `crates/balancer`) | Sealed chunks in R2 read by the servers, the exporter that seals new ones, and the balancer that directs clients |
| Stream (`crates/stream`) | gRPC subscriptions (history, then the live chain, with reorgs) and Arrow Flight tables (`blocks`, `transactions`, `receipts`, `logs`) |

## Quick start

Requirements: a recent stable Rust, and for a public node open ports 9222 (gossip) and 30303
(execution p2p), TCP and UDP.

```bash
cargo build --release
cp .env.example .env     # then edit it: e.g. OP_INDEXER_EL_ENABLED=true for receipts
./target/release/indexer
```

Every setting is an `OP_INDEXER_*` environment variable. Each binary loads `.env` from the
current directory at startup (or the file given with `--env-file <path>`); a variable already
set in the shell wins over the file. One file serves every binary.
[`.env.example`](.env.example) holds only what differs between machines: what to run,
secrets, and where things are, with a short required block per binary at the top. The tuning
knobs (Flight streams, memory, execution sessions) are sized from the machine's cores and
memory and logged at startup; the importer's options are flags (`import <step> --help`).
[docs/configuration.md](docs/configuration.md) has a minimal `.env` for each binary and, under
"Advanced settings", every other variable, the sizing formulas and the deprecated names. The
role switches:

| Variable | Default | |
|---|---|---|
| `OP_INDEXER_CHAIN_ID` | `10` | `130` for Unichain, `8453` for Base |
| `OP_INDEXER_EL_ENABLED` | `false` | Fetch receipts and serve the execution network |
| `OP_INDEXER_EL_SYNC` | `false` | Fill gaps in the archive from execution peers; turns the execution network on |
| `OP_INDEXER_L1_ENABLED` | `false` | Safe and finalized heads from L1; needs `OP_INDEXER_L1_CHECKPOINT`, turns range sync on |
| `OP_INDEXER_STREAM_LISTEN_ADDR` | `127.0.0.1:50051` | Keep it local, behind a proxy, or set `OP_INDEXER_STREAM_API_KEYS` |

`OP_INDEXER_PROFILE` (`live`, `archive` or `fleet`) sets the defaults of the three switches in
one line instead:

| Profile | Default capabilities | Requirements |
|---|---|---|
| `live` | Gossip and execution receipts | Execution peer connectivity |
| `archive` | Live ingestion, range sync and L1 tracking | Trusted `OP_INDEXER_L1_CHECKPOINT` |
| `fleet` | Archive capabilities in the `server` binary | Checkpoint and R2 configuration (or a local chunk directory) |

With no profile, the defaults above apply. Explicit `OP_INDEXER_EL_ENABLED`,
`OP_INDEXER_EL_SYNC` and `OP_INDEXER_L1_ENABLED` settings override profile defaults; an
explicit `false` that a switch needs fails at startup. The node logs the selected profile and
effective capabilities. Exporting is still explicit (`server --export`), and only one server
per deployment should export.

For initial fleet history, use `import run` to download and verify/upload in one invocation.
`import download` and `import verify` remain available for recovery and separate operation;
run `import run --help` for source and destination options.

The node keeps its state in `data-op/` (`data-unichain/`, `data-base/` for the others): its
identity, known peers, the block archive and the unsafe chain's journal. Several nodes can run
on one host (another chain, or another build): each with its own data directory, its own ports
and its own `.env`, run from its own directory or given with `--env-file`. See
[configuration.md](docs/configuration.md), "A second instance on the same host", and
[storage.md](docs/storage.md), "Several instances on one host".

## Install on a server

Each release has prebuilt Linux binaries (x86-64, glibc 2.35: Ubuntu 22.04 and newer) of
`indexer`, `server`, `import` and `balancer`, with `.env.example`, in one archive and its
SHA-256. To install the latest:

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/eabz/op-p2p-indexer/releases/latest | grep -m1 '"tag_name"' | cut -d'"' -f4)
NAME=op-p2p-indexer-$VERSION-x86_64-linux
curl -fsSLO "https://github.com/eabz/op-p2p-indexer/releases/download/$VERSION/$NAME.tar.gz"
curl -fsSLO "https://github.com/eabz/op-p2p-indexer/releases/download/$VERSION/$NAME.tar.gz.sha256"
sha256sum -c "$NAME.tar.gz.sha256"
tar -xzf "$NAME.tar.gz" && cd "$NAME"
cp .env.example .env     # then edit it
./indexer                # or ./server
```

To make a release, run `scripts/bump-version.sh patch` (or `minor`, `major`, `X.Y.Z`) on a
clean tree; it commits the new version and tags it. Pushing the tag
(`git push origin <branch> vX.Y.Z`) builds the archive and publishes the release
([`release.yml`](.github/workflows/release.yml)).

## Running in the background

`indexer`, `server` and `balancer` take service commands; with none, a binary runs in the
foreground as before (in tmux, say).

```bash
./server start            # detached; appends to server.log, writes server.pid
./server status           # running or not, pid, uptime, memory, the last log lines
./server logs -f          # the log, followed (Ctrl-C to leave)
./server stop             # SIGTERM, waits up to 30 s for a clean shutdown; --force kills after
./server restart          # stop, then start
```

- The files are next to the `.env` loaded (`--env-file`), or in the working directory: the
  log `<binary>.log` (or `--log-file <path>` / `OP_INDEXER_LOG_FILE`), the pid file
  `<binary>.pid`. The command goes first, before the binary's own arguments
  (`./server start --export`).
- `start` reports `started, pid N, log <path>` after 2 s, or, if the node exited at once, its
  error and the log's last lines. It refuses while a live pid file exists.
- **Restart on crash and on boot** (Linux, systemd): `./server install-service` writes
  `op-indexer-server.service` next to the log (`Restart=on-failure`, the working directory, the
  same arguments, output appended to the same log) and prints the commands to install it; it
  runs no `systemctl` itself. A node systemd runs is managed with `systemctl`, not
  `start`/`stop`.
- **Log size**: the binaries do not rotate their log. Use logrotate with `copytruncate` (the
  node keeps writing to the same file), e.g. in `/etc/logrotate.d/op-indexer`:

  ```
  /home/op/node/*.log {
      size 256M
      rotate 3
      copytruncate
      compress
      missingok
  }
  ```

## Consuming the data

- **gRPC** (`opindexer.v1.Stream`, [stream.proto](crates/stream/proto/opindexer/v1/stream.proto)):
  `Subscribe` from a block number or the head, with history from the archive and then live blocks, each
  marked unsafe, safe or finalized, plus reorg and late-receipts events. Each subscription picks
  decoded records or the raw consensus encoding. `GetHeads` and `GetBlock` cover lookups.
- **Arrow Flight**, on the same port (`127.0.0.1:50051` by default): `DoGet` with a ticket
  such as `logs:120000000:120010000:finalized` (at most 100,000 blocks) returns record batches
  for DuckDB, Polars, Spark and the like. Against a fleet, ask the balancer's `GetFlightInfo`
  for per-chunk tickets and their servers.

Details, schemas and limits: [stream.md](docs/stream.md).

## Status

All four programs are implemented and have run live: nodes on OP Mainnet and Unichain (on
Unichain, range sync from block 0, and L1 end to end: games matched, heads raised, blocks
promoted), and `server`s with a balancer on R2 in a three-server bench. Base has not been
deployed. The [roadmap](docs/roadmap.md) separates finished work, remaining implementation
and validation.

## Documentation

Start with [architecture.md](docs/architecture.md) for the current system, [roadmap.md](docs/roadmap.md) for next work, and [decisions.md](docs/decisions.md)
for historical decisions. Each part has its
own spec: [storage](docs/storage.md), [pipeline](docs/pipeline.md), [execution p2p](docs/el.md),
[L1](docs/l1.md), [importer](docs/import.md), [stream](docs/stream.md),
[serving at scale](docs/serving.md), [Base](docs/base.md),
[good-peer duties](docs/citizenship.md), [configuration](docs/configuration.md).

## License

MIT, see [LICENSE](LICENSE).
