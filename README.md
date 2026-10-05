# op-p2p-indexer

A self-hosted source of OP Stack chain data. It syncs blocks, transactions, receipts and logs from
the chains' own peer-to-peer networks, verifies everything against the block hashes the sequencer
signs and L1 commits to, keeps the history in a local archive, and streams it to consumers over
gRPC and Apache Arrow Flight. It calls no RPC and needs no other service: no database, no RPC
endpoint.

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
| **`server`** | A full node for serving at scale: the same p2p participation and live data, but stateless for history: it keeps no archive and reads sealed, immutable block chunks from Cloudflare R2 on demand, with no cache. One server per deployment runs with `--export` (or `OP_INDEXER_EXPORT=true`) and is the single exporter, which seals finalized blocks into new chunks | R2 (sealed chunks), a small local tail of unsealed blocks | built; not yet run against R2 |
| **`importer`** | Fills history once from an external archive (Envio HyperSync): `download`, then `verify`, which checks every block and uploads it as sealed chunks to R2, deleting each downloaded chunk once uploaded | Its state directory | built (`import`) |
| **`balancer`** | The single entry point for users: keeps the table of which chunk ranges each server holds and points every request at the right server. No data passes through it | The chunk table | in progress |

R2 holds the sealed history the servers read. Live data always comes from the p2p networks,
and chunks are sealed only once all their blocks are finalized on L1, so they never change.

## How it works

| Part | What it does |
|---|---|
| Consensus p2p (`crates/p2p`) | Joins the OP Stack gossip network, validates sequencer-signed blocks, serves `payload_by_number` to older op-nodes |
| Execution p2p (`crates/el`) | Joins the execution network (devp2p, eth/68 and eth/69): fetches each block's receipts and verifies them against the header, serves headers, bodies and receipts, and syncs missing ranges |
| L1 (`crates/l1`, optional) | A beacon light client plus L1 execution peers: finds the chain's dispute games on Ethereum and marks blocks safe and finalized |
| Storage (`crates/storage`) | The unsafe tip in memory with a local journal, a fjall block archive for committed history |
| Stream (`crates/stream`) | gRPC subscriptions (history, then the live chain, with reorgs) and Arrow Flight tables (`blocks`, `transactions`, `receipts`, `logs`) |

## Quick start

Requirements: a recent stable Rust, and for a public node open ports 9222 (gossip) and 30303
(execution p2p), TCP and UDP.

```bash
cargo build --release
cp .env.example .env     # then edit it: at least OP_INDEXER_EL_ENABLED=true
./target/release/indexer
```

Every setting is an `OP_INDEXER_*` environment variable. Each binary loads `.env` from the
current directory at startup (or the file given with `--env-file <path>`); a variable already
set in the shell wins over the file. One file serves every binary:
[`.env.example`](.env.example) lists the variables of `indexer`, `server` and the importer with
their defaults. The node's are also documented on `Config::from_env` in
[`crates/node/src/config.rs`](crates/node/src/config.rs), the importer's in `import --help`.
Some useful ones:

| Variable | Default | |
|---|---|---|
| `OP_INDEXER_CHAIN_ID` | `10` | `130` for Unichain, `8453` for Base |
| `OP_INDEXER_EL_ENABLED` | `false` | Fetch receipts and serve the execution network |
| `OP_INDEXER_EL_SYNC` | `false` | Fill gaps in the archive from execution peers |
| `OP_INDEXER_L1_ENABLED` | `false` | Safe and finalized heads from L1; needs `OP_INDEXER_L1_CHECKPOINT` and range sync |
| `OP_INDEXER_STREAM_LISTEN_ADDR` | `127.0.0.1:50051` | Keep it local, behind a proxy, or set `OP_INDEXER_STREAM_API_KEYS` |
| `OP_INDEXER_P2P_ADVERTISED_ADDR`, `OP_INDEXER_EL_ADVERTISED_ADDR` | unset | Your public `ip:port`, when behind NAT |

The node keeps its state in `data-op/` (`data-unichain/`, `data-base/` for the others): its
identity, known peers, the block archive and the unsafe chain's journal. Several nodes can run
on one host (another chain, or another build): each with its own data directory, its own ports
and its own `.env`, run from its own directory or given with `--env-file`. The end of
`.env.example` shows the settings for Unichain and Base next to an OP Mainnet node; see
also [storage.md](docs/storage.md), "Several instances on one host".

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

## Consuming the data

- **gRPC** (`opindexer.v1.Stream`, [stream.proto](crates/stream/proto/opindexer/v1/stream.proto)):
  `Subscribe` from a block number, with history from the archive and then live blocks, each
  marked unsafe, safe or finalized, plus reorg and late-receipts events. Each subscription picks
  decoded records or the raw consensus encoding. `GetHeads` and `GetBlock` cover lookups.
- **Arrow Flight**, on the same port: `DoGet` with a ticket such as
  `logs:120000000:120010000:finalized` returns record batches for DuckDB, Polars, Spark and the
  like.

Details, schemas and limits: [stream.md](docs/stream.md).

## Status

The importer has downloaded and verified the full OP Mainnet chain (157.7 M blocks) and
Unichain; `verify` checks every block and uploads the history to R2 as sealed chunks, with
no local copy ([import.md](docs/import.md)).
The `server` is built but has not run against R2 yet; the `balancer` is in progress
([serving.md](docs/serving.md)). Base support is built but has not run yet. The node's gossip
ingest and receipt fetching have run against live peers. Serving
peers, range sync, the L1 side end to end, the stream and Arrow Flight are built and reviewed but
not yet run in production. [citizenship.md](docs/citizenship.md) lists every duty on every network
with its spec and status.

## Documentation

Start with [roadmap.md](docs/roadmap.md): scope, decisions and the order of work. Each part has its
own spec: [storage](docs/storage.md), [pipeline](docs/pipeline.md), [execution p2p](docs/el.md),
[L1](docs/l1.md), [importer](docs/import.md), [stream](docs/stream.md),
[serving at scale](docs/serving.md), [Base](docs/base.md),
[good-peer duties](docs/citizenship.md).

## License

MIT, see [LICENSE](LICENSE).
