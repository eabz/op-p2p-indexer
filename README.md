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
| **`server`** | A full node for serving at scale: the same p2p participation and live data, but its history is a local cache of immutable block chunks mapped to Cloudflare R2, filled from R2 only when a chunk is missing | Chunk cache backed by R2 | planned |
| **`importer`** | Fills history once from an external archive, verifying every block, and produces the chunks the archive and R2 are built from | Its state directory | built (`import`) |
| **`balancer`** | The single entry point for users: keeps the table of which chunk ranges each server holds and points every request at the right server. No data passes through it | The chunk table | planned |

R2 is only used to fill history. Live data always comes from the p2p networks, and chunks are
sealed only once all their blocks are finalized on L1, so they never change.

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
OP_INDEXER_EL_ENABLED=true ./target/release/indexer
```

The node keeps its state in `data-op/` (`data-unichain/`, `data-base/` for the others): its identity, known
peers, the block archive and the unsafe chain's journal. Every setting is an `OP_INDEXER_*`
environment variable, all of them documented on `Config::from_env` in
[`crates/node/src/config.rs`](crates/node/src/config.rs).
Some useful ones:

| Variable | Default | |
|---|---|---|
| `OP_INDEXER_CHAIN_ID` | `10` | `130` for Unichain, `8453` for Base |
| `OP_INDEXER_EL_ENABLED` | `false` | Fetch receipts and serve the execution network |
| `OP_INDEXER_EL_SYNC` | `false` | Fill gaps in the archive from execution peers |
| `OP_INDEXER_L1_ENABLED` | `false` | Safe and finalized heads from L1; needs `OP_INDEXER_L1_CHECKPOINT` and range sync |
| `OP_INDEXER_STREAM_LISTEN_ADDR` | `127.0.0.1:50051` | The stream has no authentication: keep it local or behind a proxy |
| `OP_INDEXER_ADVERTISED_ADDR`, `OP_INDEXER_EL_ADVERTISED_ADDR` | unset | Your public `ip:port`, when behind NAT |

With Docker, `docker compose up -d` runs the node; `unichain.env.example` shows a second
instance for Unichain next to the first. See [storage.md](docs/storage.md), "Several instances on
one host".

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

The importer has run on the full OP Mainnet chain (157.7 M blocks; a ~914 GB archive) and on
Unichain; history can be filled with it instead of syncing from peers ([import.md](docs/import.md)).
Base support is built but has not run yet. The node's gossip ingest and receipt fetching have run against live peers. Serving
peers, range sync, the L1 side end to end, the stream and Arrow Flight are built and reviewed but
not yet run in production. [citizenship.md](docs/citizenship.md) lists every duty on every network
with its spec and status.

## Documentation

Start with [roadmap.md](docs/roadmap.md): scope, decisions and the order of work. Each part has its
own spec: [storage](docs/storage.md), [pipeline](docs/pipeline.md), [execution p2p](docs/el.md),
[L1](docs/l1.md), [importer](docs/import.md), [stream](docs/stream.md), [Base](docs/base.md),
[good-peer duties](docs/citizenship.md).

## License

MIT, see [LICENSE](LICENSE).
