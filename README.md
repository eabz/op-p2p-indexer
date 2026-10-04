# op-p2p-indexer

A self-hosted source of OP Stack chain data. It syncs blocks, transactions, receipts and logs from
the chains' own peer-to-peer networks, verifies everything against the block hashes the sequencer
signs and L1 commits to, keeps the history in a local archive, and streams it to consumers over
gRPC and Apache Arrow Flight. It calls no RPC and needs no database besides Redis.

It also tries to leave the networks it reads from better off: it serves the blocks it holds to
other nodes, follows each network's peer rules, and shares pre-Bedrock history with other
op-p2p-indexers.

Supported chains: **OP Mainnet** (10) and **Unichain** (130).

## How it works

| Part | What it does |
|---|---|
| Consensus p2p (`crates/p2p`) | Joins the OP Stack gossip network, validates sequencer-signed blocks, serves `payload_by_number` to older op-nodes |
| Execution p2p (`crates/el`) | Joins the execution network (devp2p, eth/68 and eth/69): fetches each block's receipts and verifies them against the header, serves headers, bodies and receipts, and syncs missing ranges |
| L1 (`crates/l1`, optional) | A beacon light client plus L1 execution peers: finds the chain's dispute games on Ethereum and marks blocks safe and finalized |
| Storage (`crates/storage`) | Redis for the unsafe tip, a fjall block archive for committed history |
| Stream (`crates/stream`) | gRPC subscriptions (history, then the live chain, with reorgs) and Arrow Flight tables (`blocks`, `transactions`, `receipts`, `logs`) |
| Importer (`bin/op-indexer-import`, binary `import`) | A separate process that fills the archive once from an external archive (Envio HyperSync), verifying every block |

## Quick start

Requirements: a recent stable Rust, Docker (for Redis), and for a public node open ports 9222
(gossip) and 30303 (execution p2p), TCP and UDP.

```bash
docker compose up -d redis
cargo build --release
OP_INDEXER_EL_ENABLED=true ./target/release/op-indexer
```

The node keeps its state in `data-op/` (`data-unichain/` for Unichain): its identity, known
peers and the block archive. Every setting is an `OP_INDEXER_*` environment variable, all of them
documented on `Config::from_env` in [`bin/op-indexer/src/config.rs`](bin/op-indexer/src/config.rs).
Some useful ones:

| Variable | Default | |
|---|---|---|
| `OP_INDEXER_CHAIN_ID` | `10` | `130` for Unichain |
| `OP_INDEXER_EL_ENABLED` | `false` | Fetch receipts and serve the execution network |
| `OP_INDEXER_EL_SYNC` | `false` | Fill gaps in the archive from execution peers |
| `OP_INDEXER_L1_ENABLED` | `false` | Safe and finalized heads from L1; needs `OP_INDEXER_L1_CHECKPOINT` and range sync |
| `OP_INDEXER_STREAM_LISTEN_ADDR` | `127.0.0.1:50051` | The stream has no authentication: keep it local or behind a proxy |
| `OP_INDEXER_ADVERTISED_ADDR`, `OP_INDEXER_EL_ADVERTISED_ADDR` | unset | Your public `ip:port`, when behind NAT |

With Docker, `docker compose up -d` runs Redis and the node; `unichain.env.example` shows a second
instance for Unichain next to the first. See [storage.md](docs/storage.md), "Several instances on
one host".

## Filling the archive

Syncing all history from peers is slow, and nobody but other op-p2p-indexers serves OP Mainnet's
pre-Bedrock blocks. The importer fills the archive once instead:

```bash
export ENVIO_API_TOKEN=...
./target/release/import --state-dir import-state download   # fetches the chain up to the newest dispute game
./target/release/import --state-dir import-state verify     # offline: rebuilds and checks every block
./target/release/import --state-dir import-state load       # writes data-<chain>/archive, checking every sender
```

Each step resumes where it stopped. Nothing reaches the archive unless `verify` proved it against
the block hashes and the dispute game on L1. See [import.md](docs/import.md).

`scripts/archive-sync.sh push|pull op|unichain` mirrors a finished archive to or from Cloudflare R2
with rclone, so another host can start from it without importing.

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
Unichain. The node's gossip ingest and receipt fetching have run against live peers. Serving
peers, range sync, the L1 side end to end, the stream and Arrow Flight are built and reviewed but
not yet run in production. [citizenship.md](docs/citizenship.md) lists every duty on every network
with its spec and status.

## Documentation

Start with [roadmap.md](docs/roadmap.md): scope, decisions and the order of work. Each part has its
own spec: [storage](docs/storage.md), [pipeline](docs/pipeline.md), [execution p2p](docs/el.md),
[L1](docs/l1.md), [importer](docs/import.md), [stream](docs/stream.md),
[good-peer duties](docs/citizenship.md).

## License

MIT, see [LICENSE](LICENSE).
