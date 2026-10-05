# op-p2p-indexer

A Rust indexer for the OP Stack peer-to-peer network. It joins the libp2p gossip network,
validates and decodes payloads with alloy / op-alloy types, and indexes them. It is a
**standalone, lightweight binary with no external services**: no L1 or L2 RPC and no embedded
node. Receipts and logs come from L2 execution peers, verified against the block header. The
node is a source of data: consumers subscribe to it over gRPC (the `stream` crate).

Current architecture is in [`docs/architecture.md`](docs/architecture.md), remaining work in
[`docs/roadmap.md`](docs/roadmap.md), and historical decisions in [`docs/decisions.md`](docs/decisions.md); each crate's
spec is linked from there (storage: [`docs/storage.md`](docs/storage.md)). Read the roadmap
before proposing a design, and record new decisions there.

## Layout (Cargo workspace)

| Path | Package | Role | Internal deps |
|---|---|---|---|
| `bin/indexer` | `indexer` | The full node for one user: thin binary over `crates/node` with the local fjall archive | node, runtime, storage |
| `bin/server` | `server` | The full node for serving at scale: thin binary over `crates/node` with sealed history read from R2 (`crates/server`), registered with a balancer; `--export` (or `OP_INDEXER_EXPORT=true`) makes it the deployment's single exporter | balancer, chainspec, chunks, node, runtime, server, storage |
| `bin/balancer` | `balancer` | The directory of a fleet of servers: thin binary over `crates/balancer` | balancer, chainspec, chunks, runtime |
| `bin/bench` | `bench` | Native Flight benchmark CLI and heavy multi-table workloads | bench, api, runtime |
| `crates/bench` | `op-indexer-bench` | Bounded Flight reads, strict plan/row checks and serializable benchmark reports; independent of storage and ingestion | api |
| `bin/op-indexer-import` | `op-indexer-import` (binary `import`) | Command-line importer, a separate process: `download` fetches a block range from an external archive (Envio HyperSync), `verify` checks every block (hashes, roots, senders), seals the blocks into chunks and uploads them to R2 (listed in the manifest only once the range matches its anchor); `fetch` downloads sealed chunks straight from R2 through a balancer's presigned URLs | chainspec, chunks, primitives, runtime |
| `crates/node` | `op-indexer-node` | Shared node wiring for `indexer` and `server`: configuration (`Config::from_env`) and machine-sized defaults, startup, the peer reports, supervision and shutdown | chainspec, el, l1, p2p, pipeline, primitives, runtime, storage, stream |
| `crates/server` | `op-indexer-server` | The server's committed store: an R2-backed `ArchiveStore` (sealed chunks through `crates/chunks`, a local fjall tail above them), its read-ahead and peer read budgets, and the exporter | chainspec, chunks, primitives, storage |
| `crates/balancer` | `op-indexer-balancer` | Servers register and heartbeat (health, heads, load, peers, what they served); Arrow Flight `GetFlightInfo` splits a range into per-chunk jobs on the least-loaded servers, or a `raw` plan of presigned chunk URLs; `Locate` for subscriptions; the registration client a server runs. No block data passes through it | api, chainspec, chunks |
| `crates/chunks` | `op-indexer-chunks` | Sealed block chunks in object storage (Cloudflare R2 through `object_store`'s S3 API, an optional public cached domain, or a local directory): the chunk format (zstd segments, index, footer), the hash-chained manifest, the global hash index, the client and its presigned URLs; written by `import verify` and the server's exporter, read by `server` and `import fetch` | chainspec, primitives |
| `crates/stream` | `op-indexer-stream` | gRPC (tonic, prost) and Arrow Flight server: subscriptions to history from the archive then the live chain from the unsafe store, decoded or raw, with block status and reorgs; heads and block lookups; Flight `DoGet` of a table over a block range | api, primitives, storage |
| `crates/pipeline` | `op-indexer-pipeline` | Gossiped blocks → unsafe store; promotion of safe/finalized blocks → archive; the receipts, missed-block (fill) and range-sync storing tasks | primitives, storage |
| `crates/storage` | `op-indexer-storage` | The unsafe store (in memory, fork choice, journaled to fjall) and the block archive (fjall), the committed store: committed blocks with their senders and the committed L1 heads; their traits; the retry policy | primitives |
| `crates/p2p` | `op-indexer-p2p` | Consensus p2p (libp2p): discv5 discovery, gossipsub block gossip (scoring, connection limits), unsafe-block validation, `payload_by_number` serving, and the fjall node state (identity, saved peers, sync progress, for `el` and `l1` too) | chainspec, primitives |
| `crates/el` | `op-indexer-el` | Execution p2p (devp2p): discovery, sessions, receipts of new blocks, missed-block fetch, range sync, and serving the archive to peers; blocks before Bedrock shared only with other op-p2p-indexers | chainspec, primitives |
| `crates/l1` | `op-indexer-l1` | L1 commitment without an RPC: a beacon light client and L1's execution network; from the L1 block hashes the light client vouches for, finds and verifies the chain's dispute games, giving the L2 blocks claimed on L1 | chainspec, el, primitives |
| `crates/chainspec` | `op-indexer-chainspec` | Every per-chain value, for each supported chain (OP Mainnet, Unichain, Base): chain id, sequencer signer, consensus and execution bootnodes, execution discovery identity, genesis, fork blocks and the per-chain list of time forks, block time, dispute game factory and its game types. The one exception is the importer's HyperSync endpoint, which stays in the importer | none |
| `crates/api` | `op-indexer-api` | Shared API keys and Flight tickets, without storage dependencies | none |
| `crates/runtime` | `op-indexer-runtime` | Shared process setup without node dependencies: `.env` loading, tracing, shutdown signals, service commands (start/stop/status/logs), the machine's cores and memory | none |
| `crates/primitives` | `op-indexer-primitives` | Shared domain types (alloy and op-alloy only) | none |

- Keep these edges: `p2p` and `storage` never depend on each other, and `pipeline` doesn't depend
  on `p2p`. `el` depends on none of `p2p`, `storage` and `pipeline`, and `l1` on none of `storage`,
  `pipeline` and the importer; `stream` on none of `p2p`, `el`, `l1` and `pipeline`. Nothing about an external API
  (HyperSync or any other) may appear outside `bin/op-indexer-import`, with one exception: object
  storage (Cloudflare R2, through the S3 API) in `crates/chunks`, which the importer, the
  `server` and the `balancer` use. `p2p` depends on `chainspec` (it is chain-specific); the binary parses overrides (e.g.
  bootnodes) at the edge. The binary wires them together with channels.
- Safe/finalized status comes from the `l1` crate (off by default), not from reth or an RPC (see `docs/l1.md`).
- New crates go in `crates/<name>` as package `op-indexer-<name>`, inherit `[workspace.package]`,
  and set `lints.workspace = true`. All dependency versions live in root `[workspace.dependencies]`.

## Storage

- **Unsafe chain** (in memory, journal `unsafe/` in the data directory): blocks received over gossip and not yet committed to L1, with fork choice; replayed from the journal on start, capped by `OP_INDEXER_UNSAFE_MAX_BYTES` (sized from the machine by default).
- **Archive** (fjall, `archive/` in the data directory, `indexer`): the committed store. Every committed block (the whole history: there is no retention window), in its consensus encoding, with its transaction senders and the committed L1 heads. It is what the node serves to peers and streams to consumers. Embedded; needs no service.
- **Sealed chunks** (`server`): the committed history in object storage (R2), read through `crates/chunks`, with a local fjall tail of the committed blocks above the last sealed chunk ([`docs/serving.md`](docs/serving.md)).
- **Node state** (fjall, `node/` in the data directory): identity keys, saved peers and range sync progress.

There is no Redis and no ClickHouse: the binaries need no other service. At startup a node opens its committed store and the unsafe chain's journal (replaying it), and exits if either belongs to another chain. Settings live in a private per-chain `config.toml`, with shared `[r2]` and role sections. CLI overrides process environment, then TOML, then machine defaults. `--config` selects a file; `--chain` selects `~/indexer/<chain>/config.toml`. Relative paths are resolved beside that file. The shared runtime validates keys/types and overlays values without mutating the process environment. `.env`/`--env-file` remains a deprecated transition path. `config.toml.example` is the schema guide; `docs/configuration.md` documents migration and all environment overrides. Tuning knobs are sized from cores and memory (`crates/node/src/sizing.rs`). Keep removed or renamed variables working for one release through `op_indexer_runtime::deprecated`. The installer configures distinct role/chain state and ports, installs named systemd services, and never removes data on unregister. Optional `OP_INDEXER_PROFILE=live|archive|fleet` selects capability defaults. L1 turns on range sync, which turns on the execution network; explicit values override both. Run in the background with `<binary> start|stop|restart|status|logs` or `install-service` (systemd). There is no Docker setup for now.

## Project skills

Before writing, editing, or reviewing Rust, load **`rust-style`** with the Skill tool. Also load
each of `rust-errors`, `rust-docs`, and `rust-async` whose description matches the work. If a skill's
convention conflicts with what the code needs, raise it with the user instead of silently deviating.

## Tooling (enforced, don't work around it)

- `[lints]` in `Cargo.toml`, `clippy.toml`, `rustfmt.toml`, and `deny.toml` hold the mechanical rules.
- Hooks (`.claude/settings.json`, `.claude/hooks/`):
  - **After each edit of a `.rs` file**: `cargo fmt --all` runs.
  - **When finishing a turn**: if Rust sources changed, `cargo fmt` and
    `cargo clippy --all-targets -D warnings` run, plus `cargo deny check` when manifests changed.
    Finishing is blocked until they pass.
  - Don't run clippy manually just to check; the hook does it.
- Not covered by hooks, so run it yourself when relevant: `cargo doc` (see `rust-docs`).
- **No tests for now** (user decision): don't add `#[cfg(test)]` modules or dev-dependencies. Verify by running the node instead.
- **Prefer maintained crates** (libp2p, discv5, alloy, ...) over hand-written protocol code, and keep the code slim: remove anything unused.

## Finishing a task

When a task that changed code is complete:

1. **Run `/simplify`** (the `simplify` skill) on the changes and apply its fixes.
2. Summarize what changed for the user.

Skip step 1 only for tasks that changed no code (questions, planning, docs-only edits).

## graphify

A knowledge graph of the code lives in `graphify-out/` (gitignored; build it with `graphify update .`).
It is optional orientation, not a source of truth:

- It lists every declaration (what exists and where), so `graphify query "<question>"` and
  `graphify explain "<concept>"` are a quick way to find the right file.
- Its call graph is incomplete: it misses most calls across modules and functions passed as
  values. For "what calls this" or tracing a flow, read the source (or use find-references).
- After modifying code, run `graphify update .` to keep it current (AST-only, no API cost).
