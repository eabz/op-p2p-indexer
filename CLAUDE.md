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
| `bin/indexer` | `indexer` | The full node for a single user: thin binary over `crates/node` with the fjall archive | node, storage |
| `bin/server` | `server` | The full node for serving at scale: thin binary over `crates/node` with history read from R2 (`crates/server`); `--export` (or `OP_INDEXER_EXPORT=true`) makes it the deployment's single exporter | node, server, chunks, storage, chainspec |
| `crates/node` | `op-indexer-node` | Shared node wiring for `indexer` and `server`: config (`Config::from_env`), `.env` loading, tracing, startup, shutdown | chainspec, p2p, el, l1, storage, pipeline, stream, primitives |
| `crates/server` | `op-indexer-server` | The server's committed store: R2-backed `ArchiveStore` (sealed chunks through `crates/chunks` plus a local fjall tail) and the exporter | chainspec, chunks, primitives, storage |
| `bin/balancer` | `balancer` | The directory for a fleet of servers: thin binary over `crates/balancer` | balancer, chainspec, chunks, runtime |
| `crates/balancer` | `op-indexer-balancer` | Servers register and heartbeat (health, heads, load); Arrow Flight `GetFlightInfo` splits a range into per-chunk jobs on the least-loaded servers; `Locate` for subscriptions; the registration client the server uses. No block data passes through it | api, chainspec, chunks |
| `bin/op-indexer-import` | `op-indexer-import` (binary `import`) | Command-line importer, a separate process: downloads a block range from an external archive (Envio HyperSync), then `verify` checks every block (hashes, roots, senders), seals it into chunks and uploads them to R2, deleting each downloaded chunk once uploaded; the chunks are listed in the manifest only when the range matches its anchor | chainspec, primitives, chunks |
| `crates/chunks` | `op-indexer-chunks` | Sealed block chunks in object storage (Cloudflare R2 through `object_store`'s S3 API): chunk format (zstd segments, index, footer), the hash-chained manifest, the global hash index, and the client; written by `import verify` and the server's exporter, read by `server` | primitives, chainspec |
| `crates/runtime` | `op-indexer-runtime` | Shared environment loading, tracing and shutdown signals, without node dependencies | none |
| `crates/api` | `op-indexer-api` | Shared API keys and Flight tickets, without storage dependencies | none |
| `crates/primitives` | `op-indexer-primitives` | Shared domain types (alloy and op-alloy only) | none |
| `crates/chainspec` | `op-indexer-chainspec` | Every per-chain value, for each supported chain (OP Mainnet, Unichain, Base): chain id, sequencer signer, consensus and execution bootnodes, execution discovery identity, genesis, fork blocks and the per-chain list of time forks, block time, dispute game factory and its game types. The one exception is the importer's HyperSync endpoint, which stays in the importer | none |
| `crates/p2p` | `op-indexer-p2p` | discv5 discovery, gossipsub block gossip (scoring, connection limits), unsafe-block validation, fjall node state (identity, saved peers and sync progress, for `el` and `l1` too) | primitives, chainspec |
| `crates/storage` | `op-indexer-storage` | Unsafe store (in memory, fork choice, journaled to fjall) and the block archive (fjall), which is the committed store: committed blocks with their senders and the committed L1 heads; their traits; the retry policy | primitives |
| `crates/pipeline` | `op-indexer-pipeline` | Unsafe blocks → unsafe store; promote safe/finalized → archive | primitives, storage |
| `crates/el` | `op-indexer-el` | Execution p2p (devp2p): discovery, sessions, receipts of new blocks, serving the archive to peers, range sync; pre-Bedrock blocks shared only with other op-p2p-indexers | primitives, chainspec |
| `crates/l1` | `op-indexer-l1` | L1 commitment without an RPC: from L1 block hashes a beacon light client vouches for, finds and verifies the dispute games created for the chain, giving the L2 blocks claimed on L1 | el, chainspec, primitives |
| `crates/stream` | `op-indexer-stream` | gRPC server (tonic, prost): subscriptions to history from the archive then the live chain from the unsafe store, decoded or raw, with block status and reorgs; heads and block lookups | primitives, storage |

- Keep these edges: `p2p` and `storage` never depend on each other, and `pipeline` doesn't depend
  on `p2p`. `el` depends on none of `p2p`, `storage` and `pipeline`, and `l1` on none of `storage`,
  `pipeline` and the importer; `stream` on none of `p2p`, `el`, `l1` and `pipeline`. Nothing about an external API
  (HyperSync or any other) may appear outside `bin/op-indexer-import`, with one exception: object
  storage (Cloudflare R2, through the S3 API) in `crates/chunks`, which the importer's export and
  the `server` binary use. `p2p` depends on `chainspec` (it is chain-specific); the binary parses overrides (e.g.
  bootnodes) at the edge. The binary wires them together with channels.
- Safe/finalized status comes from the `l1` crate (off by default), not from reth or an RPC (see `docs/l1.md`).
- New crates go in `crates/<name>` as package `op-indexer-<name>`, inherit `[workspace.package]`,
  and set `lints.workspace = true`. All dependency versions live in root `[workspace.dependencies]`.

## Storage

- **Unsafe chain** (in memory, journal `unsafe/` in the data dir): blocks received over gossip and not yet committed to L1, with fork choice; replayed from the journal on start, capped by `OP_INDEXER_UNSAFE_MAX_BYTES`.
- **fjall** (`archive/` in the data dir): the committed store. Every committed block (the whole
  history: there is no retention window), in its consensus encoding, with its transaction senders
  and the committed L1 heads. It is what the node serves to peers and streams to consumers. Embedded; needs no service.

There is no Redis and no ClickHouse: the binary needs no other service. At startup it opens the archive and the unsafe chain's journal (replaying it), and exits if either belongs to another chain. Optional `OP_INDEXER_PROFILE=live|archive|fleet` selects capability defaults; explicit environment values override them, and unset preserves legacy defaults. Settings are `OP_INDEXER_*` environment variables, which every binary also reads from `.env` in the current directory (or `--env-file <path>`); the process environment wins. `.env.example` lists every variable of every binary, and the node's are documented on `Config::from_env` in `crates/node/src/config.rs`. There is no Docker setup for now.

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
