# op-p2p-indexer

A Rust indexer for the OP Stack peer-to-peer network. It joins the libp2p gossip network,
validates and decodes payloads with alloy / op-alloy types, and indexes them. It is a
**standalone, lightweight binary with no external services**: no L1 or L2 RPC and no embedded
node. Receipts and logs are planned to come from L2 execution peers, verified against the block
header (see the roadmap; pending a viability test).

Scope, decisions and the order of work are in [`docs/roadmap.md`](docs/roadmap.md); each crate's
spec is linked from there (storage: [`docs/storage.md`](docs/storage.md)). Read the roadmap
before proposing a design, and record new decisions there.

## Layout (Cargo workspace)

| Path | Package | Role | Internal deps |
|---|---|---|---|
| `bin/op-indexer` | `op-indexer` | Thin binary: config, tracing, wiring, shutdown | chainspec, p2p, el, storage, pipeline, primitives |
| `bin/op-indexer-import` | `op-indexer-import` | Command-line importer, a separate process: downloads a block range from an external archive (Envio HyperSync), verifies it, loads it into the block archive (and optionally ClickHouse) | chainspec, primitives, storage |
| `crates/primitives` | `op-indexer-primitives` | Shared domain types (alloy and op-alloy only) | none |
| `crates/chainspec` | `op-indexer-chainspec` | Static chain parameters (chain id, sequencer signer, bootnodes) | none |
| `crates/p2p` | `op-indexer-p2p` | discv5 discovery, gossipsub block gossip (scoring, connection limits), unsafe-block validation, fjall node state | primitives, chainspec |
| `crates/storage` | `op-indexer-storage` | Unsafe store (Redis, fork choice) / committed store (ClickHouse, migrations) / local block archive (fjall), their traits and metrics | primitives |
| `crates/pipeline` | `op-indexer-pipeline` | Unsafe blocks → unsafe store; promote safe/finalized → committed store and archive; the retry policy | primitives, storage |
| `crates/el` | `op-indexer-el` | Execution p2p (devp2p): discovery, sessions, receipts of new blocks, serving the archive to peers, range sync | primitives, chainspec |
| `crates/l1` | `op-indexer-l1` | L1 commitment without an RPC: from L1 block hashes a beacon light client vouches for, finds and verifies the dispute games created for the chain, giving the L2 blocks claimed on L1 | el, chainspec, primitives |

- Keep these edges: `p2p` and `storage` never depend on each other, and `pipeline` doesn't depend
  on `p2p`. `el` depends on none of `p2p`, `storage` and `pipeline`, and `l1` on none of `storage`,
  `pipeline` and the importer. Nothing about an external API
  (HyperSync or any other) may appear outside `bin/op-indexer-import`. `p2p` depends on `chainspec` (it is chain-specific); the binary parses overrides (e.g.
  bootnodes) at the edge. The binary wires them together with channels.
- Safe/finalized status will come from an L1 p2p crate, not from reth or an RPC (see `docs/roadmap.md`).
- New crates go in `crates/<name>` as package `op-indexer-<name>`, inherit `[workspace.package]`,
  and set `lints.workspace = true`. All dependency versions live in root `[workspace.dependencies]`.

## Storage

- **Redis**: the unsafe store. Blocks received over gossip and not yet committed to L1, with fork choice.
- **ClickHouse**: the committed store. Safe/finalized blocks, whose batches are on L1, and backfill.
- **fjall** (`archive/` in the data dir): a window of committed blocks, or optionally all of them,
  in their consensus encoding, for serving peers later. Embedded; needs no service.

`docker compose up -d redis clickhouse` starts both locally; `docker compose up --build` also runs
the indexer image. The binary needs both stores to start: it connects, checks the Redis key-layout
version and runs the ClickHouse migrations, and exits if either store is unreachable. Every
`OP_INDEXER_*` variable it reads is documented on `Config::from_env` in
`bin/op-indexer/src/config.rs`.

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
