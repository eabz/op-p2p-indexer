# op-p2p-indexer

A Rust indexer for the OP Stack peer-to-peer network. It joins the libp2p gossip network,
validates and decodes payloads with alloy / op-alloy types, and indexes them. It integrates with
reth / op-reth; whether it runs as a **standalone binary or a reth ExEx is not decided yet**, so
keep core logic neutral to that choice (see `rust-async`).

## Layout (Cargo workspace)

| Path | Package | Role | Internal deps |
|---|---|---|---|
| `bin/op-indexer` | `op-indexer` | Thin binary: config, tracing, wiring, shutdown | chainspec, p2p (storage and pipeline once they exist) |
| `crates/primitives` | `op-indexer-primitives` | Shared domain types (alloy and `bytes` only) | none |
| `crates/chainspec` | `op-indexer-chainspec` | Static chain parameters (chain id, sequencer signer, bootnodes) | none |
| `crates/p2p` | `op-indexer-p2p` | discv5 discovery, gossipsub block gossip (scoring, connection limits), unsafe-block validation, redb node state | primitives, chainspec |
| `crates/storage` | `op-indexer-storage` | Hot (Redis) / cold (ClickHouse) stores | primitives |
| `crates/pipeline` | `op-indexer-pipeline` | Unsafe → hot store; promote safe/finalized → cold | primitives, storage |

- Keep these edges: `p2p` and `storage` never depend on each other, and `pipeline` doesn't depend
  on `p2p`. `p2p` depends on `chainspec` (it is chain-specific); the binary parses overrides (e.g.
  bootnodes) at the edge. The binary wires them together with channels.
- Safe/finalized status will come from L1 via reth (likely an ExEx); that integration gets its own crate when it starts.
- New crates go in `crates/<name>` as package `op-indexer-<name>`, inherit `[workspace.package]`,
  and set `lints.workspace = true`. All dependency versions live in root `[workspace.dependencies]`.

## Storage

- **Redis**: hot data. Unsafe blocks received over gossip and not yet derived from L1.
- **ClickHouse**: cold data. Safe/finalized blocks, whose batches are on L1.

`docker compose up -d redis clickhouse` starts both locally; `docker compose up --build` also runs
the indexer image. The storage crate is still a stub, so the binary does not read the Redis and
ClickHouse `OP_INDEXER_*` variables in `docker-compose.yml` yet; the ones it reads are documented
on `Config::from_env` in `bin/op-indexer/src/config.rs`.

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
