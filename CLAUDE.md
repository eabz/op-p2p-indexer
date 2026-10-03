# op-p2p-indexer

A Rust indexer for the OP Stack peer-to-peer network. It joins the libp2p gossip network,
validates and decodes payloads with alloy / op-alloy types, and indexes them. It integrates with
reth / op-reth; whether it runs as a **standalone binary or a reth ExEx is not decided yet**, so
keep core logic neutral to that choice (see `rust-async`).

## Layout (Cargo workspace)

| Path | Package | Role | Internal deps |
|---|---|---|---|
| `bin/op-p2p-indexer` | `op-p2p-indexer` | Thin binary: config, tracing, wiring, shutdown | all |
| `crates/primitives` | `op-indexer-primitives` | Shared domain types (alloy only) | none |
| `crates/p2p` | `op-indexer-p2p` | OP gossip: libp2p swarm, decoding, validation | primitives |
| `crates/storage` | `op-indexer-storage` | Hot (Redis) / cold (ClickHouse) stores | primitives |
| `crates/pipeline` | `op-indexer-pipeline` | Unsafe → hot store; promote safe/finalized → cold | primitives, storage |

- Keep these edges: `p2p` and `storage` never depend on each other, and `pipeline` doesn't depend
  on `p2p`. The binary wires them together with channels.
- Safe/finalized status will come from L1 via reth (likely an ExEx); that integration gets its own crate when it starts.
- New crates go in `crates/<name>` as package `op-indexer-<name>`, inherit `[workspace.package]`,
  and set `lints.workspace = true`. All dependency versions live in root `[workspace.dependencies]`.

## Storage

- **Redis**: hot data. Unsafe blocks received over gossip and not yet derived from L1.
- **ClickHouse**: cold data. Safe/finalized blocks, whose batches are on L1.

`docker compose up -d redis clickhouse` starts both locally (see `docker-compose.yml` for the
`OP_INDEXER_*` connection variables); `docker compose up --build` also runs the indexer image.

## Project skills

Before writing, editing, or reviewing Rust, load **`rust-style`** with the Skill tool. Also load
each of `rust-errors`, `rust-docs`, and `rust-async` whose description matches the work. If a skill's
convention conflicts with what the code needs, raise it with the user instead of silently deviating.

## Tooling (enforced, don't work around it)

- `[lints]` in `Cargo.toml`, `clippy.toml`, `rustfmt.toml`, and `deny.toml` hold the mechanical rules.
- Hooks (`.claude/settings.json`, `.claude/hooks/`):
  - **After each edit**: `rustfmt` runs on the `.rs` file that was written.
  - **When finishing a turn**: if Rust sources changed, `cargo fmt` and
    `cargo clippy --all-targets -D warnings` run, plus `cargo deny check` when manifests changed.
    Finishing is blocked until they pass.
  - Don't run clippy manually just to check; the hook does it.
- Not covered by hooks, so run them yourself when relevant: `cargo test`, and `cargo doc` (see `rust-docs`).

## Finishing a task

When a task that changed code is complete:

1. Run `cargo test` if the change touched tested code.
2. **Run `/simplify`** (the `simplify` skill) on the changes and apply its fixes.
3. Summarize what changed for the user.

Skip step 2 only for tasks that changed no code (questions, planning, docs-only edits).
