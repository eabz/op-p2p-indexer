# op-p2p-indexer

A Rust indexer for the OP Stack peer-to-peer network. It joins the libp2p gossip network,
validates and decodes payloads with alloy / op-alloy types, and indexes them. It integrates with
reth / op-reth; whether it runs as a **standalone binary or a reth ExEx is not decided yet**, so
keep core logic neutral to that choice (see `rust-async`).

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
