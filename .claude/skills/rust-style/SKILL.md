---
name: rust-style
description: House Rust style for op-p2p-indexer. Load before writing, editing, or reviewing any Rust code in this repo. Covers module layout, visibility, naming, ownership, numeric conversions on chain data, alloy primitive types, logging with tracing, dependency hygiene, and how to suppress lints.
---

# Rust style

Mechanical rules (panics, indexing, casts, prints, `mod.rs`, wildcard matches, `#[allow]`) are
enforced by `[lints]` in `Cargo.toml`, `clippy.toml`, and `deny.toml`. This skill covers the judgment calls.

## Layout

- Keep the binary thin: parse config, set up `tracing`, build components, run, map the exit code.
  Logic lives in the `crates/` libraries, so it can be tested and reused if we move to a reth ExEx.
- One concept per module. Split files that grow past roughly 500 lines, or that hold more than one main type.
- Order inside a file: imports, constants, public types, impls, private helpers, then `#[cfg(test)] mod tests`.

## Visibility

- Private by default. Use `pub(crate)` for crate internals and plain `pub` only for the intended API.
- Types with invariants have private fields and a validating constructor (`new` / `try_new`).
  Plain data carriers (configs, DTOs) may have `pub` fields.

## Naming

- Follow the Rust API Guidelines: `as_` (cheap borrow), `to_` (expensive), `into_` (consuming).
- No `get_` prefix on getters. Booleans read as `is_*` / `has_*`.
- Constructors are `new`, `with_*`, `from_*`, or `try_from_*`. Prefer implementing `From` / `TryFrom` over ad-hoc functions.
- Name things after the protocol concept (`UnsafeBlock`, `PayloadEnvelope`, `GossipTopic`), not after the data structure.

## Ethereum and OP types (alloy)

- Use `alloy_primitives` types only: `B256`, `Address`, `U256`, `Bytes`, `BlockNumber`,
  `ChainId`, `Signature`. Never use `String` / `Vec<u8>` / `[u8; 32]` for hashes or addresses in APIs.
- Use `alloy-rlp` for RLP and the `alloy` / `op-alloy` consensus and RPC types for blocks, headers,
  and payloads. Don't redefine types alloy already ships.
- Use `bytes::Bytes` (or alloy `Bytes`) for payloads passed between tasks, to avoid copies.
- Wrap domain identifiers that alloy lacks in newtypes (`struct PeerScore(i64)`), not bare primitives.

## Numbers from the network are untrusted

- When a pedantic cast lint fires, fix it with `TryFrom` (`u64::try_from(x)`, `U256::try_into`).
  Don't suppress it.
- Use `checked_*` / `saturating_*` arithmetic on peer-supplied values (block numbers, lengths, timestamps).
- Note units in names or types: `timeout_ms`, `Duration`, or a wei amount in `U256`. Never an ambiguous `u64`.

## Ownership

- Borrow in parameters (`&str`, `&[T]`, `impl AsRef<..>`). Take ownership only when the value is stored.
- Don't `.clone()` to get past the borrow checker without first reconsidering the data flow.
- Use `Arc<T>` for shared immutable state and message passing (see `rust-async`) for shared mutable
  state. Avoid `Arc<Mutex<_>>` webs.

## Control flow

- Prefer early returns, `let ... else`, and `?` over nested `if let` / `match`.
- Prefer iterators and combinators to index loops when it reads better. Don't chain more than about 4 adapters without naming intermediate steps.

## Logging (`tracing`)

- Use structured fields, not formatted strings: `info!(%block_hash, number, peer = %peer_id, "imported unsafe block")`.
- Levels: `error` for operator action needed; `warn` for degraded or recovered; `info` for lifecycle
  and milestones; `debug` for per-message detail; `trace` for wire-level detail.
- Never log full payloads at `info` or above.

## Derives and attributes

- Add `Clone`, `Copy`, `PartialEq`, `Eq`, and `Hash` when meaningful (`Debug` is required by a lint).
- Add `#[non_exhaustive]` on public enums and structs expected to grow.

## Suppressing lints

Use `#[expect(lint, reason = "...")]` on the narrowest item. `#[allow]` is linted against.
For an invariant that truly cannot fail:

```rust
#[expect(clippy::expect_used, reason = "regex literal is validated by tests")]
fn topic_regex() -> Regex { Regex::new(TOPIC_PATTERN).expect("valid regex") }
```

## Dependencies

- Declare versions once in the root `[workspace.dependencies]`; crates use `dep.workspace = true`.
- Use `default-features = false` and enable only the features you need, especially for reth, alloy, and libp2p.
- Every new crate needs a one-line justification in the commit.
