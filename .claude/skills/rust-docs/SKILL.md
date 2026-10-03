---
name: rust-docs
description: Documentation standard for op-p2p-indexer. Load when adding or changing public items, modules, or crate roots, when writing doc comments or doctests, or when documenting protocol behavior (OP Stack p2p, gossip, payloads) and its spec references.
---

# Rust docs

Lints already require docs on public items plus `# Errors` and `# Panics` sections, and they
reject broken intra-doc links. This skill sets the quality bar on top of that.

## Beyond what the lints require

- **Crate root** (`lib.rs` / `main.rs`): `//!` explaining what the crate does, its main components,
  and how data flows between them (for example peers → gossip validation → indexer → store).
- **Every module**: a `//!` paragraph on its responsibility and what it deliberately does *not* do.
- **Private items**: only when the reason or invariant isn't obvious from the code.

## Shape of a doc comment

```rust
/// Validates an unsafe-block gossip message and decodes its payload.
///
/// Checks run cheapest first: size limit, envelope decoding, timestamp window,
/// then the sequencer signature over the payload hash.
///
/// See <https://specs.optimism.io/protocol/rollup-node-p2p.html#block-validation>.
///
/// # Errors
///
/// Returns [`GossipError::Decode`] if the envelope is malformed,
/// [`GossipError::Stale`] if the timestamp is outside the accepted window, and
/// [`GossipError::InvalidSignature`] if it was not signed by the configured sequencer.
pub fn validate(&self, msg: &[u8]) -> Result<UnsafeBlock, GossipError> { /* ... */ }
```

- **Summary line**: one sentence in third person ("Validates…", "Returns…"), ending with a period.
  It appears in module listings, so it must stand alone.
- **Body**: behavior, invariants, units (ms vs s, wei), ordering, concurrency or cancellation
  guarantees, and performance traits when relevant.
- **Sections**, in this order:
  - `# Errors`, listing each condition.
  - `# Panics`.
  - `# Cancel safety` for async functions used in `select!`.
  - `# Examples` for the main public entry points.
- **Links**: use intra-doc links (`` [`Type`] ``), never plain backticked names for items we own.
- **Spec references**: when behavior follows the OP Stack, reth, or libp2p specs, link the exact section.

## Doctests

- Examples must compile. Use `no_run` for anything that touches the network or disk.
  Don't use `ignore`: it hides rot.
- Use `?` in examples with a hidden `# fn main() -> eyre::Result<()> {` wrapper.
- Keep examples minimal: show the call pattern, not a full program.

## Comments vs docs

- `///` and `//!` describe the contract for callers.
- `//` explains *why* the implementation is written that way (protocol quirks, workarounds with issue links).
- Don't narrate code (`// increment counter`).
- `TODO` must reference an issue or say what's blocking: `// TODO(#12): switch to SSZ once ...`.

## Checking

Rustdoc lints only run under `cargo doc`, not under clippy, so run it after changing docs:

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items
```
