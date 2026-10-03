---
name: rust-errors
description: Error-handling conventions for op-p2p-indexer. Load when defining error types, writing fallible functions, decoding untrusted network data (gossip, RLP, SSZ, payloads), adding retries, or deciding whether an error should penalize a peer, retry, or shut down.
---

# Rust errors

## Library code: `thiserror`

- Each module boundary owns one error enum, defined with `thiserror`. Name it `<Module>Error`
  (`GossipError`, `DecodeError`, `StoreError`).
- Variants carry the context needed to debug without logs: which peer, block hash or number, topic, or expected vs actual length.
- Messages are lowercase with no trailing punctuation, and they describe what failed, not the cause chain:
  ```rust
  #[derive(Debug, thiserror::Error)]
  #[non_exhaustive]
  pub enum GossipError {
      #[error("invalid payload signature from {peer}")]
      InvalidSignature { peer: PeerId },
      #[error("payload for block {number} is {age_secs}s old")]
      Stale { number: u64, age_secs: u64 },
      #[error("failed to decode payload envelope")]
      Decode(#[source] DecodeError),
  }
  ```
- Use `#[from]` only when the conversion is unambiguous (exactly one sensible variant for that
  source type). Otherwise use `#[source]` plus an explicit `map_err` that adds context.
- Never put `Box<dyn Error>`, `eyre::Report`, or `String` errors in a library API.

## Binary edge: `eyre`

`main.rs` and top-level wiring return `eyre::Result<()>` and add context with `.wrap_err(...)`,
matching reth. `anyhow` is banned in `deny.toml`.

## Untrusted input must never panic

Anything from a peer (gossip messages, req/resp, ENRs, RLP/SSZ bytes) is hostile.

- Decoders never allocate based on an unchecked length prefix.
- Validate in order: size limits, decoding, structure, then signature. Reject cheaply before doing expensive work.
- Classify every failure, because the classification drives the response:

  | Class | Example | Response |
  |---|---|---|
  | Peer fault | bad signature, malformed payload, wrong chain id | reject the message, penalize or score the peer |
  | Ignorable | duplicate, too old, too far in the future | ignore without penalty |
  | Transient local | DB busy, RPC timeout | retry with backoff |
  | Fatal local | corrupted store, bad config | cancel the root token, shut down cleanly |

  Expose this on the error type (`fn is_peer_fault(&self) -> bool`, or a `Severity` enum) instead
  of string-matching at the call site. For gossipsub it maps directly to
  `MessageAcceptance::{Reject, Ignore, Accept}`.

## Propagation rules

- Use `?` freely, but add context at every boundary where the callee's error alone is ambiguous.
- Log **or** return, never both. The site that *handles* the error logs it once, with structured fields.
- If ignoring an error is correct, say why: `if let Err(err) = fallible() { debug!(%err, "...") }`.

## Retries

- Retry only transient errors, with bounded exponential backoff and jitter. Retry loops must observe
  the cancellation token (see `rust-async`).
- After the retry budget runs out, escalate to the caller as a typed error. Don't loop forever.
