---
name: rust-async
description: Async and concurrency conventions for op-p2p-indexer (tokio, libp2p). Load when writing or reviewing async code, spawning tasks, using channels or select!, building the libp2p swarm event loop, handling gossipsub validation, adding timeouts or retries, or wiring shutdown and cancellation.
---

# Rust async

Runtime: **tokio** (multi-threaded). `#[tokio::main]` appears only in `main.rs`.

## Components and the edge

The indexer is a standalone binary (see `docs/roadmap.md`; it does not embed a node). Components
still do not own their own top-level task, so the binary decides how they are spawned,
supervised and shut down:

- A component is a struct built with its dependencies and run by a single method:
  ```rust
  impl Indexer {
      /// Runs until `cancel` fires or a fatal error occurs.
      pub async fn run(self, cancel: CancellationToken) -> Result<(), IndexerError> { /* ... */ }
  }
  ```
- Components never call `tokio::spawn` for their *own* top-level task. The edge (`main.rs`)
  decides how to spawn and supervise them.
- Inside a component, spawn children into a `JoinSet` that the component owns and drains on shutdown.

## Never block the runtime

- No blocking file or DB I/O, or heavy CPU work, in async code. Use `tokio::fs` or
  `tokio::task::spawn_blocking`. (`std::thread::sleep` is banned in `clippy.toml`.)
- Batch CPU-heavy work (signature recovery over many messages, hashing large payloads) into
  `spawn_blocking` or a rayon pool.
- Prefer message passing over locks. Where a lock is needed, keep the critical section short and synchronous.

## Channels

- Channels are bounded (`unbounded_channel` is banned in `clippy.toml`). Every capacity is a named
  constant with a comment explaining the size.
- Pick the type by intent:
  - `mpsc`: work queues and actor commands.
  - `oneshot`: request/response replies.
  - `broadcast`: fan-out events (handle `RecvError::Lagged`).
  - `watch`: latest-value state such as head block or sync status.
- Make backpressure explicit. For gossip, prefer `try_send` and drop or count on `Full` over
  `send().await`, so a slow consumer can't stall the network loop. Expose a metric for drops.
- Treat a closed channel as shutdown, not as an error to retry.

## Cancellation and shutdown

- One root `tokio_util::sync::CancellationToken`. Children get `child_token()`.
- Every long-running loop selects on cancellation, listed first under `biased;`:
  ```rust
  loop {
      tokio::select! {
          biased;
          () = cancel.cancelled() => break,
          Some(cmd) = commands.recv() => self.handle(cmd).await?,
      }
  }
  ```
- Only use cancel-safe futures as `select!` branches inside loops. `mpsc::Receiver::recv`,
  `cancelled()`, and `sleep` are cancel-safe; `read_exact` and multi-step async fns usually aren't.
  Pin non-cancel-safe work outside the loop, or move it into its own task.
- No detached `tokio::spawn` with an ignored handle. Await every `JoinHandle` or `JoinSet`, and
  propagate panics and errors.
- Shutdown order: stop accepting input (network), drain in-flight work, flush the store, then exit.

## libp2p

- **One task owns the `Swarm`** (actor pattern). Nothing else touches it.
  - The rest of the system sends `enum NetworkCommand { Publish { .. }, DialPeer { .. }, ... }`
    over `mpsc`, with `oneshot` replies where needed.
  - The swarm task emits domain events (`NetworkEvent::UnsafeBlock { .. }`) over a bounded channel.
  - The loop is `select!` over `cancel`, `commands.recv()`, and `swarm.select_next_some()`.
- Keep the swarm loop fast. Decode and validate in worker tasks, never inline on the swarm task.
- Use gossipsub with **manual validation** (`validate_messages()`): workers validate, then the
  swarm task calls `report_message_validation_result` with `Accept`, `Reject` (peer fault), or `Ignore`.
  This follows the error classification in `rust-errors`.
- Set limits explicitly: max message size, connection limits, peer scoring params, and topic names
  (`/optimism/{chain_id}/{version}/blocks`). Make them configurable with spec-default values.

## Time

- Wrap every network or remote call in `tokio::time::timeout`. No unbounded waits on peers.
- `tokio::time::interval` with `MissedTickBehavior::Delay` (or `Skip`); never the default `Burst` for periodic work.
- Use `Instant` for durations and `SystemTime` only for protocol timestamps.

## Observability

- Instrument tasks with `#[tracing::instrument(skip_all, fields(...))]`, or attach spans with
  `.instrument(span)` when spawning. Spans don't cross `spawn` automatically.
- Count channel drops, validation outcomes, peer counts, and task restarts; don't only log them.
