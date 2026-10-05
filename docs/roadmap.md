# Roadmap

What is left, in order. Status 2026-10-05: every program is built and has run live.
What runs today is in [architecture.md](architecture.md), and how it got here is in
[decisions.md](decisions.md).

## Run live (2026-10-05)

- **Unichain from R2**: three servers and a balancer serve the sealed history (1,694 chunks)
  from one bucket ([serving.md](serving.md)).
- **Range sync**: execution peers fill gaps, including a deep hole below the tip
  ([el.md](el.md)).
- **L1 commitment, end to end**: the beacon light client bootstrapped from a checkpoint,
  dispute games were matched to the node's blocks, and the safe and finalized heads rose
  ([l1.md](l1.md)).
- **Flight from a non-local client** (v0.1.8): blocks 129.6 MB/s on 48 streams, transactions
  406 MB/s, logs with lz4 329 MB/s ([serving §7](serving.md#7-the-bench-3-to-4-small-droplets-one-r2-bucket)).
- **Base import**: the download is done. HyperSync answered the same query completely only 4
  times in 5, so incomplete ranges are being fetched again (`download --refetch-incomplete`,
  [import §3.1](import.md#31-download)).
- **Exporter**: running on the Unichain fleet; not yet seen sealing a chunk.

## Installation and TOML-only configuration (release pending)

- Breaking change: runtime environment overrides and dotenv loading are removed; operational
  CLI flags override TOML, then defaults. `--env-file` is rejected. Explicit
  `--migrate-env INPUT --config OUTPUT` converts old files without loading them at runtime.
- Implemented: shared TOML configuration, legacy-file conversion, per-chain and per-role
  state paths, interactive and unattended setup, collision-aware port assignment, and named
  systemd services with chain start/stop targets.
- Implemented: checksum-verified installation and atomic binary replacement from published
  releases; updates preserve configuration and state, and restart only selected services.
- Validated locally: all four binaries, configuration precedence and migration, installer
  preservation/collision checks, and scoped service lifecycle generation. Rust checks pass.
- Release gate: Ubuntu CI (including generated systemd units) must pass before merging and publishing. The one-liner requires a published
  release containing the new setup helper and TOML-capable binaries.
- Pending operational validation: install on a fresh Ubuntu fleet host, migrate its existing
  environment configuration, and confirm graceful restart and boot startup with real state.

## What's left, in order

| # | Work | Done when |
|---|---|---|
| 1 | Finish Base's import: the refetch, then `verify` against the anchor ([import.md](import.md)) | Every block is complete, the chunks are uploaded and listed in the manifest; then a Base node runs |
| 2 | Confirm the exporter seals ([serving §4](serving.md#4-sealing-new-chunks-the-exporter)) | A newly finalized chunk is sealed, appended to the manifest and served, with no block missing or doubled across the tail/sealed boundary |
| 3 | Reconcile the unsafe chain when a new safe head contradicts it ([storage §3.2](storage.md#32-fork-choice-and-reorgs)) | `set_l1_heads` removes the contradicted entries, moves the head to the safe block and emits a reorg |
| 4 | Harden dispute-game selection and retention ([l1 §5](l1.md#5-not-built)) | The portal's respected game type is checked; a matched claim survives unrelated games until it finalizes |
| 5 | Failover | A server killed during Flight and subscription reads: clients resume with no gap, receipts and reorgs intact |
| 6 | Bootstrap the `indexer`'s archive from chunks ([serving §5.6](serving.md#56-the-indexers-history-from-chunks-design-not-built)) | A verified local anchor, then a resumable download of the history below it |
| 7 | Sustained peer serving | Measured over days: access, throughput, honest advertised coverage, no starvation of ingestion |

## Decisions pending measurement

- **Native benchmark client:** `bin/bench` and `crates/bench` add a release binary independent
  of Python, with bounded concurrency, per-job JSON reports, repeated runs and a `--heavy`
  four-table workload. Retain the Python client for row/byte parity checks; measure native
  throughput separately before attributing any difference to servers ([bench.md](bench.md)).
- **Serving throughput after v0.1.8:** the CPU/read-path changes and machine-sized limits are
  implemented. The v0.1.9 benchmark adds shared per-server admission, read deadlines, exact
  plan coverage and incremental decoded-byte accounting. Local Flight checks validate the
  benchmark. Ten repeated macOS reads of blocks 40M–45M completed without failures or
  retries: cap 8 reached a median 44.0 useful decoded MB/s versus cap 4's 27.8 MB/s.
  User-reported importer-hosted samples reached 139.0 and 112.0 MB/s respectively;
  repeat those with server telemetry to establish the fleet's sustainable rate. Larger batches,
  work-weighted jobs and R2 tuning remain proposals until profiling supports them
  ([serving §7](serving.md#7-the-bench-3-to-4-small-droplets-one-r2-bucket)).
- **Separate history servers from ingestion?** Only if the duplicated networking costs more
  than it gives; servers stay full nodes until then.
- **Keep the custom balancer?** Only while per-chunk parallel Flight jobs beat ordinary load
  balancing.

## Out of scope

Balances, `eth_call`, traces, execution and batch derivation. Optional API work (TLS inside
the binaries, log filters, more Flight services) is in [stream §7](stream.md#7-not-built).
