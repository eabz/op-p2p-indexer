# Native Flight benchmark

`bench` ships with the other Linux binaries. Build locally with `cargo build --release -p bench`.
It requires no Python or PyArrow. The `bin/bench` CLI uses `crates/bench`, a standalone Flight
consumer with no storage, node or ingestion dependency. `scripts/bench.py` remains available
for cross-client comparisons.

Set `KEY` in the environment (or `.env` / `--env-file`) to a key accepted by the balancer and
servers. The key is not a command-line argument and is excluded from reports and debug output.

```bash
./target/release/bench \
  --balancer grpc://146.190.220.254:50060 \
  --table blocks --from 40000000 --to 45000000 \
  --concurrency 24 --per-server 8 --compression zstd \
  --repeat 5 --json blocks-results.json
```

`--concurrency` replaces Python's processes × threads. Native tasks share one per-server
admission pool across the run and reuse each server's HTTP/2 connection. Arrow decoding runs
on blocking workers; network runtime threads stay available. Only one batch per stream is
consumed at a time, without collecting the requested range in memory. Whole suites run
sequentially, so concurrency limits also bound the heavy workload.

## Heavy requests

`--heavy` reads **blocks, transactions, receipts and logs**, sequentially over the same
explicit inclusive range. It raises the default whole-RPC deadline from 120 to 600 seconds
and the per-job budget from 300 to 1,800 seconds. It keeps the defaults of 24 concurrent jobs
and eight reads per server. Both deadlines remain explicitly overridable. `--repeat` repeats
the complete four-table workload, with a new plan for every table and repetition.

Start with a representative range containing large transactions and logs:

```bash
./target/release/bench \
  --balancer grpc://146.190.220.254:50060 \
  --heavy --from 48000000 --to 48100000 \
  --concurrency 24 --per-server 8 --compression zstd \
  --repeat 3 --json heavy-results.json
```

Then widen the range to increase total work without raising server admission:

```bash
./target/release/bench \
  --balancer grpc://146.190.220.254:50060 \
  --heavy --from 40000000 --to 45000000 \
  --concurrency 24 --per-server 8 --compression zstd \
  --json heavy-large-results.json
```

A large logical request is still split into the balancer's per-chunk tickets; heavy mode does
not bypass the server's maximum ticket range. `--table transactions` or `--table logs` allows
isolated heavy table measurements; supply longer deadlines explicitly for those single-table
runs. `--heavy` and `--table` are mutually exclusive. The default received message limit is
64 MiB before IPC decompression, configurable with `--max-message-bytes`; it is not a bound
on total output or decompressed memory. Endpoints are plaintext `grpc://`, `grpc+tcp://` or
`http://`, matching the current fleet.

## Checks and metrics

- Before reading, tickets must cover exactly the requested table, finality and range, with
  no overlaps or gaps, and every ticket must name a server. A clipped range is an error.
- Every batch must match the table schema. Block numbers must be within the ticket and
  nondecreasing. `blocks` additionally requires every requested block exactly once. Other
  tables can legitimately be empty; their row completeness is not independently proven.
- Resource exhaustion, unavailability and deadline failures retry fallback locations with
  bounded, jittered backoff. Nonretryable status, schema and data errors fail the job.
- `--rpc-timeout` bounds a complete attempt, including connection and decode; `--retry-for`
  bounds an admitted job including slot waits and retries. Work-queue time precedes that
  budget. Cancellation drains active readers and preserves partial read reports.
- Useful rows/bytes come only from completed jobs. Received bytes include failed attempts;
  those are also listed as failed/incomplete bytes. MB is decimal decoded Arrow data, not
  compressed traffic, NIC bytes or R2 bytes. Logical-byte accounting matches PyArrow's
  `RecordBatch.nbytes` for the project's flat schemas, excluding allocator padding and the
  final variable-width sentinel offset.
- Queue time, first-batch time and completion latency are measured from enqueueing. First
  batch and completion include queueing, slot waits and retries. Per-stream rates describe
  successful attempts only. Planning appears separately and in end-to-end throughput.
- Reports include per-job failure categories and per-server rates. JSON is rewritten after
  each completed run and retains all preceding runs. Any incomplete run exits nonzero and
  stops the remaining suite; its successful-subset throughput is labeled explicitly.

Compare release builds on the same client and range. Keep Python and Rust series distinct:
runtime startup, connection reuse and decoding differ even when exact rows and bytes match.
Do not infer available server capacity from `--per-server`; observe server utilization and
competing traffic before increasing it. Repeated reads do not establish a controlled cold
cache measurement, and the benchmark never restarts servers.

## Live validation, 2026-10-05

A macOS release build completed `--heavy --from 48000000 --to 48100000` with Zstd,
24 concurrent jobs and cap 8. All 12 jobs across four tables completed, with no failures or
retries and 1,289,622,785 decoded bytes in total. [Full report](benchmarks/2026-10-05-native-heavy.json):

| Table | Rows | Decoded bytes | End-to-end seconds |
|---|---:|---:|---:|
| blocks | 100,001 | 65,813,160 | 4.255 |
| transactions | 903,658 | 800,691,622 | 116.874 |
| receipts | 903,658 | 108,779,883 | 20.357 |
| logs | 968,312 | 314,338,120 | 15.193 |

Over blocks 48,000,000–48,000,050, both native repetitions returned exactly the same rows
and decoded bytes as PyArrow for **all four tables under none, LZ4 and Zstd**. A deliberately
short read deadline discarded 5,031,835 decoded bytes, reported zero useful bytes and exited
nonzero. A planning deadline exited before reading. SIGINT during a 130-job run drained
readers, recorded every planned job, balanced useful/discarded byte accounting and exited
nonzero. [Verification summary](benchmarks/2026-10-05-native-checks.json).

These are client correctness checks on an uncontrolled live fleet, not evidence of a server
speedup. Server revision, resource use and cache state were not verified.
