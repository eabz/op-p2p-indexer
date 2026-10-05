# Configuration

Each chain uses one private `config.toml`, shared by its roles. The installer writes
`~/.op-indexer/<chain>/config.toml`, where the chain is `op`, `unichain` or `base`.
Use `--chain unichain` to select that file or `--config PATH` to select another.
A normal run requires a configuration file; `--help` and `--version` do not.
Without explicit selection, discovery checks `./config.toml`, then the chain home file.
Relative paths resolve beside the file that defines them. Role state defaults to separate
`data/indexer`, `data/server` and `data/importer` directories.

**Breaking change, pending release:** runtime configuration comes from TOML only.
Operational CLI flags override child-file settings, then inherited settings, then built-in defaults. Process environment
settings and automatic `.env` loading no longer configure the programs. `--env-file` is
rejected. The legacy format is accepted only by the explicit migration command below.

```toml
chain = "unichain"
log_filter = "info"

[indexer]
profile = "live"

[indexer.stream]
listen_addr = "127.0.0.1:50051"
api_keys = ["replace-with-client-key"]

[bench]
balancer_url = "http://balancer.internal:50060"
api_key = "replace-with-client-key"
```

Keep credentials in a mode `0600` file. Unknown keys and incorrect types fail validation;
errors do not echo secret values. Each binary uses its own role section and shared `[r2]`
settings. Generated services contain the configuration path rather than credentials.

```bash
server --config "$HOME/indexer/unichain/config.toml" --check-config
import --config "$HOME/indexer/unichain/config.toml" run --help
```

`--check-config` validates without opening a database or contacting peers or R2. Import
subcommands also check their operation-specific requirements when invoked.

## Reuse settings across roles and chains

A chain file can inherit a shared file with `extends = "../config.toml"`. This produces a
simple layout: `~/.op-indexer/config.toml` holds reusable credentials and tuning, while
`~/.op-indexer/unichain/config.toml`, `~/.op-indexer/op/config.toml` and `~/.op-indexer/base/config.toml`
hold each chain's identity, configured roles, paths and ports. The shared file may omit
`chain`; the final merged chain configuration must supply it.

Shared file, `~/.op-indexer/config.toml`:

```toml
log_filter = "info"

[node]
profile = "live"

[node.stream]
api_keys = ["replace-with-client-key"]
max_subscriptions = 64

[r2]
account_id = "replace-me"
access_key_id = "replace-me"
secret_access_key = "replace-me"
```

Chain file, `~/.op-indexer/unichain/config.toml`:

```toml
extends = "../config.toml"
chain = "unichain"

[r2]
bucket = "unichain-snapshot"

[indexer]
data_dir = "data/indexer"

[indexer.stream]
listen_addr = "127.0.0.1:50151"

[server]
data_dir = "data/server"

[server.stream]
listen_addr = "127.0.0.1:50051"
```

Tables merge recursively; a child's scalar or array replaces the parent's whole value.
The `extends` path is relative to the referring file. Every path setting remains relative
to its own source file, even after inheritance. Cycles and chains exceeding eight files
are rejected. Keep every file containing credentials private (`0600`).

`[node]` supplies common capabilities and limits to explicitly configured `[indexer]` and
`[server]` roles. Their own values override `[node]`, including inherited role values.
Use `[node.stream]`, `[node.p2p]`, `[node.el]` and `[node.l1]` for shared nested settings.
State paths, log paths, listen addresses and advertised addresses are not allowed in
`[node]`: keep these per-instance values in each role to avoid shared databases and port
collisions. Adding `[node]` alone does not register another role.

## Migrate an existing deployment

Stop the old instance before changing its service registration. From its original working
directory, convert its environment file using the binary that owns the state:

```bash
mkdir -p "$HOME/indexer/unichain"
server --migrate-env /path/to/old/.env --config "$HOME/indexer/unichain/config.toml"
server --config "$HOME/indexer/unichain/config.toml" --check-config
```

Conversion preserves the source, writes a new mode `0600` file and refuses to overwrite an
existing config. Legacy relative paths become absolute so they still identify the same
state. Duplicate assignments retain the first value, matching the old loader. Unknown or
other-role settings are refused rather than silently dropped; split a shared legacy input
by role before migration. Review the output before restarting. Migration does not move data,
and two processes must never open the same database.

## Role setup and defaults

- `indexer`: no credentials required for gossip. `profile = "live"` adds execution receipts.
- `server`: shared R2 credentials (or `chunks_dir`), a balancer URL and registration key.
  Set `export = true` on only one server per deployment to seal finalized chunks.
- `balancer`: accepted server registration keys, client API keys and shared R2 settings.
- `importer`: source token and destination R2 credentials; operational range and concurrency
  flags are described by `import run --help`.
- `bench`: a balancer URL and client API key; workload flags are described by `bench --help`.

The `archive` and `fleet` profiles enable L1 tracking and range sync; L1 requires a trusted
checkpoint. L1 implies range sync, and range sync implies execution networking. Explicit
contradictory switches fail startup. With no profile, these optional capabilities are off.

Default stream binding is `127.0.0.1:50051`; balancer binding is `0.0.0.0:50060`.
The installer chooses distinct ports for every saved role and chain, including stopped
instances, and checks occupied TCP/UDP sockets. A server's advertised address must use its
selected stream port. Without an explicit address, a concrete listen IP is used; wildcard
binding requires discovery of a public IP through execution networking.

Unsafe memory, Flight build/stream limits and server read budgets are sized from available
cores and memory. The node logs the chosen limits. Their formulas live in
`crates/node/src/sizing.rs`; tune them only after measuring the limiting resource.

## Accepted TOML fields

The tables below follow `crates/runtime/src/config/schema.rs`. `chain` is a top-level chain
name or quoted ID. `extends` is an optional parent-file path. `[node]` accepts the shared
subset of indexer/server fields described above. Strings are quoted, booleans are `true`/`false`, integers are nonnegative, and
lists are arrays of strings. Path strings resolve beside the configuration file.
See [`config.toml.example`](../config.toml.example) for an editable example.

### bench

| Field | Type |
|---|---|
| `bench.api_key` | string |
| `bench.balancer_url` | string |
| `bench.table` | string |
| `bench.heavy` | boolean |
| `bench.from` | integer |
| `bench.to` | integer |
| `bench.cap` | string |
| `bench.concurrency` | integer |
| `bench.per_server` | integer |
| `bench.compression` | string |
| `bench.plan_timeout` | integer |
| `bench.rpc_timeout` | integer |
| `bench.retry_for` | integer |
| `bench.progress` | integer |
| `bench.repeat` | integer |
| `bench.max_message_bytes` | integer |
| `bench.json` | path |

### bench

| Field | Type |
|---|---|
| `bench.api_key` | string |
| `bench.balancer_url` | string |
| `bench.table` | string |
| `bench.heavy` | boolean |
| `bench.from` | integer |
| `bench.to` | integer |
| `bench.cap` | string |
| `bench.concurrency` | integer |
| `bench.per_server` | integer |
| `bench.compression` | string |
| `bench.plan_timeout` | integer |
| `bench.rpc_timeout` | integer |
| `bench.retry_for` | integer |
| `bench.progress` | integer |
| `bench.repeat` | integer |
| `bench.max_message_bytes` | integer |
| `bench.json` | path |

### r2

| Field | Type |
|---|---|
| `r2.public_url` | string |
| `r2.account_id` | string |
| `r2.access_key_id` | string |
| `r2.secret_access_key` | string |
| `r2.bucket` | string |
| `r2.prefix` | string |
| `r2.endpoint` | string |
| `r2.presign_access_key_id` | string |
| `r2.presign_secret_access_key` | string |

### General

| Field | Type |
|---|---|
| `log_filter` | string |

### indexer

| Field | Type |
|---|---|
| `indexer.data_dir` | path |
| `indexer.profile` | string |
| `indexer.unsafe_max_bytes` | integer |
| `indexer.log_file` | path |
| `indexer.stream.listen_addr` | string |
| `indexer.stream.api_keys` | string array |
| `indexer.stream.max_subscriptions` | integer |
| `indexer.stream.max_flights` | integer |
| `indexer.stream.max_builds` | integer |
| `indexer.stream.flight_queue_ms` | integer |
| `indexer.p2p.listen_addr` | string |
| `indexer.p2p.advertised_addr` | string |
| `indexer.p2p.bootnodes` | string array |
| `indexer.p2p.max_peers` | integer |
| `indexer.el.enabled` | boolean |
| `indexer.el.sync` | boolean |
| `indexer.el.listen_addr` | string |
| `indexer.el.advertised_addr` | string |
| `indexer.el.bootnodes` | string array |
| `indexer.el.trusted_peers` | string array |
| `indexer.el.max_sessions` | integer |
| `indexer.l1.enabled` | boolean |
| `indexer.l1.checkpoint` | string |
| `indexer.l1.listen_addr` | string |
| `indexer.l1.advertised_addr` | string |
| `indexer.l1.beacon_listen_addr` | string |

### server

| Field | Type |
|---|---|
| `server.data_dir` | path |
| `server.profile` | string |
| `server.unsafe_max_bytes` | integer |
| `server.log_file` | path |
| `server.stream.listen_addr` | string |
| `server.stream.api_keys` | string array |
| `server.stream.max_subscriptions` | integer |
| `server.stream.max_flights` | integer |
| `server.stream.max_builds` | integer |
| `server.stream.flight_queue_ms` | integer |
| `server.p2p.listen_addr` | string |
| `server.p2p.advertised_addr` | string |
| `server.p2p.bootnodes` | string array |
| `server.p2p.max_peers` | integer |
| `server.el.enabled` | boolean |
| `server.el.sync` | boolean |
| `server.el.listen_addr` | string |
| `server.el.advertised_addr` | string |
| `server.el.bootnodes` | string array |
| `server.el.trusted_peers` | string array |
| `server.el.max_sessions` | integer |
| `server.l1.enabled` | boolean |
| `server.l1.checkpoint` | string |
| `server.l1.listen_addr` | string |
| `server.l1.advertised_addr` | string |
| `server.l1.beacon_listen_addr` | string |
| `server.export` | boolean |
| `server.chunks_dir` | path |
| `server.id` | string |
| `server.address` | string |
| `server.balancer_url` | string |
| `server.balancer_server_key` | string |
| `server.read_budget_mb` | integer |
| `server.export_id` | string |

### balancer

| Field | Type |
|---|---|
| `balancer.listen_addr` | string |
| `balancer.server_keys` | string array |
| `balancer.api_keys` | string array |
| `balancer.log_file` | path |

### importer

| Field | Type |
|---|---|
| `importer.state_dir` | path |
| `importer.api_token` | string |
| `importer.rpc_endpoint` | string |
| `importer.endpoint` | string |
| `importer.l1_endpoint` | string |
| `importer.verify_threads` | integer |
| `importer.verify_uploads` | integer |
| `importer.first_block` | integer |
| `importer.last_block` | integer |
| `importer.anchor_hash` | string |
| `importer.legacy_only` | boolean |
| `importer.chunk_blocks` | integer |
| `importer.fill_from` | string |
| `importer.rpc_batch` | integer |
| `importer.rpc_requests` | integer |
| `importer.requests` | integer |
| `importer.refetch_incomplete` | boolean |
| `importer.refetch_requests` | integer |
| `importer.balancer_url` | string |
| `importer.api_key` | string |
