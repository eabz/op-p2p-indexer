# Configuration

Every binary (`indexer`, `server`, `balancer`, `import`) reads `OP_INDEXER_*` environment
variables, from the process environment or from `.env` in the current directory (`--env-file
<path>`, or `OP_INDEXER_ENV_FILE`, for another file). The process environment wins over the file.

Only what differs between machines goes in `.env`:

- what to run: the chain, the data directory, and the role switches;
- secrets: the R2 keys, API keys, server keys and the HyperSync token;
- where things are: the balancer, the server's public address, the R2 account and bucket, and
  the L1 checkpoint.

[`.env.example`](../.env.example) lists exactly those, with a short required block per binary
at the top. The rest has defaults that fit. The tuning knobs are sized from the machine's
cores and memory, and the node logs what they chose at startup. The importer's options are
flags. Every other variable is still read: see [Advanced settings](#advanced-settings).

## Minimal `.env` per binary

`indexer`: nothing is required, and an empty `.env` runs OP Mainnet with gossip only. A full
node that follows L1 needs:

```sh
OP_INDEXER_L1_ENABLED=true
OP_INDEXER_L1_CHECKPOINT=0x...
```

L1 turns on the range sync, which turns on the execution network.

`server` (one per machine; one of them exports):

```sh
OP_INDEXER_DATA_DIR=data-op-server
OP_INDEXER_L1_ENABLED=true
OP_INDEXER_L1_CHECKPOINT=0x...
OP_INDEXER_STREAM_LISTEN_ADDR=0.0.0.0:50051
OP_INDEXER_STREAM_API_KEYS=...
OP_INDEXER_R2_ACCOUNT_ID=...
OP_INDEXER_R2_ACCESS_KEY_ID=...
OP_INDEXER_R2_SECRET_ACCESS_KEY=...
OP_INDEXER_BALANCER_URL=http://balancer.internal:50060
OP_INDEXER_BALANCER_SERVER_KEY=...
# On the exporter only, with the write key:
# OP_INDEXER_EXPORT=true
```

The server's name is its host name. Its address is the public IP the execution network learns,
with the stream's port (see `OP_INDEXER_SERVER_ADDRESS` below).

`balancer`:

```sh
OP_INDEXER_BALANCER_SERVER_KEYS=...
OP_INDEXER_STREAM_API_KEYS=...
OP_INDEXER_R2_ACCOUNT_ID=...
OP_INDEXER_R2_ACCESS_KEY_ID=...
OP_INDEXER_R2_SECRET_ACCESS_KEY=...
```

`import` (`run`: download, then verify and upload):

```sh
OP_INDEXER_IMPORT_API_TOKEN=...
OP_INDEXER_R2_ACCOUNT_ID=...
OP_INDEXER_R2_ACCESS_KEY_ID=...
OP_INDEXER_R2_SECRET_ACCESS_KEY=...
```

The range and the rest are flags: `import run --help`.

## Advanced settings

These are out of `.env.example` because their defaults fit almost every deployment. Set one in
`.env` or the environment to override it.

### Sized from the machine

Each default is computed at startup from the cores the process may use
(`available_parallelism`, a container's CPU quota included) and its memory. On Linux the memory
is `MemTotal`, lowered to a cgroup's limit; on macOS it is `hw.memsize`. The node logs the
inputs and the results on one line ("settings sized from the machine"), and the server logs
its read budget. Memory is shared out in eighths, so the three memory-sized settings together
take at most three eighths of it, and the rest is left to the page cache and fjall. The formulas
are in `crates/node/src/sizing.rs`.

| Variable | Default | Unknown memory |
|---|---|---|
| `OP_INDEXER_UNSAFE_MAX_BYTES` | memory / 8, from 256 MiB to 2 GiB (about a day of a busy chain's blocks; more is never read) | 2 GiB |
| `OP_INDEXER_STREAM_MAX_BUILDS` | Flight builds at once, server-wide: 2 × cores, but no more than (memory / 8) / 100 MiB (each build holds about 100 MiB until sent), and at least 2 | 2 × cores |
| `OP_INDEXER_STREAM_MAX_FLIGHTS` | Flight `DoGet` streams at once: the build cap, at least 8. A stream runs up to two builds at once, so half of them keep every build busy; the others wait their turn while their consumers read | — |
| `OP_INDEXER_EL_MAX_SESSIONS` | `indexer`: 4, whatever the machine (full nodes ration their slots); `server`: 2 × cores, from 8 to 64 | — |
| `OP_INDEXER_SERVER_READ_BUDGET_MB` | server only: memory / 8, from 256 MiB to 16 GiB: decoded blocks read ahead and chunk streams open, for every reader together | 1 GiB |

Some limits are deliberately not sized from the machine. They bound network and R2 use, not
memory, so they are constants:

- the server's peer reads: 16 at once, matching the execution network's serving limit;
- the bytes served per peer: 4 GiB a minute.

### Derived

| Variable | Default |
|---|---|
| `OP_INDEXER_SERVER_ID` | The host name (`uname`). It is the server's name at the balancer and, on the exporter, in the manifest. |
| `OP_INDEXER_SERVER_ADDRESS` | Where the balancer sends clients. If `OP_INDEXER_STREAM_LISTEN_ADDR` names an IP (`127.0.0.1:50051`, `10.0.0.5:50051`), that address as it is. If it listens on every interface (`0.0.0.0:50051`), the public IP the execution network's discovery learns, with the stream's port. The server waits for that IP before it registers: tens of seconds after start. It warns after a minute and keeps waiting. Without the execution network the variable is required. |
| `OP_INDEXER_EL_ENABLED` | `true` with a profile, or with the range sync (which needs it); otherwise `false`. |
| `OP_INDEXER_EL_SYNC` | `true` with the `archive` or `fleet` profile, or with L1 (which needs it); otherwise `false`. |

An explicit `false` where a switch above needs `true` stops startup with an error naming the
variable.

### Everything else

Node (`indexer`, `server`; documented on `Config::from_env` in `crates/node/src/config.rs`):

| Variable | Default |
|---|---|
| `OP_INDEXER_PROFILE` | unset. `live`, `archive` or `fleet` pick the role switches' defaults in one line. |
| `OP_INDEXER_P2P_LISTEN_ADDR` | `0.0.0.0:9222` |
| `OP_INDEXER_P2P_ADVERTISED_ADDR` | unset: the address peers observe; set it behind NAT |
| `OP_INDEXER_P2P_BOOTNODES` | the chain's bootnodes |
| `OP_INDEXER_P2P_MAX_PEERS` | 30 |
| `OP_INDEXER_EL_LISTEN_ADDR` | `0.0.0.0:30303` |
| `OP_INDEXER_EL_ADVERTISED_ADDR` | unset: learned by discovery |
| `OP_INDEXER_EL_BOOTNODES` | the chain's execution bootnodes |
| `OP_INDEXER_EL_TRUSTED_PEERS` | none: `enode://` URLs of the deployment's other servers |
| `OP_INDEXER_L1_LISTEN_ADDR` | `0.0.0.0:30304` |
| `OP_INDEXER_L1_BEACON_LISTEN_ADDR` | `0.0.0.0:9001` |
| `OP_INDEXER_L1_ADVERTISED_ADDR` | unset |
| `OP_INDEXER_STREAM_MAX_SUBSCRIPTIONS` | 64 |
| `OP_INDEXER_STREAM_FLIGHT_QUEUE_MS` | 2000: how long a `DoGet` waits for a free stream before `RESOURCE_EXHAUSTED` |

Server (documented on `ServerConfig::from_env_and_args` in `bin/server/src/config.rs`):

| Variable | Default |
|---|---|
| `OP_INDEXER_R2_PREFIX` | `archive` |
| `OP_INDEXER_R2_ENDPOINT` | `https://<account id>.r2.cloudflarestorage.com` |
| `OP_INDEXER_R2_PUBLIC_URL` | unset: every read through the S3 API ([serving.md](serving.md) 6.9) |
| `OP_INDEXER_CHUNKS_DIR` | unset: reads the chunks from a local directory instead of R2 (local runs, the bench) |

Balancer: `OP_INDEXER_BALANCER_LISTEN_ADDR` (`0.0.0.0:50060`).

Importer (environment fallbacks of its flags, kept for endpoints, whose URLs can hold a key):

| Variable | Default |
|---|---|
| `OP_INDEXER_IMPORT_ENDPOINT` | the chain's HyperSync endpoint |
| `OP_INDEXER_IMPORT_L1_ENDPOINT` | `https://eth.hypersync.xyz` |

Every binary:

| Variable | Default |
|---|---|
| `RUST_LOG` | `info` |
| `OP_INDEXER_LOG_FILE` | `<binary>.log` next to the `.env` file, for `start`, `status` and `logs` |
| `OP_INDEXER_ENV_FILE` | `.env` |

### A second instance on the same host

Each instance needs its own data directory, its own ports and its own `.env`. For example,
Unichain next to an OP Mainnet node, with every port shifted by 100:

```sh
OP_INDEXER_CHAIN_ID=130
OP_INDEXER_P2P_LISTEN_ADDR=0.0.0.0:9322
OP_INDEXER_EL_LISTEN_ADDR=0.0.0.0:30403
OP_INDEXER_L1_LISTEN_ADDR=0.0.0.0:30404
OP_INDEXER_L1_BEACON_LISTEN_ADDR=0.0.0.0:9101
OP_INDEXER_STREAM_LISTEN_ADDR=127.0.0.1:50151
```

The data directory follows the chain (`data-unichain`). A second instance of the same chain also
sets `OP_INDEXER_DATA_DIR`. Give the importer one state directory per chain.

## Deprecated variables

Each of these is still read for this release, with one warning at startup that names its
replacement. None of them is logged with its value.

| Variable | Instead |
|---|---|
| `OP_INDEXER_EXPORT_ID` | Automatic: the exporter is named after the server (`OP_INDEXER_SERVER_ID`, default the host name). |
| `OP_INDEXER_IMPORT_FIRST_BLOCK` | `--first-block` |
| `OP_INDEXER_IMPORT_LAST_BLOCK` | `--last-block` |
| `OP_INDEXER_IMPORT_ANCHOR_HASH` | `--anchor-hash` |
| `OP_INDEXER_IMPORT_LEGACY_ONLY` | `--legacy-only` |
| `OP_INDEXER_IMPORT_CHUNK_BLOCKS` | `--chunk-blocks` |
| `OP_INDEXER_IMPORT_FILL_FROM` | `--fill-from` |
| `OP_INDEXER_IMPORT_RPC_BATCH` | `--rpc-batch` |
| `OP_INDEXER_IMPORT_RPC_REQUESTS` | `--rpc-requests` |
| `OP_INDEXER_IMPORT_REQUESTS` | `--requests` |
| `OP_INDEXER_IMPORT_REFETCH_INCOMPLETE` | `--refetch-incomplete` |
| `OP_INDEXER_IMPORT_REFETCH_REQUESTS` | `--refetch-requests` |
| `OP_INDEXER_IMPORT_VERIFY_THREADS` | `--threads` |
| `OP_INDEXER_IMPORT_VERIFY_UPLOADS` | `--uploads` |
| `ENVIO_API_TOKEN` | `OP_INDEXER_IMPORT_API_TOKEN` |
