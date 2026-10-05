# op-p2p-indexer

A self-hosted source of **OP Mainnet, Unichain and Base** blocks, transactions, receipts and
logs. Nodes sync and verify data through peer-to-peer networks and serve it through gRPC and
Apache Arrow Flight. The running node needs no L1 or L2 RPC.

| Program | Purpose |
|---|---|
| `indexer` | Standalone node with a local fjall archive |
| `server` | Fleet node reading sealed history from R2; one server can export finalized chunks |
| `balancer` | Direct clients to healthy servers with the requested history |
| `import` | Download, verify and upload initial history; fetch sealed chunks |
| `bench` | Benchmark Flight reads, including a four-table heavy workload |

## Install on a server

Ubuntu 22.04 or newer, x86-64, from a sudo-enabled account:

```bash
curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh | sudo bash
```

On a fresh box without curl, first run `sudo apt-get update && sudo apt-get install -y curl ca-certificates`.
The installer handles its remaining Ubuntu prerequisites.

Use ↑/↓ to move, Space to toggle services, and Enter to confirm. The wizard reuses shared TOML settings and
shows the selected paths and ports before saving. Downloads display progress and stalled
transfers time out. It can register and start systemd services. Run it again to update binaries
or change registrations;
configuration and data are retained. Downloads use the latest published release and verify
its checksum before replacing binaries atomically. An update takes effect when a process
restarts. Shared binaries live in `/usr/local/bin`.

**Release status:** the TOML-only configuration described here is a breaking change pending
publication. The installer downloads published artifacts, not the current source tree.
Old releases can be installed with `--binaries-only`; guided setup requires compatible binaries.

For unattended setup, download the script and specify the chain and roles:

```bash
bash install.sh --non-interactive --chain unichain --roles indexer --register --enable --start
bash install.sh --action update --non-interactive --chain unichain --roles server --restart
bash install.sh --binaries-only --version v0.1.8 --prefix "$HOME/.local"
```

Put required secrets in the private chain configuration before unattended setup. The importer
is an on-demand command, not a restarting service.

## Configure and run

Keep reusable settings in `~/indexer/config.toml`:

```toml
[node]
profile = "live"

[node.stream]
api_keys = ["replace-with-client-key"]
```

Each chain selects that shared file and its own roles. For example,
`~/indexer/unichain/config.toml`:

```toml
extends = "../config.toml"
chain = "unichain"

[indexer]
data_dir = "data/indexer"

[indexer.stream]
listen_addr = "127.0.0.1:50051"
```

The installer assigns separate state directories and ports to each role. Shared `[node]`
settings apply to configured indexer/server roles; explicit role settings override them.

```bash
indexer --chain unichain --check-config
indexer --chain unichain
# Or select an explicit file:
indexer --config /path/to/config.toml
```

Operational CLI flags override the chain file, then inherited settings, then built-in defaults. Runtime environment
configuration and `.env` loading are removed; `--env-file` is rejected. Relative paths resolve
beside the file that defines them. Keep credentials private with mode `0600`.
See [configuration and migration](docs/configuration.md) and [the example](config.toml.example).
For a local source build, run `cargo build --release` and use the binaries in `target/release`.

## Control services

```bash
sudo systemctl restart server-unichain
sudo journalctl -u server-unichain -f
sudo systemctl start indexer-chain-unichain.target
sudo systemctl stop indexer-chain-unichain.target
```

A chain target controls all registered roles together. Enabling a selected service starts only
that service at boot. Stop and disable old registrations before starting replacements against
the same state. Removing registrations keeps configuration and data.

Without systemd, `indexer`, `server` and `balancer` also support `start`, `stop`, `restart`,
`status` and `logs`; logs and PID files live beside the selected config. Use `--log-file` to
choose another log path. Foreground execution is the default.

## Benchmark

Add `[bench]` settings to the same private config:

```toml
[bench]
balancer_url = "http://balancer.internal:50060"
api_key = "replace-with-client-key"
```

```bash
bench --chain unichain --heavy --from 48000000 --to 48100000 \
  --concurrency 24 --per-server 8 --compression zstd --repeat 3 --json results.json
```

The native client validates plan coverage and batches, bounds concurrent reads and retries,
and reports useful versus received bytes. See [benchmark usage and measurements](docs/bench.md).

## Documentation

- [Configuration, defaults and migration](docs/configuration.md)
- [Architecture](docs/architecture.md), [roadmap](docs/roadmap.md) and [decisions](docs/decisions.md)
- [gRPC and Arrow Flight](docs/stream.md), [fleet serving](docs/serving.md) and [importing history](docs/import.md)
- [Storage](docs/storage.md), [pipeline](docs/pipeline.md), [execution peers](docs/el.md) and [L1](docs/l1.md)

MIT licensed; see [LICENSE](LICENSE).
