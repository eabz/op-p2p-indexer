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

Ubuntu 22.04 or newer, x86-64, as root (it writes `/usr/local/bin` and systemd units):

```bash
sudo bash -c "$(curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh)"
```

The installer takes no options. Two checklists (↑/↓ move, Space ticks, Enter confirms): the
programs, then the chains. It installs the programs from the latest release (checksum
verified) to `/usr/local/bin`; missing prerequisites are installed with apt in the background.

Each service (`server`, `indexer`, `balancer`) runs once per chain, as
`op-indexer-<program>-<chain>`, run by the account that ran sudo, from its own directory:

```text
~/.op-indexer/config.toml             shared by every chain: keys, R2 (each chain extends it)
~/.op-indexer/<chain>/config.toml     the chain, a section per program with its own ports
~/.op-indexer/<chain>/data/<program>/ the program's state
~/.op-indexer/<chain>/<program>.log
```

Each chain gets its own block of ports (the defaults, plus 1000 per chain, recorded in its
file), so chains never collide. A program added later gets its section appended; nothing
written is rewritten. Services whose configuration was just written are not started: fill in
the keys, then `sudo systemctl start op-indexer-<program>-<chain>`. Run the installer again to
update (services whose configuration checks are restarted), or to add or remove programs and
chains: what is unticked is stopped and removed; configuration and data are kept.

## Configure and run

Keep reusable settings in `~/.op-indexer/config.toml`:

```toml
[node]
profile = "live"

[node.stream]
api_keys = ["replace-with-client-key"]
```

Each chain selects that shared file and its own roles. For example,
`~/.op-indexer/unichain/config.toml`:

```toml
extends = "../config.toml"
chain = "unichain"

[indexer]
data_dir = "data/indexer"

[indexer.stream]
listen_addr = "127.0.0.1:50051"
```

Shared `[node]` settings apply to configured indexer/server roles; explicit role settings
override them.

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
sudo systemctl restart op-indexer-server-unichain
sudo systemctl status op-indexer-server-unichain
tail -f ~/.op-indexer/unichain/server.log
```

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
