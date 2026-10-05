# Install and start a node

This walkthrough uses **v0.1.12**, Ubuntu 22.04 or newer and an x86-64 machine with glibc 2.35+. Use an interactive SSH terminal and a sudo-enabled account. Reserve storage for your intended dataset; a live node is a smaller first step than importing a whole chain.

## Install

```bash
curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh | sudo bash -s -- --version v0.1.12
```

The script downloads release binaries and verifies their published checksums. It can install missing Ubuntu prerequisites. If curl is missing, install `curl` and `ca-certificates` with apt first. To review the script before execution, download the same URL to `install.sh`, inspect it, then run `sudo bash install.sh --version v0.1.12`.

Choose **Unichain** and the **indexer** role for a standalone local-data node. Review the wizard's paths, ports and service registration before applying them. For this first run, leave automatic startup off. A `server` role additionally requires configured object storage; a `balancer` does not ingest blocks.

## Configure explicitly

Use the config path shown by setup. The released default is `~/indexer/unichain/config.toml`, under the service owner's home. Development versions can use different defaults; do not mix their instructions with this release.

For a simple foreground run, this independent file avoids inherited settings:

```bash
mkdir -p "$HOME/chain-data-demo"
cat > "$HOME/chain-data-demo/config.toml" <<'TOML'
chain = "unichain"
[indexer]
data_dir = "data"
profile = "live"
[indexer.stream]
listen_addr = "127.0.0.1:50051"
[indexer.el]
enabled = true
TOML
chmod 600 "$HOME/chain-data-demo/config.toml"
indexer --config "$HOME/chain-data-demo/config.toml" --check-config
indexer --config "$HOME/chain-data-demo/config.toml"
```

The process stays in this terminal; run the query in another terminal on the same host. The first command validates configuration, not connectivity. Wait for logs to show peer connections and received blocks. Discovery and receipts can take time; an open process alone is not proof of a healthy chain feed.

This starts live collection, not a complete historical archive. L1 tracking is off in the live profile, so safe/finalized heads may be absent. [Read ten available blocks](query.md) before enabling additional capabilities.

## Run as a service

Use the wizard-generated configuration for a registered service instead of running a second node with overlapping ports. In v0.1.12 the indexer service is `indexer-unichain`:

```bash
sudo systemctl start indexer-unichain
sudo systemctl status indexer-unichain
sudo journalctl -u indexer-unichain -f
```

Enable boot startup only after checking the config and observing healthy operation. [Configuration reference](../configuration.md) covers profiles, inheritance, keys and migration. [Operations](operate.md) covers upgrades and recovery.
