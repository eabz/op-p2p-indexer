# Install and start a node

You need Ubuntu 22.04 or newer on x86-64, and a terminal on it with sudo. Leave room on disk
for the data you want: a live node is a small first step; a whole chain's history is not.

## Install

```bash
sudo bash -c "$(curl -fsSL https://eabz.github.io/op-p2p-indexer/install.sh)"
```

The installer takes no options. It asks two questions, with ↑/↓ to move, Space to tick and
Enter to confirm:

1. **Which programs.** For a first node, tick `indexer`: a full node with its own local
   archive. `server` and `balancer` are for serving many clients from object storage (see
   [fleet and object storage](../serving.md)); `import` and `bench` are tools you run by hand.
2. **Which chains.** Tick `unichain` for a first run. You can run several chains on one
   machine; each gets its own ports.

It downloads the latest release, checks its checksum, installs the programs to
`/usr/local/bin`, and creates one systemd service per program and chain, named
`<chain>-<program>` (here `unichain-indexer`).

## Configure

Everything lives under `~/.op-indexer/`:

```text
~/.op-indexer/config.toml              shared by every chain: API keys, R2
~/.op-indexer/unichain/config.toml     the chain, with a section per program
~/.op-indexer/unichain/data/indexer/   the indexer's data
~/.op-indexer/unichain/indexer.log     its log
```

A new service is not started. Check its configuration, then start it:

```bash
indexer --config ~/.op-indexer/unichain/config.toml --check-config
sudo systemctl start unichain-indexer
tail -f ~/.op-indexer/unichain/indexer.log
```

Wait for the log to show peers and received blocks; finding peers can take a minute. A live
node collects new blocks from now on, not the chain's history, and safe and finalized heads
need L1 tracking (see [trust and finality](trust.md)).

Then [read ten blocks](query.md).

## Change or update

Run the installer again. It offers to **update** the programs to the latest release, **add
services** (more programs or chains) or **remove services**. Removing stops a service and
deletes its unit; its configuration and data stay where they are, and the installer lists
them.

The [configuration reference](../configuration.md) lists every setting. [Operations](operate.md)
covers health, backups and upgrades.
