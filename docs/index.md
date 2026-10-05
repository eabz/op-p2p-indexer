# Own your chain data

op-p2p-indexer brings OP Stack blocks, transactions, receipts and logs into your application,
straight from the chain's peer-to-peer network, and keeps them current. You run it on your
own infrastructure.

## Get started

1. [Install and start a node](guides/start.md) on a Linux server.
2. [Read ten blocks](guides/query.md) with Python and Arrow Flight.
3. [Follow live updates](guides/live.md), including receipts and reorganizations.
4. [Operate it](guides/operate.md): health, access, backups and upgrades.

## What it does

- **Collects from peers.** Joins the chain's gossip and execution networks; it needs no L1
  or L2 RPC at runtime.
- **Verifies.** Checks sequencer signatures, block hashes and the transaction and receipt
  roots. With L1 tracking on, it also follows the safe and finalized heads.
- **Serves.** Arrow Flight for block ranges as columnar tables, gRPC for history followed by
  live blocks, receipts and reorgs.
- **Scales out.** One node with a local archive, or a fleet of servers reading history from
  object storage behind a balancer.

It is a data source, not an execution node: no balances, `eth_call`, traces or EVM state.
You bring the database and the transformations.

Supported chains: OP Mainnet, Base and Unichain. See [supported chains and limits](guides/readiness.md).

## For agents

Every page is available as Markdown (the **View Markdown** link on each page), and the whole
documentation is indexed in [`llms.txt`](https://eabz.github.io/op-p2p-indexer/llms.txt) and concatenated in
[`llms-full.txt`](https://eabz.github.io/op-p2p-indexer/llms-full.txt).
