# Own your chain data

Bring OP Stack blocks, transactions, receipts and logs into your application, then keep your dataset current with live updates. Run the data source yourself and choose where your application stores and processes its output.

This guide targets **v0.1.12**. Start with a small local read before committing to a historical import or a serving fleet.

## Your first workflow

1. [Install and start a node](guides/start.md) on a Linux server.
2. [Read ten blocks](guides/query.md) with Python and Arrow Flight.
3. [Follow live updates](guides/live.md), including receipts and reorganizations.
4. [Operate the service](guides/operate.md) with explicit recovery and access controls.

## Where it fits

Use this project as the data input to an indexer, an analytics pipeline or your own chain dataset. Arrow Flight supplies columnar ranges; gRPC supplies ordered history and live events. You bring the database, transformations and application.

The running node connects to consensus and execution peers without L1 or L2 RPC. Historical bootstrapping is a separate task: the importer can use HyperSync and RPC to complete archived records. Fleet serving uses object storage.

It is not an execution node: balances, `eth_call`, traces, EVM state and batch derivation are outside its scope. Flight does not implement SQL or address/topic filters.

## Choose with evidence

OP Mainnet and Unichain have live-run evidence. Base is implemented, but its complete import and live operation remain validation work. See [readiness and limits](guides/readiness.md) before choosing a deployment, and [trust and finality](guides/trust.md) before deciding what your application treats as settled.

This is an early open-source project under the MIT license. Production reliability is not yet established for all recovery and failover paths. [Contribution and governance](guides/community.md) explains how to participate and how decisions are made today.
