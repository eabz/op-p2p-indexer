# Supported chains and limits

| Chain | Status |
|---|---|
| OP Mainnet | Live nodes run, with L1 commitment (safe and finalized heads). |
| Unichain | Live nodes run; a fleet of servers and a balancer serves its history from R2. |
| Base | Supported; its history import is in progress. |

## Known limits

- **Not an execution node.** No balances, `eth_call`, traces or EVM state; no batch
  derivation.
- **Flight reads ranges.** It does not run SQL or filter by address or topic: read the
  range, then filter in your own tools.
- **Safe and finalized heads need L1 tracking**, which is off by default. Without it, a node
  reports only its unsafe head (a server reports the last sealed block as finalized).
- **No built-in TLS.** Keep the API on loopback, or put it behind a TLS proxy or a private
  network.
- **Still to be proven in long runs:** a server failing over during a read, a safe head that
  contradicts the unsafe chain, and several days of serving peers.

The [roadmap](../roadmap.md) tracks the work in progress, and the [benchmarks](../bench.md)
record measured read throughput.
