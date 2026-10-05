# Readiness and limits

Assessment based on repository run records through **2026-10-05**, for the v0.1.12 documentation baseline. These are observations, not an uptime commitment or continuous status feed.

| Chain | Evidence | Decision today |
|---|---|---|
| OP Mainnet | Live peer ingestion and L1 commitment work recorded. | Evaluate on a bounded workload; validate your own coverage and recovery. |
| Unichain | Three servers and a balancer served R2-backed history; 1,694 sealed chunks recorded. | Best-documented fleet evaluation path; complete operational gates before reliance. |
| Base | Chain support and importer exist; completeness/refetch and verification remain work. | Experimental until complete import and a live node are demonstrated. |

## Before a production dependency

| Gate | Evidence still needed |
|---|---|
| Fresh installation | Fresh Ubuntu installation, real service startup, migration and boot restart. |
| Complete Base data | Verified anchor, continuous manifest and live Base node. |
| Exporter boundary | New finalized chunk sealed and served with no omitted/duplicated block at the tail boundary. |
| Chain reconciliation | Safe-head contradiction repairs the unsafe chain and emits the correct reorg. |
| Claim selection | Respected game type and retained matched claims handled correctly. |
| Failover | Kill a server during Flight and subscriptions; recover without lost blocks, receipts or reorgs. |
| Peer durability | Several days of honest serving without starving ingestion. |

The engineering [roadmap](../roadmap.md) tracks implementation work. Release snapshots can contain historical wording such as “pending release”; that wording describes the source record and does not override this site's explicitly selected version.

## Performance evidence

Existing [benchmark reports](../bench.md) give concrete ranges, client workloads and measured throughput. They establish that those reads ran; they do not establish sustained fleet capacity, a hardware-independent rate or superiority over another provider. Repeat a workload with server telemetry before using it to plan capacity.

## A useful pilot

For an indexer or analytics team, choose one chain and one dataset. Record time to first successful query, missing/late receipts, recovery behavior, storage cost and the work required to maintain a correct consumer. Compare these against your current pipeline on the same workload.

A future hosted service should earn its place through easier operation, predictable recovery and useful support. There is no hosted product, pricing promise or SLA offered by this site today. Pilot feedback and reproducible reports are welcome through the [contribution process](community.md).
