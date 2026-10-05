# Roadmap and delivery status

Updated 2026-10-05. This is the current work list. [Architecture](architecture.md) explains
how the system fits together; [historical decisions](decisions.md) preserve earlier designs.
**Finished implementation does not mean production verified.** Recorded runs below are prior
evidence unless explicitly identified as checks of the simplification branch.

## Finished implementation

| Area | Delivered | Verification recorded |
|---|---|---|
| Chain support | OP Mainnet, Unichain and Base configuration; one chain per node | Base's complete node/import path remains pending |
| Peer ingestion | Gossip validation, execution receipts, range sync and peer serving | Gossip and tip receipts run live; complete range/serving exercise pending |
| Persistence | Unsafe chain with journal, local fjall archive, receipt attachment and promotion | Runtime verification of recovery and conflicting safe heads remains pending |
| L1 tracking | Beacon light client, saved checkpoints and dispute-game claim matching | Light client followed live and restarted; full L1-to-promotion run pending |
| Consumer APIs | gRPC history/live subscriptions, lookups, reorg events, Arrow Flight and API keys | Local serving benchmarks recorded; production end-to-end pending |
| Fleet storage | Immutable chunks, manifest, hash index, local tail and single exporter | Local chunk/reader benchmarks recorded; R2 fleet validation pending |
| Fleet directory | Registration, health/contiguity checks, load reporting, Flight scheduling and Locate | Local fake-server routing, keys, disconnect and timeout checks recorded |
| Importer | Resumable download/fill and verify/upload; `run` joins the steps | Prior OP/Unichain verification recorded; latest combined path and Base pending |

### Finished in the simplification branch

- [x] Extract shared environment loading, logging and signals into `runtime`.
- [x] Extract authentication, Flight tickets and Arrow schemas into `api`; remove the
  balancer's dependency on node, stream implementation and storage engine.
- [x] Share R2 configuration parsing while retaining importer command-line precedence.
- [x] Add opt-in `live`, `archive` and `fleet` profiles, effective-capability logging and
  validation; preserve legacy settings when no profile is selected.
- [x] Separate node wiring, sync coordination, persistence and supervision; name tasks and
  propagate unexpected exits while preserving staged shutdown.
- [x] Give Flight and subscription history readers their own archive stream and cancellation;
  retain shared memory budgets and remove the shared cursor map and idle eviction.
- [x] Separate current architecture, this work list and historical decisions; correct stale
  balancer, authentication and bandwidth-reporting descriptions.

### Dead-code and redundancy cleanup

- Removed the unused `StorageError::is_archive_locked` helper and trait bounds already
  guaranteed by `ArchiveStore`.
- Shared empty-environment-value handling and signal registration through `runtime`, keeping
  the importer's SIGINT fallback when SIGTERM registration fails.
- `Table::schema()` now returns `SchemaRef` directly; callers no longer handle an impossible
  schema-construction error. Rust consumers must remove `?`/`map_err` on this method; the
  Arrow wire schemas and compatibility reexports are unchanged.

### Branch verification

- All four binaries build; strict workspace Clippy, rustdoc, dependency-policy and
  unused-dependency checks pass.
- Configuration rejects unknown profiles, missing archive checkpoints and incompatible
  overrides; the indexer rejects the fleet profile. The importer's env-file/help path works.
- Legacy, live and overridden-live nodes start with temporary data and loopback listeners,
  then exit cleanly on SIGTERM. This verifies lifecycle wiring, not live-chain correctness.
- A scratch run against real fjall verifies bounded batches, continuous ranges, middle-range
  starts and empty reads beyond the tip. A controlled chunk source verifies independent
  same-start readers, cancellation of stalled producers, and error/panic propagation.
- Independent review confirms all 64 Arrow fields retain their wire order, type and
  nullability; every produced batch is checked against the shared schema.

These checks use local temporary data and a controlled chunk source, not production R2.

## Pending implementation

| Priority | Work | Done when |
|---|---|---|
| P1 | Reconcile an unsafe canonical branch when a new safe head contradicts it ([storage §3.2](storage.md#32-fork-choice-and-reorgs)) | The store removes contradicted entries, resets the unsafe head and emits the required reorg; recovery is demonstrated with conflicting branches |
| P1 | Harden dispute-game selection and retention ([L1 trust](l1.md#3-trust), §5) | Portal respected-type policy is defined and checked; matched claims survive unrelated-game churn through finalization |
| P2 | Bootstrap the standalone indexer's local archive from sealed chunks ([serving §5.6](serving.md#56-the-indexers-history-from-chunks-design-not-built)) | A verified local anchor supports resumable historical prepend, including the gap above sealed history |

These are outstanding platform work, not changes to the trust model made by this refactor.
The light client already warns about a stalled head and attempts committee catch-up; the old
roadmap's claim that it always stalls silently has been removed. Recovery still needs the
end-to-end exercises below.

## Pending validation and release gates

| Priority | Exercise | Evidence needed |
|---|---|---|
| P1 | Full L1 → claim matching → promotion → consumer status | A real node advances safe/finalized heads correctly, survives restart and recovers after interrupted peer access |
| P1 | R2 fleet and exporter ([bench plan](serving.md#7-the-bench-3-to-4-small-droplets-one-r2-bucket)) | Conditional publication, concurrent reads, manifest refresh, bounded memory and no missing/duplicate blocks across tail/sealed boundaries |
| P1 | Client failover and history-to-live transitions | Kill a serving node during Flight and subscription reads; demonstrate correct resume, receipt delivery and reorg handling |
| P1 | Current importer and Base support | Latest verify/upload completes against its anchor and resumes after interruption; Base's full node path runs successfully |
| P2 | Sustained peer serving and range sync | Measured access, throughput, honest advertised coverage and no starvation of ingestion |

Attach revision, environment, input range and observed results to each completed gate. Prior
local probes and successful compilation do not close production validation gates.

## Decisions pending measurement

- **Separate historical query servers from ingestion?** Measure duplicated networking and
  fleet cost first. A replacement needs live-data delivery, failover and peer-participation
  design; current servers remain full nodes.
- **Keep the custom balancer?** Measure the need for per-chunk parallel Flight scheduling and
  direct-to-server transfer before replacing it with ordinary load balancing.

## Scope boundaries

The running node uses peers rather than L1/L2 RPC. The standalone indexer needs no external
database; fleet servers use object storage, and the separate importer may use archive/RPC
sources. Safe/finalized retain the [documented claim-based meaning](l1.md#3-trust). Verification,
bounded memory, peer limits and one-chain-per-node isolation remain requirements.

Balances, `eth_call`, traces, full transaction execution and batch derivation are outside the
current delivery scope. TLS inside the binaries, log filters and additional Flight services
are optional API work ([stream §7](stream.md#7-not-built)), not prerequisites automatically
added by this simplification.
