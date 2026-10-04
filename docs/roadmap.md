# Scope and roadmap

This file records what the indexer is meant to do, the decisions that shape it, and the order
of work. Update it when a decision changes; link the spec for each piece instead of repeating it.

## Goal

Serve decoded OP Stack chain data to users: blocks, headers, transactions, receipts and logs.
Live data comes from Redis, historical data from ClickHouse.

## Decisions

| Date | Decision | Why |
|---|---|---|
| 2026-10-03 | **No dependency on external services.** The indexer does not call an L1 or L2 RPC and does not embed op-reth. | Embedding a node needs a full state sync and an L1 endpoint, which defeats a lightweight indexer. An RPC is an external dependency. |
| 2026-10-03 | **Receipts and logs come from L2 execution peers, verified against the header** (proposed, pending the viability test below). The indexer asks execution nodes for a block's receipts over the execution p2p network (devp2p `GetReceipts`), recomputes the receipts root, and accepts them only if it equals the `receiptsRoot` in the sequencer-signed header. | Receipts and logs are not in gossip and not on L1. Fetching and verifying them needs no state and no execution, and peers are the same kind of dependency as gossip. A peer cannot forge them. |
| 2026-10-03 | **Be a good peer.** On gossip the node already validates and forwards blocks. On the execution network it will serve headers, bodies and receipts for the range it holds, with limits. It cannot serve state. | Peers tend to deprioritise or drop nodes that only download. |
| 2026-10-03 | **A local block archive for serving, on fjall**. Committed blocks are also appended, in their consensus encoding, to an embedded store. Serving reads Redis for unsafe blocks and the archive for committed ones; ClickHouse is never used to serve peers. It is part of the `storage` crate, behind the `ArchiveStore` trait. The default keeps a 30-day window; keeping the whole history is an option. | Serving history from ClickHouse would cost several queries per block, with the load driven by strangers. The whole history is plausibly 300 to 500 GB today and grows by roughly 200 GB a year (a full OP Mainnet node is about 700 GB including state; our own measurement gives about 280 GB a year of headers and bodies at current block sizes). redb was built and measured first: it used 1.7 times the data on disk, returned no space when trimming without a slow full compaction, and its appends slowed as the file grew, so it is ruled out for that size. fjall is the only maintained, published, pure-Rust engine of the log-structured kind that suits append-and-trim data. NippyJar was evaluated and rejected: unpublished internal reth crate, receipts only in block order, two cargo-deny exceptions. |
| 2026-10-03 | **Trust model without execution.** Transactions are confirmed by L1 once their batch is derived. Receipts and logs are as trustworthy as the sequencer's signature over the header that commits to them. Independent proof would need execution, or output roots resolved on L1 after the challenge period. | Stated so users of the data know what each status means. The indexer will not serve balances, `eth_call` or traces: it has no state. |
| 2026-10-03 | *Fallback, not the plan:* receipts from our own execution, keeping an L2 state and replaying blocks. Only if the viability test shows execution peers will not serve us. | It works without any peer serving receipts, but needs the full state, an OP-correct EVM and upkeep for every hardfork. |
| 2026-10-03 | *Fallback only:* two states, a committed state (as of the last L1-committed block) and a live overlay of per-block changes for unsafe blocks. | Part of the execution fallback above; not needed if receipts come from peers. |
| 2026-10-03 | **L1 awareness comes from an L1 p2p layer**, not an L1 RPC. It supplies the safe and finalized heads. | Same reason as the first row. |
| 2026-10-03 | **Storage is built first and is independent of all of the above.** It takes an already decoded block, with or without its receipts, as input (`DecodedBlock`). How the block is decoded and where its receipts come from is solved in later crates. | Nothing in storage depends on where the data comes from, so it does not have to wait. |
| 2026-10-03 | Redis holds unsafe (live) blocks and handles reorgs; ClickHouse holds L1-committed and backfilled blocks. | Live data is small and changes shape; committed data is large and append-mostly. |
| 2026-10-03 | The two stores are named **unsafe** and **committed**, not hot and cold (`UnsafeStore`, `CommittedStore`). | The split is whether L1 has committed a block, not how often it is read. |
| 2026-10-03 | **Users read through one query layer**, not from Redis or ClickHouse directly. It routes each request to the unsafe or the committed store and labels results unsafe, safe or finalized. | One API regardless of where a block currently lives; the Redis key layout and the ClickHouse schema stay internal and free to change. |
| 2026-10-03 | **Stored data is disposable until the first release.** Migrations may be edited in place and databases dropped and recreated. From the first release on, applied migrations are never edited and data is kept. | Nothing is deployed yet; getting the schema right is worth more than preserving empty tables. The first release is the threshold; it has not been defined yet. |
| 2026-10-03 | **One embedded engine: fjall.** The p2p node store (identity and known peers) moves from redb to fjall too, and redb leaves the workspace. Existing data directories are not migrated: a node gets a new identity and an empty peer list. | One storage dependency to understand and audit instead of two. Data is disposable until the first release. |
| 2026-10-03 | Redis, not DragonflyDB. | One block every 2 seconds needs no extra throughput, and fork choice relies on Lua scripts that build key names from a prefix, which Dragonfly may not allow by default (not tested). The standard protocol keeps a later switch cheap. |
| 2026-10-03 | One chain: OP Mainnet. No dual-stack listening. No backfill of missed gossip blocks over op-node's consensus-layer request-response protocol (op-node is retiring it); backfill is planned over the execution p2p network instead (crate 4). | Decided during the p2p PR. |
| 2026-10-03 | **Promote now, backfill later.** Blocks are promoted to the committed store without waiting for receipts, and a hole in the committed range (a block missed on gossip) does not stall promotion: it is counted, the archive restarts, and backfill fills it later. `p2p` hands the pipeline the decoded block, not raw SSZ. | Nothing has receipts or backfill until the `el` crate exists; waiting would mean nothing is ever committed or pruned. Decoding once keeps SSZ out of the pipeline. |

## Order of work

| # | Crate | Role | Spec | Status |
|---|---|---|---|---|
| 0 | `p2p`, `chainspec`, `primitives` | L2 gossip: discovery, validation, unsafe blocks on a channel | crate docs | merged |
| 1 | `storage` | Unsafe store on Redis with fork choice, committed store on ClickHouse, local block archive, migrations, connectors | [storage.md](storage.md) | merged |
| 2 | `pipeline` | Decode gossip payloads into blocks, write them to the unsafe store, promote to the committed store when L1 commits, append committed blocks to the archive and trim it to its retention. Owns the retry policy. | [pipeline.md](pipeline.md) | in progress |
| 3 | `query` | The read path: block by number or hash, transaction by hash, logs by filter, the heads. Routes by the safe head (above it the unsafe store, at or below it the committed store), reads the unsafe store first across the promotion boundary, and labels every result unsafe, safe or finalized. Needs read methods on `CommittedStore` that do not exist yet. A transport (JSON-RPC or REST) sits on top of it. | not written | |
| 4 | `el` (execution p2p) | Connect to L2 execution peers over devp2p. Fetch receipts for each block and verify them against the header's receipts root, then attach them (`UnsafeStore::set_receipts`). Fetch headers and bodies by number to backfill blocks missed on gossip, verified by the hash chain. | not written | viability test first |
| 5 | `l1` | L1 p2p: batches and finality, giving the safe and finalized heads. Batches carry the user transactions, not deposits (those come from L1 logs), so matching a batch against a stored block confirms its user transactions. Comes with making the unsafe store reconcile itself when the safe head contradicts its canonical chain (storage.md 3.2, known gap). | not written | |
| 6 | `el`, serving | Give back to the execution network: answer header, body and receipt requests for the range we hold, and advertise that range honestly. Unsafe blocks come from the unsafe store, committed blocks from the local archive (storage.md section 9), within its retention window; nothing older is served. Only verified data is served, re-encoded and checked against the hash or root first. Request and bandwidth limits in configuration, so serving never starves ingestion. | not written | sized by the viability test |

**Viability test before crate 4** (runs alongside `pipeline`; it depends on nothing we have
built). A small throwaway program, run for a few days:

- Connect to OP Mainnet execution peers; log how many are found and how many stay connected.
- Request receipts for sample blocks from different eras: the last hour, the last week, the
  last year, and just after Bedrock.
- For each request record how many peers answered, the latency, and whether the recomputed
  receipts root equals the header's `receiptsRoot`, for every receipt type, deposits included.
- Record what the handshake needs from us (genesis hash, fork id, protocol version) and whether
  the protocol version in use lets a node advertise the block range it holds.
- Record whether peers keep serving a node that only asks and never serves.

The result decides how far back backfill over p2p is realistic, how many peers we need, and how
soon serving (crate 6) has to come. If peers are not reachable or will not serve us, the
fallback is our own execution, as three crates (`state`, `execution`, `state-sync`); see the
fallback rows in the decisions above.

Until 4 exists, blocks reach storage without receipts. Until 5 exists, nothing is marked
committed, so ClickHouse stays empty and Redis relies on its TTL and retention.

## Known hard problems (not solved, recorded so they are not forgotten)

- **Peer availability for receipts.** The whole receipts plan depends on execution peers
  serving us. Unknown until the viability test.
- **Serving is an attack surface.** Requests from untrusted peers need limits on rate, size
  and bandwidth from the first version.
- **A second p2p stack.** devp2p is not libp2p: its own transport, handshake and discovery.
- **Deposits are not in L1 batches.** Confirming them against L1 needs L1 logs.
- **Trusting L1 gossip** needs light-client verification from a trusted checkpoint.
- **If we fall back to execution:** state bootstrap, state size (not measured), verification
  without a state trie, and upkeep for every hardfork.
