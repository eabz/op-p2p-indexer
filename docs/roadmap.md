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
| 2026-10-03 | **Receipts and logs come from L2 execution peers, verified against the header.** The indexer asks execution nodes for a block's receipts over the execution p2p network (devp2p `GetReceipts`), recomputes the receipts root, and accepts them only if it equals the `receiptsRoot` in the sequencer-signed header. | Receipts and logs are not in gossip and not on L1. Fetching and verifying them needs no state and no execution, and peers are the same kind of dependency as gossip. A peer cannot forge them. |
| 2026-10-03 | **Be a good peer.** On gossip the node already validates and forwards blocks. On the execution network it will serve headers, bodies and receipts for the range it holds, with limits. It cannot serve state. | Peers tend to deprioritise or drop nodes that only download. |
| 2026-10-03 | **A local block archive for serving, on fjall**. Committed blocks are also appended, in their consensus encoding, to an embedded store. Serving reads Redis for unsafe blocks and the archive for committed ones; ClickHouse is never used to serve peers. It is part of the `storage` crate, behind the `ArchiveStore` trait. The default keeps a 30-day window; keeping the whole history is an option. | Serving history from ClickHouse would cost several queries per block, with the load driven by strangers. The whole history is plausibly 300 to 500 GB today and grows by roughly 200 GB a year (a full OP Mainnet node is about 700 GB including state; our own measurement gives about 280 GB a year of headers and bodies at current block sizes). redb was built and measured first: it used 1.7 times the data on disk, returned no space when trimming without a slow full compaction, and its appends slowed as the file grew, so it is ruled out for that size. fjall is the only maintained, published, pure-Rust engine of the log-structured kind that suits append-and-trim data. NippyJar was evaluated and rejected: unpublished internal reth crate, receipts only in block order, two cargo-deny exceptions. |
| 2026-10-03 | **Trust model without execution.** Transactions are confirmed by L1 once their batch is derived. Receipts and logs are as trustworthy as the sequencer's signature over the header that commits to them. Independent proof would need execution, or output roots resolved on L1 after the challenge period. | Stated so users of the data know what each status means. The indexer will not serve balances, `eth_call` or traces: it has no state. |
| 2026-10-03 | *Fallback, not the plan:* receipts from our own execution, keeping an L2 state and replaying blocks. Only if execution peers stop serving us. | It works without any peer serving receipts, but needs the full state, an OP-correct EVM and upkeep for every hardfork. |
| 2026-10-03 | *Fallback only:* two states, a committed state (as of the last L1-committed block) and a live overlay of per-block changes for unsafe blocks. | Part of the execution fallback above; not needed if receipts come from peers. |
| 2026-10-03 | **L1 awareness comes from an L1 p2p layer**, not an L1 RPC. It supplies the safe and finalized heads. See the 2026-10-04 row on dispute games for how. | Same reason as the first row. |
| 2026-10-03 | **Storage is built first and is independent of all of the above.** It takes an already decoded block, with or without its receipts, as input (`DecodedBlock`). How the block is decoded and where its receipts come from is solved in later crates. | Nothing in storage depends on where the data comes from, so it does not have to wait. |
| 2026-10-03 | Redis holds unsafe (live) blocks and handles reorgs; ClickHouse holds L1-committed and backfilled blocks. | Live data is small and changes shape; committed data is large and append-mostly. |
| 2026-10-03 | The two stores are named **unsafe** and **committed**, not hot and cold (`UnsafeStore`, `CommittedStore`). | The split is whether L1 has committed a block, not how often it is read. |
| 2026-10-03 | **Users read through one query layer**, not from Redis or ClickHouse directly. It routes each request to the unsafe or the committed store and labels results unsafe, safe or finalized. | One API regardless of where a block currently lives; the Redis key layout and the ClickHouse schema stay internal and free to change. |
| 2026-10-03 | **Stored data is disposable until the first release.** Migrations may be edited in place and databases dropped and recreated. From the first release on, applied migrations are never edited and data is kept. | Nothing is deployed yet; getting the schema right is worth more than preserving empty tables. The first release is the threshold; it has not been defined yet. |
| 2026-10-03 | **One embedded engine: fjall.** The p2p node store (identity and known peers) moves from redb to fjall too, and redb leaves the workspace. Existing data directories are not migrated: a node gets a new identity and an empty peer list. | One storage dependency to understand and audit instead of two. Data is disposable until the first release. |
| 2026-10-03 | Redis, not DragonflyDB. | One block every 2 seconds needs no extra throughput, and fork choice relies on Lua scripts that build key names from a prefix, which Dragonfly may not allow by default (not tested). The standard protocol keeps a later switch cheap. |
| 2026-10-03 | One chain: OP Mainnet. No dual-stack listening. No backfill of missed gossip blocks over op-node's consensus-layer request-response protocol (op-node is retiring it); backfill is planned over the execution p2p network instead (crate 4). | Decided during the p2p PR. |
| 2026-10-03 | **Promote now, backfill later.** Blocks are promoted to the committed store without waiting for receipts, and a hole in the committed range (a block missed on gossip) does not stall promotion: it is counted, the archive restarts, and backfill fills it later. `p2p` hands the pipeline the decoded block, not raw SSZ. | Nothing has receipts or backfill until the `el` crate exists; waiting would mean nothing is ever committed or pruned. Decoding once keeps SSZ out of the pipeline. |
| 2026-10-04 | **The `el` crate is built before peer access is proven**, on reth's network crates pinned to a release tag (a git dependency and one `cargo deny` sources exception). Receipts for new blocks first, backfill after. | The one-hour test showed fetching and verifying receipts works, but only 1 of 28 peers gave a session. The crate was built while a longer run measured access (stopped after one hour, see [el-viability.md](el-viability.md)). No usable published crate exists for the encrypted transport. |
| 2026-10-04 | **History is imported once, by a separate binary, and then served to peers.** No execution peer we reached serves blocks below the Bedrock block (105,235,063), and op-reth no longer imports them. `op-indexer-import` downloads the whole chain, from block 0 to the L2 block of the newest dispute game on L1, from an external archive (Envio HyperSync). It rebuilds every header, transaction and receipt, checks each block hash and the transactions and receipts roots, links the range by parent hash, and checks the top block against the dispute game's output root. It then writes the block archive, and ClickHouse when asked. The running indexer never links or calls it. "No external services" therefore means none at runtime; one-time imports are allowed when everything is verified. | The legacy chain cannot be re-executed by a modern EVM, so the data has to be fetched, and importing the rest in the same pass is faster than syncing it from peers. Keeping the importer out of the indexer keeps its dependencies and its trust in an external service out of the long-running process. Serving the range afterwards lets other instances get it from peers. See [import.md](import.md). |
| 2026-10-04 | **The safe and finalized heads come from dispute games, not from deriving batches.** A beacon light client follows Ethereum from one trusted checkpoint and vouches for L1 execution block hashes. From those, the `l1` crate fetches L1 headers, bodies and receipts over devp2p, verifies them, and decodes the dispute games created for the chain. A game names an L2 block and its output root; when our block at that height gives the same root, it is safe, and finalized once the game's L1 block is finalized. There is no provisional head. | Deriving batches needs blobs, which L1 peers prune after about 18 days, and a full derivation pipeline. A game's output root commits to the block hash, so one comparison covers the whole chain below it. See [l1.md](l1.md). |
| 2026-10-04 | **The retry policy lives in `storage`** (`op_indexer_storage::retry`); `pipeline` and the importer both use it. | The importer writes to the stores without the pipeline and needs the same backoff. |

## Order of work

| # | Crate | Role | Spec | Status |
|---|---|---|---|---|
| 0 | `p2p`, `chainspec`, `primitives` | L2 gossip: discovery, validation, unsafe blocks on a channel | crate docs | merged |
| 1 | `storage` | Unsafe store on Redis with fork choice, committed store on ClickHouse, local block archive, migrations, connectors | [storage.md](storage.md) | merged |
| 2 | `pipeline` | Decode gossip payloads into blocks, write them to the unsafe store, promote to the committed store when L1 commits, append committed blocks to the archive and trim it to its retention. Owns the retry policy. | [pipeline.md](pipeline.md) | merged |
| 7 | `query` | The read path: block by number or hash, transaction by hash, logs by filter, the heads. Routes by the safe head (above it the unsafe store, at or below it the committed store), reads the unsafe store first across the promotion boundary, and labels every result unsafe, safe or finalized. Needs read methods on `CommittedStore` that do not exist yet. A transport (JSON-RPC or REST) sits on top of it. | not written | |
| 4 | `el` (execution p2p) | Connect to L2 execution peers over devp2p. Fetch receipts for each block and verify them against the header's receipts root, then attach them (`UnsafeStore::set_receipts`). Fetch headers and bodies by number to backfill blocks missed on gossip, verified by the hash chain. | [el.md](el.md) | built; receipts at the tip run live, range sync never run |
| 5 | `l1` | L1 p2p: a beacon light client and L1 execution peers, giving the dispute games created for the chain and from them the safe and finalized heads. Still open: making the unsafe store reconcile itself when the safe head contradicts its canonical chain (storage.md 3.2, known gap). | [l1.md](l1.md) | built, off by default; light client run live, the whole chain never run end to end |
| 6 | `el`, serving | Give back to the execution network: answer header, body and receipt requests for the range we hold, and advertise that range honestly. Unsafe blocks come from the unsafe store, committed blocks from the local archive (storage.md section 9), within its retention window; nothing older is served. Only verified data is served, re-encoded and checked against the hash or root first. Request and bandwidth limits, so serving never starves ingestion. | [el.md](el.md) section 11 | built, never run live |

`op-indexer-import` ([import.md](import.md)) sits outside this order: a separate binary that fills
the archive, and optionally ClickHouse, before the node starts. The viability tests behind crates
4 and 5 are in [el-viability.md](el-viability.md) and [l1-viability.md](l1-viability.md). If
execution peers stop serving us, the fallback is our own execution, as three crates (`state`,
`execution`, `state-sync`); see the fallback rows in the decisions above.

## Known hard problems (not solved, recorded so they are not forgotten)

- **Peer availability for receipts.** The whole receipts plan depends on execution peers
  serving us. Measured: few peers give a session, and none we reached serve pre-Bedrock blocks
  ([el-viability.md](el-viability.md)). L1 execution slots are scarcer still.
- **Serving is an attack surface.** Requests from untrusted peers need limits on rate, size
  and bandwidth from the first version.
- **A second p2p stack.** devp2p is not libp2p: its own transport, handshake and discovery.
- **Deposits are not in L1 batches.** Confirming them against L1 needs L1 logs.
- **The light client can go quiet.** It never trusts a wrong block, but after more than one
  sync-committee period without peers, or an Ethereum fork this build does not know, it stops
  advancing without a warning ([l1.md](l1.md)).
- **If we fall back to execution:** state bootstrap, state size (not measured), verification
  without a state trie, and upkeep for every hardfork.
