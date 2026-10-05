# Execution p2p spec (`crates/el`)

What this covers: the execution-layer p2p stack (devp2p): discovery, sessions, receipts of new
blocks, missed blocks, range sync, serving our blocks to peers, and the policy that keeps it a
polite peer. Status: **built and run live**: receipts at the tip, range sync from block 0 with
the skeleton walk (Unichain), the fill of missed spans, and serving history to execution
peers from `server`s (section 1). On by default only with a profile, range sync or the L1 side
([configuration.md](configuration.md)).

The `el` crate connects to the configured chain's execution peers (OP Mainnet, Unichain or
Base, `op-indexer-chainspec`) over devp2p and fetches what gossip does not carry: receipts for
every block, and headers and bodies for blocks missed on gossip. All of
it is verified against data we already trust before it is handed on. It is a second p2p stack
next to `p2p` (libp2p); the two never depend on each other.

It fetches receipts at the tip (sections 2 to 10), serves its own blocks to peers (section 11)
and syncs a range of blocks from peers (section 12).

## 1. What has been measured

Before the crate existed, a probe (2026-10-04, a program outside the repo) established that
the protocol and the verification work against op-reth and reth peers, that peers are scarce
and full ("too many peers"), that sessions with a node that only asks last (46 minutes and
more), that receipt depth is not advertised, and that no public peer serves blocks before
Bedrock. That account is kept as a historical record in [el-viability.md](el-viability.md).

Since then, with the crate (live unless said otherwise):

- Receipts at the tip on OP Mainnet and Unichain, verified against the gossiped headers.
- Range sync: the skeleton walk at 6–7k headers/s on 3 sessions (OP Mainnet, from a laptop);
  blocks 0 to 400,000 of Unichain at 3,400 blocks/s from one peer (section 12).
- The fill of missed spans of the unsafe chain ([pipeline.md](pipeline.md), "Missed blocks").
- Serving: `server`s answered history requests of execution peers; fixed 2026-10-05 to read in
  runs and only the parts a request needs (section 11, [serving.md](serving.md)).
- Fork ids observed from peers: OP Mainnet and Unichain (section 5).

Rules observed:

- *Fork id* (EIP-2124): block forks 3,950,000 and 105,235,063; time forks Canyon 1704992401,
  Ecotone 1710374401, Fjord 1720627201, Granite 1726070401, Holocene 1736445601, Isthmus
  1746806401, Jovian 1764691201, and Karst at 1783526401 (2026-07-08), first learned from
  peers (the published `alloy-op-hardforks` 0.5.0 lacked it). With the stale fork id the node only peers with nodes that
  have not upgraded, which are days behind the tip.
- *Receipts root*: from Canyon on, each receipt is hashed in its EIP-2718 consensus encoding.
  Before Canyon the deposit nonce is on the wire but not in the hashed receipt.
- *eth/69 receipts* (EIP-7642): no bloom on the wire; it is rebuilt from the logs. Deposit
  receipts carry the deposit nonce and version after the logs when present.
- *Open:* an erigon peer on eth/68 returned receipts whose root did not match for 3 of 5
  blocks; not diagnosed. The crate asks only eth/69 peers for blocks (it serves eth/68 peers
  but never asks them), so it does not meet this case.

## 2. Inputs and outputs

| Direction | What | Type (in `primitives`) | From / to |
|---|---|---|---|
| in | a block that needs receipts | `ReceiptsRequest { block: BlockRef, receipts_root, timestamp_secs, transaction_count }` on an `mpsc` channel | the pipeline, as each block is ingested, and at startup for stored blocks without receipts |
| out | verified receipts for a block | `VerifiedReceipts { block: BlockRef, receipts: Vec<OpReceiptEnvelope> }` on an `mpsc` channel | the pipeline, which attaches them in the stores |
| in / out | a missed span of the unsafe chain, and its blocks | `FillRequest` in, `Vec<EncodedBlock>` out, on `mpsc` channels | the pipeline's fill task ([pipeline.md](pipeline.md), "Missed blocks"); fetched by `sync/fill.rs` in 64-block segments |
| out | range-sync batches and checkpoints | `Vec<EncodedBlock>`, `Vec<BlockRef>` | the pipeline's range task and the node store (section 12) |

`el` depends on `primitives` and `chainspec` only: like `p2p`, it talks to the binary through
channels, and never depends on `p2p`, `storage` or `pipeline`.

A request that cannot be served (no peer, or no peer has the receipts) stays queued and is
tried again; the queue is bounded and drops its oldest entries first. Headers and bodies of a
missed span or a range come from the same stack: the fill and range sync, section 12.

## 3. Parts

1. **Discovery**: discv5, filtered by the chain's fork id in the node record (`eth` and
   `opel` keys), seeded with the chain's execution bootnodes. The discv5 protocol id is the
   chain's (`ChainSpec::execution_discovery_id`): `discv5` on OP Mainnet and Unichain, whose
   execution nodes share the global DHT and the Superchain bootnode list; `basev0` on Base,
   whose execution nodes run a discv5 network of their own since Azul, apart from its
   consensus nodes, with their own bootnodes (a node on the default id cannot decrypt their
   packets). Ethereum's network (the L1 side) uses `discv5`.
2. **Session**: RLPx (ECIES handshake, framing), the p2p hello, the eth status exchange.
3. **Peer set**: a small number of peers kept connected, with redial and backoff, preferring
   peers at the tip; "too many peers" is retried politely, not hammered.
4. **Requests**: `GetReceipts`, `GetBlockHeaders`, `GetBlockBodies`, with timeouts, one peer at
   a time per request, a second peer on failure.
5. **Verification**: receipts against the header's receipts root (with the per-fork encoding
   rules); bodies against the transactions root; headers by hash, chained back from a block
   verified on gossip.
6. **Scheduling**: receipts for new blocks first, then holes. Low request rate.

## 4. Trust

Nothing from an execution peer is trusted. Receipts are accepted only if their root equals the
`receiptsRoot` of a header we already hold (sequencer-signed on gossip, or reached by the hash
chain from one). A peer that returns data that fails verification is dropped and remembered.

## 5. Fork activations

The fork id is derived from the chain's genesis hash, its block forks and the time forks
after its genesis time ([EIP-2124](https://eips.ethereum.org/EIPS/eip-2124): a fork at block 0
or at or before the genesis time is part of the genesis). The activations are configuration
this project keeps current (in `chainspec`), not something taken from a crate alone.

| Chain | Fork hash | Status |
|---|---|---|
| OP Mainnet (10) | `c29239af` | observed from peers (2026-10-04) |
| Base (8453) | `68647e86` | computed from the chain spec (genesis `0xf712…73dd`; Canyon through Jovian, Azul, Beryl, Cobalt); equal to what Base reports through `eth_config` (`docs/base.md` §3); not yet confirmed by a peer |
| Unichain (130) | `1faa456e` | computed (genesis `0x3425…befe`; Holocene, Isthmus, Jovian, Karst); observed from peers (Unichain range sync, 2026-10-05) |

**The known-forks horizon** (`horizon.rs`), information only. A peer whose fork id announces a
`next` time this build does not know is on a fork that is coming and that the node cannot
follow: it warns at once ("execution peers are on a hardfork this build does not know"), from a
node record or an eth status. Once three distinct hosts announce the same unknown time in a
completed eth status on our chain, that time is the horizon, and the node warns "execution
peers announce a hardfork at this time that this build does not know: upgrade before then".
- A host is an IPv4 address or an IPv6 /48 (a free tunnel hands out a /48), and counts for one
  time only, the last it announced. A node record does not count (anyone can write one), nor
  a time already past or more than a year ahead. Times that have passed are forgotten; at
  most 1,024 hosts are remembered.
- The horizon is recomputed from what hosts announce now: the earliest time three of them
  share, or none.
- It never refuses a block and never stops the node: a few hosts agreeing would otherwise be a
  lever on any node. What is stored is guarded by verification; a change of the block format
  is caught by the protocol-change stop on gossip (sequencer-signed blocks that do not decode).
- The peer set logs at startup that there is none (with the newest fork time this build knows), and,
  while one is set, the warning again every ten minutes.

## 6. Being a polite peer

The node joins networks of full nodes that ration their peer slots. It takes few slots, gives
them back when it does not use them, asks slowly, retries a full node less and less often,
advertises honestly (section 11) and answers what it holds. The numbers, for each `eth`
network it joins (the OP Stack chain's and, with the L1 side, Ethereum's):

| | Policy | Where |
|---|---|---|
| Sessions | 4 dialed and 4 accepted by an `indexer`, two per core (8 to 64) by a `server`, which exists to serve (`OP_INDEXER_EL_MAX_SESSIONS`; the L1 side uses 4); on the OP Stack network one more dialed, not counted against these, for an op-p2p-indexer with blocks before Bedrock (section 13) | `PeerConfig::max_sessions`, `sizing::server_el_sessions` (`crates/node`) |
| Peers that want history | up to 4 more accepted for a peer that wants history most peers prune: an op-p2p-indexer (by its node record, or its `op-indexer/` client name) or a node whose head is over 10,000 blocks behind ours (syncing). When those are full too, the inbound peer that has asked us for nothing the longest (2 minutes at least, and not one that wants history) is disconnected ("too many peers") to make room, so a full node's slots cannot lock them out | `peers.rs` `HISTORY_SLOTS`, `SYNCING_BEHIND`, `UNASKED_EVICTION` |
| Our own deployment | peers listed in `OP_INDEXER_EL_TRUSTED_PEERS` (the other servers): dialed first, always accepted, never released for being unused, and counted against no limit, so they neither take the slots kept for others nor are refused | `PeerConfig::trusted_peers` |
| Inbound connections | at most 8 handshakes at once, 2 from one host (an IPv4 address or an IPv6 /64), with 5 s each for the encrypted handshake, the hello and the status; further connections are closed and warned about once a minute, with a count; one established inbound session per host | `session/listener.rs`, `peers.rs` |
| Unused sessions | an outbound session we have not sent a request on for 10 minutes is closed ("disconnect requested"), keeping the 2 used most recently (the receipts of new blocks) and the session in the indexer slot; the outbound target then drops to 2, so no other peer is dialed in their place, and goes back up only after every kept session has been busy (used within a minute) for 5 minutes in a row; a released peer is not dialed again for 30 minutes; sessions peers opened are theirs and stay | `peers.rs` `IDLE_RELEASE`, `KEEP_IDLE`, `BUSY`, `BUSY_TICKS`, `RELEASED_REDIAL` |
| Dials | at most 8 at once, 30 a minute, no peer more often than once a minute | `peers.rs`, `peers/schedule.rs` |
| A full peer | a dial refused with "too many peers" (or a dropped handshake): again after 60 to 90 s, doubling with each refusal in a row, up to 8 to 12 minutes; a session the peer ended with "too many peers": again after 60 to 90 s | `FULL_PEER_RETRY` |
| Other failed dials | 5 to 7.5 minutes, doubling, up to 40 to 60 minutes; another fork or no shared protocol: 1 hour; bad data: banned 6 hours | `peers/schedule.rs` |
| Requests we send | per session and requester: the receipts of new blocks one at a time, range sync and the fill up to 4 at once; a requester starts a request on a session at most every 200 ms; a peer that leaves three requests in a row unanswered is dropped as unresponsive | `pacing.rs`, `sync/schedule.rs` |
| Requests we answer | 1,200 a minute and 4 at once per peer, 1,024 items or about 2 MiB per answer, 16 answered at once in all; a request beyond these is answered empty, never left unanswered | `serve.rs`, `serve/session.rs` |
| A server's R2 reads for peers | 16 at once and 4 GiB a minute over all peers; beyond them, an empty answer | `crates/server/src/budget.rs` |
| What we advertise | the blocks we hold and serve, or the tip alone; never blocks we would refuse (section 13) | `serve/session.rs` |

## 7. Not done

Snap sync, transaction gossip, state.

Receipts that arrive after their block was promoted are attached in the archive, the
committed store; the number of archived blocks still waiting for them is logged when it changes.

Known limit: at startup the pipeline reads up to 1024 stored blocks in full to learn which
lack receipts, because the unsafe store has no lighter call. It runs in its own task and does
not delay ingest; a small storage call (parent hash plus has-receipts) would remove it.

## 8. Decisions

1. **Wire code: reth's crates, pinned to a release tag** (`reth-ecies`, `reth-eth-wire`,
   `reth-eth-wire-types`, `reth-network-peers`), as a git dependency with one `cargo deny`
   sources exception. No usable published alternative exists, and hand-writing the encrypted
   transport (about 2,500–3,000 lines) goes against preferring maintained crates. OP's
   receipts are decoded on the raw stream, since reth's typed stream is for Ethereum types.
2. **Receipts for new blocks first**; range sync runs on the same sessions.
3. **Built before peer access is proven.** If sessions cannot be held in practice, the
   options are a test from a public server, a configurable list of trusted peers, or the
   roadmap's execution fallback.

## 9. Modules

| File (`crates/el/src`) | Holds |
|---|---|
| `lib.rs`, `config.rs`, `error.rs` | `ExecutionNetwork::new(...)` / `run(cancel)`, plain-data config, the crate's error |
| `network.rs` | `NetworkSpec` (network id, genesis, fork schedule, bootnodes, record keys) and `PeerNetwork`: discovery, sessions and the peer set for one devp2p network, reused by `l1` for Ethereum L1 |
| `discovery.rs` | discv5 on the chain's discovery network (`execution_discovery_id`), filtered by the current fork id (`opel` and `eth` keys); the advertised address, and the public IP learned |
| `horizon.rs` | The known-forks horizon (section 5) |
| `warn_limit.rs` | Warnings at most once per interval |
| `session.rs`, `session/{context,driver,handshake,listener}.rs` | One RLPx session, dialed or accepted: ECIES, hello, eth/69 or eth/68 status (the highest both speak; an eth/68 peer is served, never asked), ping/pong, disconnect reasons; requests as async calls; peers' requests passed to the server |
| `wire.rs` | The message types used and OP's eth/69 receipt decoding, including the bloom rebuilt from the logs |
| `peers.rs`, `peers/schedule.rs` | The peer set: who to dial and when, polite retry and backoff, how many sessions to keep in each direction, banning peers that fail verification |
| `pacing.rs` | Request pacing per session |
| `fetch.rs` | The receipt request queue: newest blocks first, one peer per request, timeout, another peer on failure |
| `verify.rs` | The receipt count and the receipts root against the header, with the per-fork rules |
| `serve.rs`, `serve/{provider,session}.rs` | Serving: the `BlockProvider` trait, the server task, per-session answering and limits (section 11) |
| `sync.rs`, `sync/{fill,headers,schedule,segment}.rs` | Range sync: the header walk, the per-segment fetch, scheduling across sessions (section 12); the fill of missed spans |

Elsewhere: `chainspec` holds the genesis hash, the fork activations and the fork id;
`primitives` the channel types; the pipeline has the receipts, fill and range tasks; `crates/node`
has the configuration, the provider over the archive and the unsafe store, the range sync
planner and the wiring; a `server`'s provider reads sealed history from R2 (`crates/server`).

## 10. Configuration and identity

- The variables (`OP_INDEXER_EL_*`: enabled, listen address, bootnodes, sessions, advertised
  address, trusted peers, range sync) and their defaults are in
  [configuration.md](configuration.md). Behaviour: with an advertised address set, the node
  record carries it for TCP and UDP (a server or a forwarded port). Unset, discovery fills in
  the address it learns from other nodes, and withdraws it again if nothing reaches the node
  within five minutes, so a node behind NAT without a port forward advertises no address; the
  last IP learned is kept for the server's own address. With `el` disabled the pipeline has no
  receipts, fill or range task.
- The execution network has its own secp256k1 key, kept in the node store next to the
  libp2p identity and supplied by the binary. It is not the same key: both networks run a
  discv5 node, and one node id announcing two different records would look like a node
  flapping between addresses.
- Up to 32 execution peers that served a verified answer are saved in the node store and
  dialed first on the next start, as soon as a tip is known.
- No peer is dialed, and no inbound session accepted, until the node knows a tip to put in
  its status: peers end a session at once with a node whose status says genesis.
- Shutdown: both networks stop first, then the pipeline.

## 11. Serving

The node answers peers from its own stores, so that another node can sync from it. Run live
from `server`s; an `indexer` serves the same way from its archive.

- `el` does not depend on `storage`. It defines the `BlockProvider` trait (`read` a run of
  headers, bodies or receipts within `ReadLimits`, and the held `range`), and the binary
  implements it (`NodeProvider`, `crates/node/src/provider.rs`):
  - Committed blocks come from the archive. The canonical unsafe blocks above its tip come
    from the unsafe store (in memory), because peers syncing the tip need those most: op-node
    relies on execution-layer sync to fill unsafe gaps.
  - **One chain per answer.** A run that crosses the archive's tip, or continues in the
    unsafe store, is checked by parent hash as it is read, and ends at the first block that
    does not link, so a reorg during a read never puts a block of the old branch after one
    of the new.
  - **Orphans and missing receipts are not held.** A block asked for by hash is served from
    the unsafe store only while it is canonical; its receipts only once they are attached.
  - **Steps.** Headers every `step > 1` blocks cannot be linked, so they come from the
    archive only.
  - **Bytes.** Unsafe blocks are encoded from the stored gossip block, which gives the
    original bytes (signed transactions survive decoding).
  - **Range.** `range()` is the archive's first block up to the end of the unbroken run of
    canonical unsafe blocks above its tip that have their receipts
    (`UnsafeStore::canonical_run`, continued from the last end found while it is still
    canonical, at most 16,384 new heights looked at). eth/69 promises bodies and receipts
    for every block of the advertised range, so a block without receipts ends it, even
    though its header and body are served. In the archive part, a block promoted before its
    receipts arrived is listed in the archive's `pending_receipts` until the pipeline fetches
    them (`docs/pipeline.md` section 4b): until then the advertised range ends below it (the
    lowest such block within the archive caps `latest`), and the node fills it within
    minutes, so the range only briefly shrinks.
  - **Cost.** The unsafe part of an answer is read from memory, the bytes held, without
    decoding (their roots were checked when they were stored). Bodies and receipts are read 16
    hashes at a time, stopping at the byte limit.
- `serve.rs`: one `Server` task reads from the provider. A session driver never waits for it:
  it hands a request over with `try_send` and writes the answer when it arrives on the
  session's own answer channel, so serving does not delay the tip fetcher. A peer that reads
  a large answer slowly holds up its own session only.
- **Bytes.** Headers and bodies go on the wire exactly as the archive holds them: the response
  is assembled around the stored RLP, which is copied in and never decoded. With
  `ArchiveStore::append_batch` storing the bytes the importer or range sync verified, what is
  served is what was verified. Receipts are held with their blooms (the form up to eth/68),
  and eth/68 sessions get them as held; eth/69 sends `[tx-type, status, cumulative-gas, logs]` plus the deposit nonce and version, so
  they are decoded and encoded again without the bloom. Only the bloom is dropped (the
  receiver rebuilds it from the logs); type, status or post-state root, gas, logs and the
  deposit fields are carried over for every receipt type.
- `GetBlockHeaders` (by number or hash, with count, skip and direction), `GetBlockBodies` and
  `GetReceipts` are answered up to the first block not held. `GetPooledTransactions` is always
  answered empty.
- Limits (named constants in `serve.rs`):

  | Limit | Value | Over it |
  |---|---|---|
  | Items per response | 1024 | the response ends |
  | Bytes per response (soft) | 2 MiB | the response ends after the item that crosses it |
  | Requests per peer per minute | 1,200 (a syncing op-p2p-indexer asks up to ~15 a second, and leaves a peer for a minute after an empty answer) | empty answer |
  | Requests of one peer being answered | 4 | empty answer |
  | Requests of all peers waiting | 64 | empty answer |
  | Requests read from the provider at once | 16 (a server's reads wait on R2) | the others wait in the queue |

- **What is advertised** (status and `BlockRangeUpdate`, at most once every two minutes per
  eth/69 session, as devp2p `caps/eth.md` recommends; the range is read again whenever the
  head moves):
  only blocks this node serves, or its tip alone. The server reads the held range every 10 s.
  - Blocks are held: the held range as it is, `earliest` = its first block (block 0 once the
    legacy range is imported), `latest` = its last block with its hash, however far that is
    behind the chain's tip. Every block advertised is served, and peers (and our own range
    sync) know to ask for them: making an imported history available is why it is held. The
    node then looks like one that is behind. Earlier runs suggest peers accept that (a stale
    but real head kept sessions; only genesis as the head ended them), but **a status hours or
    days behind is not confirmed live:** whether peers keep sessions with such a node, and
    still answer its requests for the tip's receipts, is unmeasured.
  - Nothing is held: the tip alone, `earliest` = `latest` = the tip.
  - The provider (the binary's) also serves the canonical unsafe blocks above the archive,
    linked to it by parent hash, so `latest` is the newest of those whose receipts are held:
    op-node relies on execution-layer sync to fill unsafe gaps, so the recent chain is what
    helps peers most.
  What is advertised is logged at info at startup and when its kind or its first block
  changes.
  The tip is an input: the binary provides the newest block the node knows (gossip, or the
  newest block held when there is no gossip), so a node that only serves still peers.
- A request body larger than a request for 1,024 hashes (about 34 KiB) is answered empty
  before it is copied or decoded.
- A session writes with a 10 s limit: a peer that asks and does not read is dropped and not
  dialed for an hour. What the peer sends is read last in the session's loop, so a peer
  flooding messages cannot keep its own answers or the flush from running.
- A failed provider read is warned about at most once a minute per kind (headers, bodies,
  receipts, the held range). After 8 failed reads of the held range in a row (about 80 s),
  requests are answered empty without reading and the advertised range stays where it was,
  until a read of the held range succeeds again. A failed request read does not count: one
  corrupt stored block that peers keep asking for fails their requests only.
- A `server`'s provider reads sealed history from R2 in runs (a downward header request read
  upwards as one stream, bodies and receipts by hash continuing from the block before, a
  skeleton page at most 64 headers), and only the parts of each block it sends, within its R2
  budget for peers ([serving.md](serving.md), section 5). Until 2026-10-05 it read a block at a
  time, so peers timed out on it ("it stopped answering").
- What was served is counted and logged once a minute ("serving execution peers"), and a
  `server` sends the totals in its heartbeat ([citizenship.md](citizenship.md)).

## 12. Range sync

The node fetches a range of blocks from peers and verifies it, so that a node without
history can get it from one that has it.

- Input: a target range and a trusted anchor (a block hash at the top of the range).
- Headers are walked down from the anchor and verified by the hash chain, in parallel: a
  *skeleton* (the hash of every 1,024th block, a thousand per `GetBlockHeaders` with a skip of
  1,023) is fetched first, one page at a time; the 1,024-block gaps below each skeleton hash
  are fetched on every session at once (up to 256 ahead), each as a hash chain from its claimed
  top, and linked from the top down: a gap counts only once its top is the hash the gap above
  names as parent, so the trust still comes from the anchor alone; a gap fetched from a wrong
  claim is fetched again from the trusted hash. The walk's checkpoints are saved as they are
  linked, so a restart continues it. Measured on OP Mainnet from a laptop: 720 headers/s one
  page at a time before, 6–7k/s on 3 sessions with the skeleton. Then, from the bottom up,
  bodies are
  checked against each header's transactions root and receipts against its receipts root, with
  the rules of the block's era: plain legacy receipts before Bedrock, the deposit nonce left
  out of the hash before Canyon, the consensus encoding after.
- Each session takes up to 4 requests of the sync at once (one start per 200 ms), the fastest
  peers (verified blocks per second, smoothed) first; up to 32 segments are fetched or waiting
  at once, within 256 MiB of verified blocks waiting to be handed on. A segment spans whole
  checkpoints (every 256th block) up to about 2 MiB of blocks of the size seen so far, from
  256 to 1,024 blocks: a request then carries as many light blocks as a peer answers at once
  (1,024 items), so the 200 ms between requests, not the size of an answer, bounds the sync of
  light blocks. Measured on Unichain blocks 0 to 400,000 from a laptop (2026-10-05): 3,400
  blocks/s from one peer with 1,024-block segments, against ~785/s on 3 sessions with 256;
  storing a batch of 1,024 took about 10 ms, so the pipeline never held the fetch up. The
  progress line shows the segment size, the jobs in flight, the segments waiting and the time
  spent waiting on the store.
- An empty receipts list for a block with transactions is "not held" (a node that pruned its
  receipts answers so), never bad data: until 2026-10-05 it was hashed and failed the
  receipts root, and banned honest reth peers (seen on Unichain at block 385). While a
  round runs the peer set dials for twice `OP_INDEXER_EL_MAX_SESSIONS` outbound sessions and
  releases none for being unused.
- Items in an answer are matched by what verifies: leading items that belong to their blocks
  are kept and the rest asked for again; an item of a later block means "not held from here";
  an item of no block asked for is bad data and bans the peer. The rejection is logged with what
  did not match for the first block asked (which root, computed and the header's, or the
  receipt count).
- Verified blocks go to the pipeline in ascending order, in batches, as the exact bytes
  received (`EncodedBlock`). The pipeline decodes them on blocking threads for the committed
  store, recovers senders in parallel, and appends the bytes to the archive
  (`ArchiveStore::append_batch`). A zero-signature legacy transaction gets the zero address.
- Progress: the archive's last block is where the fetch resumes, and the anchor of an
  unfinished sync with its walk's checkpoints is saved in the node store, so a restart
  continues the walk instead of starting it again.
- Range sync's requests run next to the tip fetcher's on the same sessions; neither waits for
  the other.
- It needs an archive that keeps every block and whose range the sync continues; otherwise
  the binary refuses to start it. A store that refuses a batch stops the process.
- On with `OP_INDEXER_EL_SYNC`, a profile that turns it on, or the L1 side
  ([configuration.md](configuration.md)); it then runs in rounds, for as long as the node runs.
  It only closes the gaps gossip cannot; otherwise promotion extends the archive from the
  unsafe store, and no block is fetched twice. Each round goes from the block after the
  archive's last one (block 0 on an empty archive) to an anchor whose hash is trusted; the
  saved anchor of an unfinished round is resumed first.
  - **With the L1 side**, a round is planned while the archive's last block is 1,024 blocks
    or more (`CAUGHT_UP_BLOCKS`, the unsafe store's read limit) below the safe head, or below
    the committed safe block, and it is anchored on the safe head only (a bonded claim on L1
    matched it, [l1.md](l1.md#3-trust); heads never move back). Everything the sync writes is
    then at or below a block L1 commits to.
  - **Without it**, a round is planned while the archive is that far below the gossiped head,
    anchored on the gossiped block 64 below the unsafe head (`ANCHOR_DEPTH`), or lower, at the
    newest block peers advertise, whose hash the sequencer signed, looked up in the unsafe
    store (right after a start the sync waits until gossip has delivered that many blocks, and
    until a peer advertises a height). An unsafe reorg deeper than 64 blocks would leave
    the archive on a dead branch: such an archive has to be rebuilt.
- **The chain must reach the archive.** The plan carries the archive's last block, and the
  parent the first fetched block names is checked against it as soon as the header walk
  reaches it (or, for a range shorter than one segment, from the first segment), before
  anything is handed on. A mismatch ends the round as `RoundEnd::NotLinked`.
- **An anchor no peer serves is given up**, as `RoundEnd::AnchorUnavailable`: once at least
  three peers, and every open session whose peer says it holds the anchor's height, have
  answered that they do not hold it and none has served it (a walk page or, for a short
  range, a segment from it). After a round is given up, for any reason, the next waits a
  minute, doubling for each round given up in a row, up to 30 minutes; the next anchor
  chosen replaces the saved one.
- **The archive is shared.** Promotion extends the archive from the unsafe store above its
  last block, and the binary holds an L1 head back from promotion while the archive is more
  than 1,024 blocks below it; the range task and promotion each leave out what the other
  already wrote ([pipeline.md](pipeline.md), sections 4 and 4b). A batch that does not extend
  the archive is left out with a warning; the round then never reaches its anchor, and the
  planner gives it up two minutes after it was fetched (`ROUND_STORE_TIMEOUT`). A node filled
  by the importer continues from the import's last block, with no range to configure. On an
  empty OP Mainnet archive the range begins before Bedrock, which only op-p2p-indexers serve
  (section 13): the header walk reaches as far down as peers hold and then waits, warning once
  a minute.
- Logs: every line of the execution network carries `el{network=op}` or `el{network=l1}`;
  the range sync's progress line says how many peers hold the next headers, and waits "for a
  peer that holds the anchor" when none does.

**State: run live** (Unichain from block 0, 2026-10-05; the numbers above). Known
inefficiencies: bodies and receipts of a segment are fetched one after the other, and headers
are downloaded twice (once by the walk, once per segment). Before Bedrock no public peer serves
blocks, so a sync of OP Mainnet from genesis only works against another op-p2p-indexer.

## 13. op-p2p-indexer peers

Blocks before Bedrock are the one thing no public peer serves and this node holds: they stay
among op-p2p-indexers, so ordinary nodes are not offered blocks they cannot use, and indexers
find each other without a protocol of their own.

- **The flag.** On the OP Stack network our node record carries `opidx` (version byte 1)
  next to the fork id. Discovery remembers which peers carry it; a session knows whether its
  peer is one from the record discovery saw in this run. The flag is not saved: a restart
  learns indexers from discovery again. At
  most 4,096 peers are remembered as indexers; when full, new ones are ignored and the known
  ones stay. Ethereum's
  network (the L1 side) has no indexers: no flag, nothing held back.
- **What is shared with whom.** Blocks below the chain's Bedrock block are served to indexer
  peers only: anyone else gets the empty answer for them, as for blocks not held, and is not
  penalised for asking. The range we advertise (status and `BlockRangeUpdate`) starts at the
  first block we hold for an indexer, and at the Bedrock block (or the first block we hold, if
  later) for anyone else. Range sync asks for blocks below Bedrock only of indexer peers, and
  waits for one; other peers are not asked for them. On a chain without a legacy chain
  (Unichain) the Bedrock block is 0 and none of this changes anything.
- **Finding each other.** Indexers are few: one outbound slot beyond
  `OP_INDEXER_EL_MAX_SESSIONS` is kept for an indexer that says it holds blocks before
  Bedrock (its advertised `earliest` is below Bedrock), and its session is not counted
  against the ordinary slots; inbound, indexers share the four history slots (section 6). Only an indexer discovery saw in this run is dialed for it.
  The session in the outbound slot is exempt from the release of unused sessions, and is
  left out of the "every kept session busy" check. Beyond the slot indexers compete for the
  ordinary slots like any peer. An indexer that answers "not held" three times in a row for
  blocks before Bedrock is dropped (`not_holding`) and not dialed for a long while, so
  another can take the slot; one that answers nothing useful otherwise is dropped like any
  useless peer. On a chain with nothing before Bedrock (Unichain) there is no outbound slot.
- **Spoofing is accepted.** The flag is self-declared: any node can carry `opidx`, take the
  indexer slot and be served blocks before Bedrock. That costs bandwidth only: those blocks
  are public history, and the slots are few.
- **Not done:** an indexer we only meet inbound and whose record discovery has not seen is not
  served blocks before Bedrock until discovery finds its record (its `op-indexer/` client
  name already gets it a history slot). Sharing blocks before Bedrock between two indexers
  has not run.
