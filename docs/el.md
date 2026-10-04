# Execution p2p spec (`crates/el`)

Status: **built**, off by default (`OP_INDEXER_EL_ENABLED`). Receipts at the tip have run
live; serving (section 11) and range sync (section 12) have not. Peer access is the open
risk (section 1).

The `el` crate connects to OP Mainnet execution peers over devp2p and fetches what gossip does
not carry: receipts for every block, and headers and bodies for blocks missed on gossip. All of
it is verified against data we already trust before it is handed on. It is a second p2p stack
next to `p2p` (libp2p); the two never depend on each other.

It fetches receipts at the tip (sections 2 to 10), serves its own blocks to peers (section 11)
and syncs a range of blocks from peers (section 12).

## 1. What the viability test established

From a viability probe (a program outside the repo), 2026-10-04; the full account, with
numbers and sources, is [el-viability.md](el-viability.md).

- **The protocol works.** Handshake over eth/69, then headers, bodies and receipts from
  op-reth and reth peers at the tip, a few hundred milliseconds each. 1,024 headers per
  request in 0.4 to 1.1 s, parent links intact.
- **Verification works.** Transactions roots matched for every body. Receipts roots matched
  for every receipt answer at the tip (167 of 167) and for samples back to Bedrock (legacy,
  EIP-1559, EIP-7702 and deposit receipts).
- **Peers are scarce and full.** About 25 nodes with OP Mainnet's current fork id in an hour,
  arriving about one every two minutes. Most dials end in the encrypted handshake with no
  reason given; every reason that was given was "too many peers".
- **Sessions last.** Two sessions with a node that only asks were still open after 46 and 42
  minutes; beyond an hour nothing is known.
- **Receipt depth is not advertised.** A peer's advertised range covers headers and bodies;
  many peers keep receipts for hours, a few for up to a year.
- **No peer serves blocks before Bedrock.**
- **Anchoring old headers** by hash chain from a gossip-verified block costs minutes for a
  week and hours for a year, per peer.

Rules observed:

- *Fork id* (EIP-2124): block forks 3,950,000 and 105,235,063; time forks Canyon 1704992401,
  Ecotone 1710374401, Fjord 1720627201, Granite 1726070401, Holocene 1736445601, Isthmus
  1746806401, Jovian 1764691201, and one more at 1783526401 (2026-07-08) that the published
  `alloy-op-hardforks` 0.5.0 lacks. With the stale fork id the node only peers with nodes that
  have not upgraded, which are days behind the tip.
- *Receipts root*: from Canyon on, each receipt is hashed in its EIP-2718 consensus encoding.
  Before Canyon the deposit nonce is on the wire but not in the hashed receipt.
- *eth/69 receipts* (EIP-7642): no bloom on the wire; it is rebuilt from the logs. Deposit
  receipts carry the deposit nonce and version after the logs when present.
- *Open:* an erigon peer on eth/68 returned receipts whose root did not match for 3 of 5
  blocks; not diagnosed. The crate speaks eth/69 only, so it does not meet this case.

## 2. Inputs and outputs

| Direction | What | Type (in `primitives`) | From / to |
|---|---|---|---|
| in | a block that needs receipts | `ReceiptsRequest { block: BlockRef, receipts_root, timestamp_secs, transaction_count }` on an `mpsc` channel | the pipeline, as each block is ingested, and at startup for stored blocks without receipts |
| out | verified receipts for a block | `VerifiedReceipts { block: BlockRef, receipts: Vec<OpReceiptEnvelope> }` on an `mpsc` channel | the pipeline, which attaches them in the stores |

`el` depends on `primitives` and `chainspec` only: like `p2p`, it talks to the binary through
channels, and never depends on `p2p`, `storage` or `pipeline`.

A request that cannot be served (no peer, or no peer has the receipts) stays queued and is
tried again; the queue is bounded and drops its oldest entries first, counted. Headers and
bodies of a range come from the same stack: range sync, section 12.

## 3. Parts

1. **Discovery**: discv5 in the global DHT, filtered by OP Mainnet's fork id in the node
   record (`eth` and `opel` keys), seeded with the execution bootnodes.
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

The fork id is derived from the list of OP Mainnet fork activations. That list is
configuration this project keeps current (in `chainspec`), not something taken from a crate
alone. The node should also notice when most peers reject its fork id or announce a `next`
fork it does not know, and say so loudly: that is the sign the build is behind.

## 6. Being a polite peer

The node asks at a low rate, advertises honestly (section 11) and answers peers' requests from
what it holds; with nothing held it answers them empty.

## 7. Not done

Snap sync, transaction gossip, state.

Known limit: receipts that arrive after their block was promoted are attached in the archive
but not in the committed store, which has no call for it yet; they are counted
(`op_indexer_pipeline_blocks_promoted_without_receipts_total`). Promotion runs when the `l1`
side is enabled, so this can happen now.

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
| `discovery.rs` | discv5 in the global DHT, filtered by the current fork id (`opel` and `eth` keys) |
| `session.rs`, `session/{context,driver,handshake,listener}.rs` | One RLPx session, dialed or accepted: ECIES, hello, eth/69 status (a peer that speaks only eth/68 is refused at hello), ping/pong, disconnect reasons; requests as async calls; peers' requests passed to the server |
| `wire.rs` | The message types used and OP's eth/69 receipt decoding, including the bloom rebuilt from the logs |
| `peers.rs`, `peers/schedule.rs` | The peer set: who to dial and when, polite retry and backoff, how many sessions to keep in each direction, banning peers that fail verification |
| `pacing.rs` | Request pacing per session |
| `fetch.rs` | The receipt request queue: newest blocks first, one peer per request, timeout, another peer on failure |
| `verify.rs` | The receipt count and the receipts root against the header, with the per-fork rules |
| `serve.rs`, `serve/{provider,session}.rs` | Serving: the `BlockProvider` trait, the server task, per-session answering and limits (section 11) |
| `sync.rs`, `sync/{headers,schedule,segment}.rs` | Range sync: the header walk, the per-segment fetch, scheduling across sessions (section 12) |
| `metrics.rs` | Names and recording functions, like the other crates |

Elsewhere: `chainspec` holds the genesis hash, the fork activations and the fork id;
`primitives` the channel types; the pipeline has a receipts task and sends requests; the
binary has the configuration, the provider over the archive and the wiring.

## 10. Configuration and identity

- `OP_INDEXER_EL_ENABLED` (default `false` while peer access is unproven),
  `OP_INDEXER_EL_LISTEN_ADDR` (default `0.0.0.0:30303`, TCP and UDP),
  `OP_INDEXER_EL_BOOTNODES`. The session limit is a constant (8 in each direction).
  `OP_INDEXER_EL_ADVERTISED_ADDR` (optional `ip:port`): the public address the node record
  carries for TCP and UDP, for a server or a forwarded port. Unset, discovery fills in the
  address it learns from other nodes, and withdraws it again if nothing reaches the node
  within five minutes, so a node behind NAT without a port forward advertises no address.
  With `el` disabled the pipeline has no receipts task and the binary behaves as before.
- The execution network has its own secp256k1 key, kept in the node store next to the
  libp2p identity and supplied by the binary. It is not the same key: both networks run a
  discv5 node, and one node id announcing two different records would look like a node
  flapping between addresses.
- Up to 32 execution peers that served a verified answer are saved in the node store and
  dialed first on the next start, as soon as a tip is known.
- No peer is dialed, and no inbound session accepted, until the node knows a tip to put in
  its status: peers end a session at once with a node whose status says genesis.
- Shutdown: both networks stop first, then the pipeline.

## 11. Serving (not run live)

The node answers peers from its own stores, so that another node can sync from it.

- `el` does not depend on `storage`. It defines the `BlockProvider` trait (`header`, `body`,
  `receipts` by number, `number_of` a hash, the held `range`) and the binary implements it
  over the local archive; `Option<P>` implements it too, `None` holding nothing.
- `serve.rs`: one `Server` task reads from the provider. A session driver never waits for it:
  it hands a request over with `try_send` and writes the answer when it arrives on the
  session's own answer channel, so serving does not delay the tip fetcher. A peer that reads
  a large answer slowly holds up its own session only.
- **Bytes.** Headers and bodies go on the wire exactly as the archive holds them: the response
  is assembled around the stored RLP, which is copied in and never decoded. With
  `ArchiveStore::append_batch` storing the bytes the importer or range sync verified, what is
  served is what was verified. Receipts are held with their blooms (the form up to eth/68);
  eth/69 sends `[tx-type, status, cumulative-gas, logs]` plus the deposit nonce and version, so
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
  | Requests per peer per minute | 120 | empty answer |
  | Requests of one peer being answered | 4 | empty answer |
  | Requests of all peers waiting | 64 | empty answer |
  | Requests read from the provider at once | 4 | the others wait in the queue |

- **What is advertised** (status and `BlockRangeUpdate`, at most once a minute per session):
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
  What is advertised is logged at info at startup and when its kind or its first block
  changes.
  The tip is an input: the binary provides the newest block the node knows (gossip, or the
  newest block held when there is no gossip), so a node that only serves still peers.
- A request body larger than a request for 1,024 hashes (about 34 KiB) is answered empty
  before it is copied or decoded.
- A session writes with a 10 s limit: a peer that asks and does not read is dropped and not
  dialed for an hour. What the peer sends is read last in the session's loop, so a peer
  flooding messages cannot keep its own answers or the flush from running.
- Metrics: `op_indexer_el_served_requests_total{kind,outcome}`,
  `op_indexer_el_served_items_total{kind}`, `op_indexer_el_served_bytes_total{kind}`.
- Serving itself has not run live.

## 12. Range sync

The node fetches a range of blocks from peers and verifies it, so that a node without
history can get it from one that has it.

- Input: a target range and a trusted anchor (a block hash at the top of the range).
- Headers are walked in pages down from the anchor and verified by the hash chain; the walk's
  checkpoints are saved, so a restart continues it. Then, from the bottom up, bodies are
  checked against each header's transactions root and receipts against its receipts root, with
  the rules of the block's era: plain legacy receipts before Bedrock, the deposit nonce left
  out of the hash before Canyon, the consensus encoding after.
- Items in an answer are matched by what verifies: leading items that belong to their blocks
  are kept and the rest asked for again; an item of a later block means "not held from here";
  an item of no block asked for is bad data and bans the peer.
- Verified blocks go to the pipeline in ascending order, in batches, as the exact bytes
  received (`EncodedBlock`). The pipeline decodes them on blocking threads for the committed
  store, recovers senders in parallel, and appends the bytes to the archive
  (`ArchiveStore::append_batch`). A zero-signature legacy transaction gets the zero address.
- Progress: the archive's last block is where the fetch resumes, and the anchor of an
  unfinished sync with its walk's checkpoints is saved in the node store, so a restart
  continues the walk instead of starting it again.
- One sync request per session, next to the tip fetcher's own; neither waits for the other.
- It needs an archive that keeps every block and whose range the sync continues; otherwise
  the binary refuses to start it. A store that refuses a batch stops the process.
- Off by default; `OP_INDEXER_EL_SYNC=true` runs it in rounds, for as long as the node runs.
  It only closes the gaps gossip cannot; otherwise promotion extends the archive from the
  unsafe store, and no block is fetched twice. Each round goes from the block after the
  archive's last one (block 0 on an empty archive) to an anchor whose hash is trusted; the
  saved anchor of an unfinished round is resumed first.
  - **With the L1 side**, a round is planned while the archive's last block is 1,024 blocks
    or more (`CAUGHT_UP_BLOCKS`, the unsafe store's read limit) below the safe head, or below
    the committed safe block, and it is anchored on the safe head only (its batch is on L1:
    no reorg replaces it). Everything the sync writes is then committed on L1.
  - **Without it**, a round is planned while the archive is that far below the gossiped head,
    anchored on the gossiped block 64 below the unsafe head (`ANCHOR_DEPTH`), whose hash the
    sequencer signed, looked up in the unsafe store (right after a start the sync waits until
    gossip has delivered that many blocks). An unsafe reorg deeper than 64 blocks would leave
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
- **Promotion extends the archive.** Promotion reads the range above the archive's last block
  when that block is above the committed safe head and below the new safe head, so a first
  safe head above the archive's last block promotes everything from it on, not the safe block
  alone, and blocks the archive holds are not read again (no hole is reported for them). The
  binary holds an L1 head back from promotion while the archive is more than 1,024 blocks
  below its safe block, or below the committed safe block; the finalized head is held with it
  (promotion takes the two together). The hold is looked at again every 2 s against the
  growing archive, so a head is released as soon as a round has stored enough, often with the
  round still storing: promotion and the range task then both append, and each leaves out
  what the other already wrote. A failing read of the archive or of the node store is
  retried, warned about once a minute; the head forwarder treats an unreadable archive as no
  reason to hold a head.
- **The range task and promotion share the archive.** For each batch the pipeline leaves out
  the blocks the archive already holds and requires the first block left to name the
  archive's last one as its parent, before it writes anything. If promotion appends between
  that check and the archive append, the append is checked once more against the new tip. A
  batch that does not extend the archive is left out with a warning and nothing written; the
  round then never reaches its anchor, and the planner gives it up two minutes after it was
  fetched (`ROUND_STORE_TIMEOUT`). The pipeline does not stop for it. A node filled by the importer therefore continues
  from the import's last block, with no range to configure. On an empty archive the range
  begins before Bedrock, which no public peer serves: the header walk reaches as far down as
  peers hold and then waits, warning once a minute.
- Logs: every line of the execution network carries `el{network=op}` or `el{network=l1}`;
  the range sync's progress line says how many peers hold the next headers, and waits "for a
  peer that holds the anchor" when none does.

**State: never run.** Nothing has executed it, because it needs execution sessions with a
peer that serves the range. Known inefficiencies: bodies and
receipts of a segment are fetched one after the other, and headers are downloaded twice (once
by the walk, once per segment). Before Bedrock no public peer serves blocks, so a sync from
genesis only works against another instance of this node.
