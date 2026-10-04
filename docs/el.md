# Execution p2p spec (`crates/el`)

Status: **agreed 2026-10-04, being built** on `feat/el`. The user decided to build while the
six-hour viability run is still going; peer access is the open risk (section 1).

The `el` crate connects to OP Mainnet execution peers over devp2p and fetches what gossip does
not carry: receipts for every block, and headers and bodies for blocks missed on gossip. All of
it is verified against data we already trust before it is handed on. It is a second p2p stack
next to `p2p` (libp2p); the two never depend on each other.

This PR is the fetching side only. Serving peers (roadmap row 6) comes after it.

## 1. What the viability test has established so far

From Worker 3's probe (a throwaway program outside the repo): a one-hour run on 2026-10-04
with the current fork id. A six-hour run follows; its numbers replace this section.

- **The protocol works.** Handshake over eth/69, then headers (155–340 ms), bodies
  (160–220 ms) and receipts (175–870 ms) from an op-reth peer at the tip. 1,024 headers per
  request in 0.5–1.1 s, parent links intact.
- **Verification works.** Transactions roots matched for every sample. Receipts roots matched
  for blocks from the tip back to one year old (legacy, EIP-1559, EIP-7702 and deposit
  receipts), and, against op-geth in earlier runs, back to Bedrock.
- **Peers are scarce and full.** 28 nodes with OP Mainnet's current fork id in an hour, all
  op-reth, arriving about one every two minutes. 26 of 34 dial attempts were dropped during
  the encrypted handshake with no reason given; every reason that was given was "too many
  peers". Three completed the handshake, all at the tip; **one served us**.
- **Receipt depth is not advertised.** The serving peer held headers and bodies back to block
  105,000,000 but returned empty answers for receipts older than its window.
- **Anchoring old headers** by hash chain from a gossip-verified block runs at roughly
  1,000–2,000 headers per second per peer: minutes for a week, hours for a year.
- **Not measured yet:** whether retrying wins a slot, how long a session lasts, and whether
  peers drop a node that only asks.

Rules observed, to cite and verify when the spec is finalised:

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
  blocks; not yet known whether the probe's decoding or erigon is at fault.

## 2. Inputs and outputs

| Direction | What | Type (in `primitives`) | From / to |
|---|---|---|---|
| in | a block that needs receipts | `ReceiptsRequest { block: BlockRef, receipts_root, timestamp_secs, transaction_count }` on an `mpsc` channel | the pipeline, as each block is ingested, and at startup for stored blocks without receipts |
| out | verified receipts for a block | `VerifiedReceipts { block: BlockRef, receipts: Vec<OpReceiptEnvelope> }` on an `mpsc` channel | the pipeline, which attaches them in the stores |

`el` depends on `primitives` and `chainspec` only: like `p2p`, it talks to the binary through
channels, and never depends on `p2p`, `storage` or `pipeline`.

A request that cannot be served (no peer, or no peer has the receipts) stays queued and is
tried again; the queue is bounded and drops its oldest entries first, counted. Backfill of
headers and bodies for holes uses the same stack and comes in a later PR.

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

## 6. Being a polite peer before serving exists

Until serving is built, the node only asks. It answers requests with empty responses,
advertises honestly, and keeps its request rate low. Whether peers tolerate this over days is
part of the viability result.

## 7. Not in this PR

Backfill of headers and bodies for holes; serving headers, bodies and receipts (roadmap
row 6); snap sync; transaction gossip; state.

Known limit: receipts that arrive after their block was promoted are attached in the archive
but not in the committed store, which has no call for it yet; they are counted. Nothing is
promoted until the `l1` crate exists, and the committed-store call comes with it.

Known limit: at startup the pipeline reads up to 1024 stored blocks in full to learn which
lack receipts, because the unsafe store has no lighter call. It runs in its own task and does
not delay ingest; a small storage call (parent hash plus has-receipts) would remove it.

## 8. Decisions

1. **Wire code: reth's crates, pinned to a release tag** (`reth-ecies`, `reth-eth-wire`,
   `reth-eth-wire-types`, `reth-network-peers`), as a git dependency with one `cargo deny`
   sources exception. No usable published alternative exists, and hand-writing the encrypted
   transport (about 2,500–3,000 lines) goes against preferring maintained crates. OP's
   receipts are decoded on the raw stream, since reth's typed stream is for Ethereum types.
2. **Receipts for new blocks first**; backfill follows on the same stack.
3. **Built before peer access is proven.** If the six-hour run shows sessions cannot be held,
   the options are a second test from a public server, a configurable list of trusted peers,
   or the roadmap's execution fallback.

## 9. Modules and ownership

| File (`crates/el/src`) | Holds | Owner |
|---|---|---|
| `lib.rs`, `config.rs`, `error.rs` | `ExecutionNetwork::new(...)` / `run(cancel)`, plain-data config, the crate's error | Worker 3 |
| `discovery.rs` | discv5 in the global DHT, filtered by the current fork id (`opel` and `eth` keys) | Worker 3 |
| `session.rs` | One RLPx session, dialled or accepted: ECIES, hello, eth/69 status (eth/69 only: a peer that speaks only eth/68 is refused at hello), ping/pong, disconnect reasons; requests as async calls; answers peers' requests with empty responses | Worker 3 |
| `wire.rs` | The message types used and OP's eth/69 receipt decoding, including the bloom rebuilt from the logs | Worker 3 |
| `peers.rs` | The peer set: who to dial, polite retry and backoff, how many sessions to keep (inbound ones are handed over by `session.rs` and kept or refused here), dropping peers that fail verification | Worker 1 |
| `fetch.rs` | The request queue: newest blocks first, one peer per request, timeout, another peer on failure | Worker 1 |
| `verify.rs` | The receipt count and the receipts root against the header, with the per-fork rules. The root check is what proves the logs; nothing it does not cover is trusted | Worker 1 |
| `metrics.rs` | Names and recording functions, like the other crates | Worker 1 |

Elsewhere: `chainspec` gets the genesis hash, the fork activations and the fork id (Worker 3);
`primitives` gets the two channel types; the pipeline gets a receipts task and sends requests;
the binary gets configuration and wiring (all Worker 2). Worker 3 owns the root manifest and
`deny.toml` for this PR.

## 10. Configuration and identity

- `OP_INDEXER_EL_ENABLED` (default `false` while peer access is unproven),
  `OP_INDEXER_EL_LISTEN_ADDR` (default `0.0.0.0:30303`, TCP and UDP),
  `OP_INDEXER_EL_BOOTNODES`, `OP_INDEXER_EL_MAX_SESSIONS` (default 8, for each direction).
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

## 11. Serving (built, not yet run live)

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

- The server reads the held range every 10 s. The status advertises `earliest` = the first
  block held (block 0 once the legacy range is imported) and `latest` = the tip the node
  knows, with its hash, exactly as before serving existed; a node holding nothing advertises
  its tip alone. **The range is honest at both ends, and complete once the import has reached
  the tip.** Until then blocks in the middle are not held, and requests for them get empty
  answers, like every other node on this network; the same holds for the few newest blocks,
  which are not yet committed. `latest` is the tip and not the last block held because peers
  keep a session whose status carries the real tip and end one that does not look like a live
  node (seen live). `BlockRangeUpdate` follows the same rule, once a minute per session.
- Metrics: `op_indexer_el_served_requests_total{kind,outcome}`,
  `op_indexer_el_served_items_total{kind}`, `op_indexer_el_served_bytes_total{kind}`.
- Not shown live: serving itself (built without a live run).

Owner: Worker 1 (`serve.rs`, the provider trait, the hooks in the session driver). The
binary's provider over `ArchiveStore`: Worker 2.

## 12. Range sync (being built)

The node fetches a range of blocks from peers and verifies it, so that a node without
history can get it from one that has it.

- Input: a target range and a trusted anchor (a block hash at the top of the range: the
  Bedrock block's parent for the legacy range, or a gossip-verified block).
- Headers are fetched in pages and verified by the hash chain down from the anchor; then
  bodies against each header's transactions root and receipts against its receipts root, with
  the rules of the block's era: plain legacy receipts before Bedrock, the deposit nonce left
  out of the hash before Canyon, the consensus encoding after.
- Verified blocks go to the pipeline in ascending order, in batches, as `DecodedBlock`s; the
  pipeline writes them to the committed store and appends them to the archive
  (`ArchiveStore::append_batch`). Senders are recovered as for gossip blocks; a zero-signature
  legacy transaction gets the zero address.
- Progress is stored, so a sync resumes where it stopped. A peer that returns data failing
  verification is dropped and banned, as for tip receipts.
- It shares sessions with the tip fetcher and never starves it: tip requests go first.
- Off by default: with `OP_INDEXER_EL_SYNC_FROM`, `OP_INDEXER_EL_SYNC_TO` and
  `OP_INDEXER_EL_SYNC_ANCHOR` unset there is no sync task at all.

**State (2026-10-04): parked.** Written and wired, never run, not reviewed. Before it is
turned on: split `sync.rs`, decide who appends to the archive when promotion and a range sync
both run (the archive is one contiguous range), run it against a node that serves.

Owner: Worker 2 (`sync.rs` in `el`, the pipeline's range input, configuration and wiring),
on Worker 3's session calls for headers and bodies.
