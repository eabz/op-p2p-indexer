# Being a good peer

What this covers: every duty a well-behaved node has on each network it joins (consensus and
execution layers of the OP Stack chain, Base's differences, Ethereum L1, other op-p2p-indexers),
where it comes from, and whether it is met; how each is configured is in the crate specs
([el.md](el.md), [l1.md](l1.md)). Status: per row, below.

The node is self-hosted and should leave every network it joins more stable and better
connected: easier to find peers through, and with more data available. This file lists every
duty a well-behaved node has on each network, the specification it comes from, and whether we
meet it. Keep it current: a row changes when the code does.

Status: **yes** (built and run against real peers), **built** (compiled, not yet run against real
peers), **no** (not built), **n/a** (does not apply to a node without state or a mempool, with the
reason).

## Seeing the duties done

Every node (`indexer` and `server`) logs, once a minute, what it gave each network since the
last line: counts kept as they happen (plain counters in the swarm task, atomics elsewhere; no
lock on a hot path), so a duty marked built can be watched being met. A `server` also sends the
main totals in its heartbeat, and the balancer shows them on each server's `server status`
line ([serving.md](serving.md) §6.1).

| Counter | Line | Duty it shows (row below) |
|---|---|---|
| `blocks_accepted`, `blocks_forwarded`, with `mesh` | `consensus peers` | Validate before relaying, relay what is accepted: a block is forwarded when gossipsub still holds it at acceptance, to the mesh peers that do not have it (how many is gossipsub's, not counted) |
| `payloads_served`, `payloads_refused` | `consensus peers` | Serve `payload_by_number`: answered with a block, or without (throttled, not held, outside the range, the peer left) |
| `inbound`, `new_inbound`, `outbound` | `consensus peers` | Be dialable: other nodes reach this one |
| `discv5_contactable`, `discv5_inbound_per_s`, `discv5_sessions` | `consensus peers` | Advertise a public address / be findable: discv5's own view, shared by every discv5 node of the process (consensus, execution, L1), so not per network |
| `headers`, `bodies`, `receipts`, `items`, `bytes`, `peers_served` | `serving execution peers` (span `el{network=op}`, the chain's) | Serve headers, bodies and receipts of the canonical chain; serve the recent chain |
| `empty` | `serving execution peers` | Serve every block or announce the range: asked for blocks not held, answered empty |
| `refused` | `serving execution peers` | Accept inbound sessions and serve them, within the per-peer limits; on L1 (span `el{network=l1}`), answer every request with empty results |

## OP Stack consensus layer (libp2p: discv5, gossipsub, req/resp)

Spec: [Rollup node P2P](https://specs.optimism.io/protocol/rollup-node-p2p.html) (sections below
are its headings), and op-node's implementation where the spec is silent or differs
(`op-node/p2p/sync.go`).

| Duty | Spec section | Status | Notes |
|---|---|---|---|
| A secp256k1 network identity, separate from any consensus identity, kept across restarts | "Identification" | yes | `p2p/store.rs`; the same key for discv5 and libp2p. |
| Node record with `tcp`, `udp` and the `opstack` entry (`uvarint(chain id) ++ uvarint(fork 0)`, an RLP byte string) | "Discv5", "Consensus Layer Structure" | yes | `p2p/discovery.rs`. Without an advertised address the IP is learned from PONG votes. `seq` starts at the Unix time in seconds (geth uses milliseconds), so it is above any earlier run's and peers replace a stale record. |
| Advertise a public address when behind NAT | "NAT" | built | `OP_INDEXER_P2P_ADVERTISED_ADDR`: the record carries that IP and port and is not rewritten from votes. Without it, the IP is learned from votes as before. |
| Discovery pipeline: random lookups, filter records by chain id and current or future fork, do not redial recently disconnected peers | "Consensus Layer Structure", steps 1–5 | yes | Every fork version is current or future of our 0, so the chain id decides. Redials back off 2 min (10 min after eviction). |
| Be dialable on IPv4 and IPv6 | "Dialing" | no | One listen socket, one family. A dual-stack listener is not built. |
| TCP transport, Noise XX, multistream-select 1.0 | "Transport", "Transport security", "Protocol negotiation" | yes | libp2p. |
| Multiplexing: mplex required, yamux recommended | "Multiplexing" | no (deliberate) | yamux only. libp2p 0.57 removed mplex and asks users to migrate to yamux; op-node, op-reth and kona all offer yamux, so no peer is lost. Recorded as a deviation. |
| Ping enabled | "Ping" | yes | `libp2p::ping` in the behaviour. |
| Identify | "Identify" (optional) | yes | `libp2p::identify`, protocol `ipfs/0.1.0`, agent `op-indexer/<version>`. The spec allows leaving it out, but op-node (go-libp2p) opens its gossip stream only once identify says we speak gossipsub: without it peers connected, never subscribed, and were evicted. |
| GossipSub 1.1 with `StrictNoSign`, snappy block compression, 10 MiB limit, the message id over the decompressed data | "GossipSub", "Content-based message identification", "Message compression and limits", "Message ID computation" | yes | `p2p/gossip.rs`. A message that is not valid snappy is rejected by gossipsub's transform, so the invalid-snappy id is never needed. |
| Gossip parameters D 8, D_low 6, D_high 12, D_lazy 6, heartbeat 0.5 s, fanout_ttl 24 s, mcache 12/3, seen_ttl 130 heartbeats | "Heartbeat and parameters" | yes | `p2p/gossip.rs`. |
| Topics `/optimism/<chain>/0..3/blocks` | "Topic configuration", "blocksv1" to "blocksv4" | yes | All four, as op-node. |
| Validate before relaying, report ACCEPT / IGNORE / REJECT, relay what is accepted | "Topic validation", "Block validation" | yes | Manual validation in gossipsub; accepted blocks are relayed. |
| Every block-validation rule, in its class | "Block validation" | built | One deviation: a block outside the time window (60 s old, 5 s ahead) that the sequencer signed is IGNOREd, not REJECTed as the spec and op-node do, when it is dated ahead of us or at most 120 s old; an older one is a replay and REJECTed (without checking its signature), but never a reason to ban. It points at our clock, and unlike op-node we score invalid deliveries per topic, so rejecting it would graylist every honest peer while our clock is off. A second deviation: a block the sequencer signed that this build cannot read is IGNOREd, not REJECTed: the sequencer does not sign invalid blocks, so the protocol changed under this build and the peer is not at fault; three distinct such blocks within ten minutes stop the node, asking for an upgrade. Only what the sequencer alone can produce counts: a signed payload that decodes as no known payload version; one on the topic the current time requires that breaks a fork rule or does not rebuild to its hash; a transaction type this build does not know. A signed payload sent on another version's topic, which decodes as its own version (a replay: the signature covers the bytes, not the topic), is REJECTed as the peer's fault. Compression, encoding, timestamp window (60 s / 5 s), block hash, the per-version field rules (types enforce most), more than 5 at a height, already seen (IGNORE), signature. Added: from V2 an empty withdrawals list, from V3 no excess blob gas, and before Jovian no blob gas used. From Jovian, blob gas used is the DA footprint (`jovian/exec-engine.md`), so that rule stops at Jovian. |
| Sequencer signature over `keccak256(domain ++ chain_id ++ payload_hash)`, y-parity 0 or 1 | "Block signatures" | yes | `p2p/block.rs`. |
| Peer scoring | "Block topic scoring parameters" (TODO in the spec) | yes | op-node's light parameters. |
| Tide-based peer count, grace period, prune by score at high tide | "Peer management" | built (partly) | Discovery searches harder below 8 peers; a hard cap (`OP_INDEXER_P2P_MAX_PEERS`, at most half inbound) instead of score-based pruning; 30 s to subscribe. |
| Persist peer records | "Peer management" | built (partly) | Up to 64 peers that delivered valid blocks, without a TTL; one is forgotten after 3 failed dials in a row, across restarts. |
| Ban peers whose score is too low | "Peer management" | built (deviation) | A peer below op-node's ban threshold (-100, `p2p.ban.threshold`) is banned for an hour (`allow_block_list`), which takes repeated REJECTs; below the graylist threshold (-40) gossipsub already ignores it, and its messages stop being validated, so the ban is reached only by REJECTs delivered before graylisting, and rarely. Never for a block outside the time window, which may be our clock's fault. **Deviation:** bans are kept in memory, not persisted to a data store as the spec states; a restart clears them. |
| Serve `payload_by_number`, so older nodes can fill gaps from us | "Req-Resp", "payload_by_number"; [req/resp CL sync deprecation](https://docs.optimism.io/notices/archive/req-resp-cl-sync-deprecation) | built | `p2p/sync.rs`; the node answers and never asks. Protocol id as op-node registers it (`…/payload_by_number/<chain>/0`, no trailing `/`). Version 0 for V1/V2 payloads, 1 (envelope) from Ecotone, SSZ in snappy frames, 10 MB limit. Results 1 (not held), 2 (before Bedrock or past the expected tip), 3. op-node's rate limits, with `governor`: 20/s overall (burst 40), 4/s per peer (burst 15), a request waiting up to 20 s, then result 3; at most 512 waiting, idle peers' limits forgotten past 1,000. Blocks come from the archive, else the unsafe store. Limits: the peer's own rate first, then the global one; at most 4 waiting requests per peer (512 in all), 32 answers read, encoded or being written at once (a task that panics is counted out of its peer's requests), a 10 s write deadline (op-node's). |
| Never ask peers by `payload_by_number` | req/resp CL sync deprecation | yes | No client. |

## OP Stack execution layer (devp2p: discv4, discv5, RLPx, eth)

Specs: [devp2p](https://github.com/ethereum/devp2p) (discv4, discv5, RLPx, eth/68, eth/69 —
[EIP-7642](https://eips.ethereum.org/EIPS/eip-7642)), EIP-2124 (fork id), and op-node's reliance on
execution-layer sync to fill unsafe gaps
([deprecation of req/resp CL sync](https://docs.optimism.io/notices/archive/req-resp-cl-sync-deprecation)).

| Duty | Spec section | Status | Notes |
|---|---|---|---|
| Publish a signed node record with the external endpoint; omit IP and UDP port when the endpoint is unknown; bump `seq` on change | discv5 theory, "Maintaining The Local Node Record"; ENR, "Record Structure" (`seq`) | built | `el/discovery.rs`. With `OP_INDEXER_EL_ADVERTISED_ADDR` the record carries that address and discv5 never rewrites it; without it the record carries only the ports and discv5 fills in the IP from the endpoint mirrored in PONGs (and withdraws it when nothing reaches the node). Both paths never mix. `seq` starts at the Unix time in seconds, so it is above any earlier run's. |
| Carry the fork id in the record and keep it current across forks | EIP-2124, Specification (`forkid` = `[FORK_HASH, FORK_NEXT]`); op-reth's `opel` and geth's `eth` keys | built | Our record carries both `opel` and `eth`; `refresh_fork_id` rewrites them when a time fork activates while running. |
| Dial only peers of our chain and fork, validated per the EIP-2124 rules, not naively compared | EIP-2124, "Validation rules" | built | Discovery filters records by fork hash; the eth status is validated with the fork filter (reth's `ForkFilter`) in the handshake. |
| Routing table, liveness checks, FINDNODE only with live nodes, replacement cache | discv5 theory, "Node Table", "Table Maintenance In Practice" | built | Done by the `discv5` crate (sigp); we only run lookups and read `table_entries_enr`. |
| Find peers through discv4 as well (many execution nodes still use it) | devp2p discv4 | no (deliberate) | Not built on this branch. reth's `reth-discv4` at our tag (v2.7.0, also reth's newest tag; `main` is the same on 2026-10-04) depends on discv5 from git (sigp rev 7663c00) and `enr` 0.13, while we use discv5 0.12 and `enr` 0.14 from crates.io: two copies of each in the binary and a second git source. Discovery over discv5 alone reaches OP execution peers: our live runs found and completed sessions with op-reth, reth and a Geth build through it alone, all on eth/69 (el-viability.md §2, "Peer supply"). To revisit when reth moves to crates.io discv5. |
| RLPx: send Hello (or Disconnect) after the handshake; ignore unknown Hello fields and higher p2p versions | RLPx, "p2p Capability", "Hello (0x00)" | built | reth's `UnauthedP2PStream`. |
| Answer Ping with Pong | RLPx, "Ping (0x02)", "Pong (0x03)" | built | reth's `P2PStream`; pongs are flushed every 5 s at the latest. |
| Reject messages larger than the limit before decoding them (16 MiB RLPx, 10 MiB eth in practice), disconnecting the peer | RLPx, "Message Encoding"; eth, "Basic Operation" | built | After reth's `P2PStream` has read and decompressed the message (up to 16 MiB), before RLP: over 10 MiB the peer is disconnected with "breach of protocol". |
| Give a Disconnect reason, and the right one: too many peers when full, already connected, client quitting on shutdown, disconnect requested for a session we release | RLPx, "Disconnect (0x01)" | built | Close after the message is flushed, up to 2 s. |
| Negotiate the highest shared eth version; speak eth/69 and eth/68 | RLPx, "Message ID-based Multiplexing" (highest shared version wins); eth change log | yes | Hello offers eth/69, and eth/68 on the networks we serve (the OP Stack chain's), so peers that do not speak eth/69 can sync from us. eth/68 peers are served, never asked: their receipts come in the other format and they announce no range. |
| Send the Status for the negotiated version: eth/69 with the available range, eth/68 with total difficulty and head hash | eth, "Status (0x00)"; EIP-7642, "Status message changes" | yes | reth's `UnifiedStatus`. eth/68's total difficulty is sent as 0: no spec gives it a meaning after the merge (EIP-7642, "Rationale"), and reth checks only its size. |
| Serve headers, bodies and receipts of the canonical chain, as a run, within soft limits (2 MiB) | eth, "GetBlockHeaders (0x03)" to "Receipts (0x10)" | yes | 1,024 items or about 2 MiB per answer; 1,200 requests a minute and 4 at once per peer, 16 answered at once over all peers (64 waiting), and on a `server` an R2 budget for peers of 16 reads at once and 4 GiB a minute. A request beyond these is answered empty, never left unanswered ([el.md](el.md#11-serving)). |
| Serve receipts in the negotiated format: with bloom (eth/68), without (eth/69) | EIP-7642, "Receipts message changes" | yes | Held with their blooms; dropped for eth/69 sessions. |
| Serve the recent chain (the unsafe tip), since op-node relies on execution-layer sync to fill unsafe gaps | op-node req/resp CL sync deprecation notice | built | The advertised range runs to the newest canonical block whose receipts are held; the provider reads the archive, then the unsafe store, linked by parent hash (`crates/node/src/provider.rs`). A block promoted to the archive before its receipts arrived can briefly answer receipts requests short; the archive lists it (`pending_receipts`) and the pipeline fetches its receipts from peers within minutes. |
| Advertise only what is served: status range and `BlockRangeUpdate` mean bodies and receipts are available | eth, "BlockRangeUpdate (0x11)" | partly | Held blocks: built. The range is read again whenever the head moves, and `latest` stops below an archived block still waiting for its receipts (the provider caps it). Not met: with nothing held (or, for a non-indexer, nothing held from Bedrock on) the tip alone is advertised, a block not held. Genesis, as a fresh geth advertises, was not used: peers ended every session whose status had genesis as the head in earlier live runs, which would leave the node without receipts. |
| Send `BlockRangeUpdate` about every two minutes, at most once per 32 blocks | eth, "BlockRangeUpdate (0x11)"; EIP-7642, "BlockRangeUpdate message" | built | 2 minutes per peer, eth/69 sessions only. |
| Disconnect a peer whose range update has `earliest > latest` | eth, "BlockRangeUpdate (0x11)" | built | "Breach of protocol". |
| Serve every block from genesis, or announce the range served; headers "are always assumed to be available for the full range of blocks from genesis" | eth, "Chain Synchronization"; "BlockRangeUpdate (0x11)" | built (partly) | We announce `earliest`; headers below the archive's first block are not held. A node filled by the importer holds the whole chain. |
| Answer GetPooledTransactions even without a pool | eth, "GetPooledTransactions (0x09)" ("may respond with an empty list iff none of the hashes match") | built | Always an empty `PooledTransactions`. |
| Tolerate transaction announcements and transactions without disconnecting | eth, "Transaction Exchange"; "Transactions (0x02)" ("must not disconnect peers for sending invalid transactions") | built | Ignored: no pool. We announce nothing. |
| Discard NewBlock and NewBlockHashes; never send them | eth, "NewBlockHashes (0x01)", "NewBlock (0x07)" (EIP-3675) | built | Discarded. We do not disconnect peers that keep sending them (the spec says should); OP peers do not send them. |
| Ignore unknown message IDs | RLPx, "Message ID-based Multiplexing" | built | Ignored. |
| Verify what peers send before using it | eth, "Block Encoding and Validity", "Receipt Encoding and Validity" | built | Header hash chain to a trusted anchor, transactions root, receipts root by era. |
| Accept inbound sessions up to the cap and serve them | — (network health) | yes | `OP_INDEXER_EL_MAX_SESSIONS` each way (4 on an `indexer`; a `server` sizes it from its cores, 8 to 64); a full node refuses with "too many peers". |
| Do not hoard full nodes' slots; back off from full peers; pace requests | — (network health) | yes | Policy in [el.md](el.md) §6: unused outbound sessions released after 10 minutes (2 kept); a full peer retried after 60 to 90 s, doubling to 8 to 12 minutes, other failed dials after 5 minutes doubling to 40 to 60; 30 dials a minute; 200 ms between our requests per peer. |

## Base (chain 8453): where it differs

Base left the OP Stack for its own client (`docs/base.md`). Every row above applies to it as
written, except these.

| Duty | Spec section | Status | Notes |
|---|---|---|---|
| Discover execution peers on the chain's own discovery network: discv5 with protocol id `basev0`, apart from the consensus nodes, seeded with Base's execution bootnodes | Base `node.rs` (`BASE_V0_PROTOCOL_VERSION`); `docs/base.md` §3 | built | `el/discovery.rs` sets discv5's protocol identity from `ChainSpec::execution_discovery_id` (version left at the default, as Base does) and uses `ChainSpec::execution_bootnodes` (5 enodes, discv5 on port 9200). Our record keeps `opel`, `eth` and `opidx`: Base reads `opel`, and ignores keys it does not know (ENR, "Record Structure"). |
| Announce the fork id Base peers announce | EIP-2124 | built | `68647e86`, next 0, computed from the chain spec's forks (Azul, Beryl, Cobalt and no Karst); equal to Base's `eth_config` on 2026-10-04. Not yet confirmed by a peer. |
| Find execution peers through discv4 (Base still runs it) | devp2p discv4 | no (deliberate) | As for the OP Stack: discv5 alone. |
| Serve `payload_by_number` | "Req-Resp" | built (unused by Base) | Base's consensus client neither serves nor asks it; we keep serving it for op-node peers and never depend on it. Gaps in the unsafe chain fill through execution range sync. |
| Gossip on every block topic | "Gossip topics" | built | Base carries blocks only on v3 (`/optimism/8453/3/blocks`) and retires older topics; we subscribe to all and stay correct. |

## Unknown hardforks (every chain)

| Duty | Spec section | Status | Notes |
|---|---|---|---|
| Notice a fork this build does not know before it activates, and tell the operator to upgrade | EIP-2124 (`FORK_NEXT`) | built | The known-forks horizon (`el/horizon.rs`, `docs/el.md` §5), information only: it takes three distinct hosts (IPv4 addresses or IPv6 /48s, each counting for one time) announcing the same unknown `next` in their eth status, and setting it only produces a warning. It never refuses blocks or stops the node. |

## Ethereum L1 (beacon light client over libp2p; L1 execution over devp2p)

Specs: [consensus-specs light client networking](https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/p2p-interface.md)
(the light-client sections below are its headings), phase0
[p2p interface](https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/p2p-interface.md),
devp2p as above.

| Duty | Spec section | Status | Notes |
|---|---|---|---|
| Subscribe to `light_client_finality_update` and `light_client_optimistic_update` and validate every message | Light client networking, "Light clients" | yes | `l1/beacon/client.rs`; run against mainnet. |
| Forward only valid updates that advance the local store: REJECT invalid, IGNORE stale | "light_client_finality_update", "light_client_optimistic_update" | yes | A gossip message is held until verified; accepted only if it moves the finalized header (finality topic) or the head (optimistic topic). |
| Forward only after the sync messages of the signature slot had time to spread (`get_sync_message_due_ms()`, 3333 bps of a slot, less `MAXIMUM_GOSSIP_CLOCK_DISPARITY`) | same, second IGNORE rule | built | `BeaconSpec::is_due`: IGNORE before 3.499 s into the signature slot. |
| Process an update even when it is not forwarded | same ("SHOULD call `process_light_client_*_update` even if the message is ignored") | yes | Every message is verified and applied. |
| Report genesis in `Status` when holding only limited data | "Light clients" ("SHOULD use data based on `genesis_block` and `GENESIS_SLOT`") | built | The status is the genesis one throughout: zero finalized root, epoch 0, the genesis block root, slot 0. Before, it followed the store, which told full nodes we held blocks we cannot serve. |
| Serve light-client data (bootstrap, updates by range, finality and optimistic updates) | "Light clients" ("MAY also collect historic light client data and make it available") | n/a | Optional for a light client; we keep no history. The protocols are offered outbound only, so peers do not ask us for them. |
| Serve beacon blocks, blobs, columns; join subnets | phase0 p2p interface, Req/Resp domain | n/a | A light client holds none. The metadata says no subnets and the Fulu minimum custody (4), the value peers accept. |
| Answer `Status`, `Ping`, `GetMetaData`; read `Goodbye` | phase0 p2p interface, "Status", "Ping", "GetMetaData", "Goodbye" | built | `l1/beacon/network`. |
| L1 execution: honest status, holding no L1 blocks | devp2p eth, "Status (0x00)" | yes | eth/69 only (no eth/68 on a network we do not serve). The status names the newest trusted block as head, a range of that one block, and the node serves no block. |
| L1 execution: answer every request, with empty results for what we do not hold | eth, "GetBlockHeaders (0x03)" to "GetReceipts (0x0f)"; "GetPooledTransactions (0x09)" | built | Empty answers, never left unanswered. |
| L1 execution: few sessions, paced requests, do not hold full nodes' slots idly | — (network health) | yes | At most 4 sessions; requests 200 ms apart per peer, and unused sessions released, as on the OP Stack execution layer (above). |

## Among op-p2p-indexers

Pre-Bedrock blocks are shared only between nodes that carry the indexer entry in their node
record ([decisions](decisions.md), 2026-10-04).

| Duty | Status | Notes |
|---|---|---|
| Mark ourselves as an indexer: an `opidx` entry (version byte 1) in the OP Stack execution node record | built | `el/discovery.rs`; on every OP Stack chain (on Unichain too, where nothing is held back); not on Ethereum's network. |
| Recognise indexers from their record (discovery) | built | Learned again each run: the flag is not saved. A session knows whether its peer is an indexer, for both directions. An indexer met only inbound, whose record discovery has not seen, counts as an ordinary peer until discovery finds it. |
| Serve blocks before Bedrock to indexers only; answer others "not held" for them, without penalty | built | `ReadLimits.lowest` in every read; Unichain has no such blocks. |
| Advertise to non-indexers only from Bedrock on (status and `BlockRangeUpdate`); the full held range to indexers | built | `SessionServing::range`. |
| Fetch blocks before Bedrock only from indexers, and wait for one rather than ask others | built | Range sync's `Syncer::serves`. |
| Find each other: keep one outbound slot and four inbound slots, beyond the ordinary ones, for indexers (and inbound for nodes syncing far behind); beyond them indexers compete as ordinary peers, and only the outbound slot's session is exempt from the idle release | built | Inbound, an indexer is known by its node record or by its `op-indexer/` client name, so one that only dials in still gets a slot; when the four are taken, an inbound peer that has asked for nothing for 2 minutes makes room. Servers keep two ordinary slots per core each way (8 to 64) and accept their own deployment's servers outside every limit (`OP_INDEXER_EL_TRUSTED_PEERS`). `peers.rs`, `peers/schedule.rs`. The outbound slot goes only to an indexer that advertises blocks before Bedrock, and is dialed only for one discovery saw in this run; an indexer that answers "not held" three times in a row for them is dropped so another can take it. |
| Run between indexers | partly | An `indexer` and our `server`s have held sessions with each other on Unichain, the servers serving history (`op-indexer/` client name, history slots). Sharing blocks before Bedrock between two indexers has not run. |
