# L1 viability test

What a set of probes, and then the `l1` crate itself, measured on 2026-10-04 about following
Ethereum mainnet without an RPC: a beacon light client over libp2p for trusted L1 block
hashes, and L1 execution peers over devp2p for the blocks behind them. It is the evidence
behind [l1.md](l1.md), which stays the spec. The probes are not in the repository.

**Short answer.** It works. A light client built from published crates verifies a new L1 head
every slot and a new finalized L1 block every epoch, from a cold start in under 20 seconds,
and kept doing so for 30 minutes. From a hash it vouched for, L1 execution peers served the
headers, body and receipts that hold the user's dispute game, and the game decoded to the same
L2 block and output root the importer had read through HyperSync. Two limits: L1 execution
peers almost never have a free slot for a node that only dials out (one session per 4.5 to
15.5 minutes, both times Geth), and only recent Lighthouse versions serve light-client data.

## 1. Method

| | |
|---|---|
| Probes | Rust programs outside the repo. L1 execution: discv5 discovery, then per peer TCP, ECIES, p2p hello, eth status and requests, with the `el` crate's session code (reth v2.7.0's `reth-ecies`, `reth-eth-wire`, `reth-eth-wire-types`, `reth-network-peers`); blocks and receipts decoded with alloy. Beacon: discv5, then libp2p (TCP, noise, yamux, identify, request-response); the light-client containers parsed and verified by hand, BLS with `blst`. |
| Crate | `crates/l1` (`beacon/` for the light client), run from a scratch harness that prints each `TrustedL1Block` it emits. |
| Vantage | One home connection behind NAT, no port forward, dial-only. One L1 execution node and one beacon node at a time. |
| Politeness | A handful of blocks per peer, about a second between requests, empty answers to peers' execution requests, client strings saying the node only asks. |
| External services | None for chain data. No RPC, no HyperSync. |

Runs, all times UTC:

| Run | Time | Length | What |
|---|---|---|---|
| L1-1 | 06:12 | 4 min | L1 execution: handshakes, peer supply |
| L1-2 | 06:16 | 9 min | Same, 210 peers dialed once each |
| L1-3 | 06:26 | 19 min | Redial every 150 s, a real head in our status; samples tip to 30 days |
| L1-4 | 06:45 | 7 min | Walk down from a light-client-verified hash; find the user's game |
| B-1, B-2 | 06:28, 06:32 | 4 and 5 min | Beacon peers: records, clients, protocols they list (identify) |
| LC-1, LC-2 | 06:40, 06:44 | 2.5 min and 42 s | Light-client requests and full verification against peers |
| Crate-1 | 07:09 | 2.5 min | First `TrustedL1Block`s from crate code |
| Crate-2 | 07:38 | 32 min | The crate following the chain (after the network half's fixes) |
| Crate-3 | 08:18 | 3.5 min | The crate after `/simplify` |

Worker 1 also ran the crate's network half live (7 minutes and 200 s); its numbers are in
[l1.md](l1.md) §7.

Trusted values, and where each came from:

- **Mainnet genesis hash** `0xd4e5…8fa3`: recalled, then confirmed by every execution peer's
  status.
- **Execution fork schedule**: `alloy-hardforks` 0.4.9 `EthereumHardfork::mainnet()`. It
  gives fork id `07c9462e`, next 0: the commonest `eth` record entry (648 of 1,035 mainnet
  records in run L1-3) and what peers reported in their status.
- **Beacon genesis validators root, fork epochs and versions, blob schedule**: the
  consensus-specs mainnet configuration (`configs/mainnet.yaml`, read on its `master`
  branch). They give fork digest `8c9f62fe`, the `eth2` record entry of 132 of 174 beacon
  records seen in B-2.
- **The checkpoint** (the light client's one trusted input): not from an independent source.
  Each run took the finalized root that the first peers' finality updates reported (six
  peers agreed in LC-1). Everything after it is verified, but the root itself was trusted
  from peers. An operator would take it from a beacon node or checkpoint provider they trust.
- **Dispute game factory** `0xe596…2fA9`: superchain registry, as in the importer.

## 2. L1 execution peers

### Supply and slots

| | L1-1 (4 min) | L1-2 (9 min) | L1-3 (19 min) | L1-4 (7 min) |
|---|---|---|---|---|
| Node records seen | 1,208 | 2,787 | 4,668 | 2,140 |
| With mainnet's current fork id | 285 | 607 | 1,035 | 505 |
| Distinct peers dialed / dials | 20 / 20 | 210 / 210 | 400 / 804 | 320 / 320 |
| Refused "too many peers" at hello | 16 | 148 | 553 | 232 |
| Refused "too many peers" after status | 3 | 42 | 139 | 72 |
| Sessions that served | 0 | 0 | 1, after 15.5 min (Geth v1.16.7) | 1, after 4.5 min (Geth v1.17.5) |

- Discovery through the OP bootnodes finds mainnet execution nodes: the DHT is shared, and
  the `eth` record key carries their fork id. No mainnet-specific bootnode was needed.
- The handshake works unchanged: network id 1, eth/69, our fork id accepted. Clients that
  completed the status exchange: erigon (v3.3 to v3.7), Nethermind v2.0, besu v26.8, Geth and
  reth (v2.5.2, v2.7.0). Peers now advertise eth/66 to eth/71; we offered 68 and 69 and got
  69.
- Every refusal gave the same reason: too many peers. Redialing the same full peers every
  150 s (L1-3) did not change the odds. Two reth peers completed the handshake and dropped us
  within a second, for the same reason. Both sessions that served were Geth.
- Inbound was not tested: the node was not reachable.

### What a serving peer gives

From the two Geth sessions, for the tip and for blocks 1 hour, 1 day, 7 days and 30 days back:
header, body and receipts every time, each request about 0.1 to 0.15 s; transactions roots
and receipts roots all matched. 512 headers in one request took 0.24 to 0.32 s, parent links
intact. The peers asked us for nothing.

### The bloom filter

Mainnet headers' logs blooms are about three quarters full, so a bloom test passes a lot.
Over 718 consecutive headers (L1-4):

| Bloom contains | Headers |
|---|---|
| The factory's address | 327 (46%) |
| Address and the `DisputeGameCreated` topic | 138 (19%) |
| Address, topic and this game's address | 81 (11%) |

Of 19 candidates whose receipts were fetched, 18 were false positives. The bloom saves about
four fetches in five, no more: a watcher reads about one L1 block in five, roughly 50 an hour
with bodies of 50 to 530 KB. The crate therefore reads the body first and the receipts only
when a transaction in it was sent to the factory ([l1.md](l1.md) §6).

### The game, found without trusting a provider

Anchor: L1 execution block 26,117,277 (`0xe726…3731`), verified by the light client (sync
committee signature and execution-payload branch). From a Geth peer, 718 headers walked down
from that hash, each linked to its child by parent hash; candidates in the hour after the
game's L2 timestamp tested by receipts and body against the header's roots. The 19th was it:

| | |
|---|---|
| L1 block | 26,116,793 (`0xd2de…732d`), timestamp 1791090443 |
| Transaction | index 14, sent to the factory, `create` selector |
| Game | `0x3B7c…Fb85`, type 9 (super fault dispute game) |
| Root claim | `0xbfbc…4807`, equal in the event and the calldata |
| Extra data | 73 bytes, a super-root preimage hashing to the root claim |
| Super-root timestamp | 1791088823, so L2 block 157,745,023 |
| OP Mainnet's output root in it | `0xfe1e…4b85` |

The same L2 block and game the importer found through HyperSync's L1 endpoint
(`docs/import.md` §11), here with nothing trusted but the light client's hash.

## 3. Beacon peers and the light client

### Who is there and what they serve

B-2, 5 minutes of discovery: 444 records, 174 of beacon nodes, 132 on mainnet's current
digest (118 of those also advertise QUIC). 121 dialed over TCP: 73 connected, 50 answered
identify, 53 failed transport negotiation (not investigated; QUIC or another multiplexer).

| Client (identify) | Peers | Lists the light-client protocols | Serves light-client data |
|---|---|---|---|
| Lighthouse v8.2.0 to v8.2.3 | 30 | bootstrap, finality, optimistic, updates by range | yes, every time asked |
| Lighthouse v8.0.0, v8.0.1, v8.1.x | 15 | bootstrap, finality, optimistic | no: empty answers |
| Prysm v7.1, v7.2 | 3 | none | no |
| Crawler | 1 | none | no |

Listing a protocol is not serving it. Clients other than Lighthouse and Prysm did not
complete a TCP connection, so they are unmeasured.

### The full loop, verified

LC-2, four Lighthouse v8.2 peers, 42 s from a cold start: `Status` (0.14 to 0.5 s), finality
update (2.1 KB, 0.08 to 0.27 s), bootstrap for the finalized root (25.7 KB, 0.14 to 0.6 s),
optimistic update (1 KB, 0.07 to 0.2 s). Checked and passing every time: the finality branch,
the current sync-committee branch, the execution-payload branch, and the BLS aggregate
signature of the sync committee over the attested header, 9 to 19 ms including the key
decompression; `BLST_SUCCESS` on 8 of 8 updates.

### What peers do with a light client

- **They drop it fast.** In the first crate runs every Lighthouse peer closed a new
  connection 0.2 to 1 s after it opened. Two causes, found by Worker 1 from trace logs:
  - our `MetaData` v3 answer said custody group count 0; Lighthouse closed 0.2 s after it.
    With 4 (`CUSTODY_REQUIREMENT`, the Fulu minimum) that stopped;
  - full nodes prune a peer without subnets at their next heartbeat: `Goodbye` with reason
    129 after 2 to 35 s. Codes above 128 are client-specific; Lighthouse's 129 is "too many
    peers". This one is not fixable from our side.
- **A request has to leave at once.** Sent after waiting for the status answer, a request
  lost the race against the close. Sent with our status the moment identify arrives, it was
  answered. The network half now does that and rotates peers: 404 connections in the
  32-minute run, all goodbyes reason 129.
- **A genesis-like status is accepted** (zero finalized root, epoch 0, the genesis block as
  head) before the bootstrap.
- **Gossip delivers both updates every slot.** With gossip, the 32-minute run sent 4 requests
  in total. From Crate-2 on, a gossip message is forwarded to the mesh only after the light
  client verified it and found it newer than what it holds.

### The crate following the chain

| | Crate-1 (2.5 min) | Crate-2 (32 min) | Crate-3 (3.5 min) |
|---|---|---|---|
| Bootstrap verified after | 13.5 s | 18.9 s | 15.7 s |
| First head after | 101 s | 20.1 s | 17.6 s |
| Heads | 1 | 155, covering 156 consecutive L1 blocks, strictly increasing | 16, strictly increasing |
| Gap between heads | | median 12 s (one slot), max 36 s | |
| Finalized blocks | 1 | 6, one per epoch (6.4 min) | 1 |
| Requests sent | many (one-shot connections) | 4 | 4 |
| Data that failed verification | 0 | 0 | 0 |
| Updates ignored | | 3, signed by 203 of 512 members | 15 duplicates, refused before any BLS work |

Crate-1 predates the network half's fixes; the long gap was waiting for another usable
connection. Crate-3 ran after the light client's per-committee key cache: verifying an update
no longer decompresses 512 public keys (that was about 90% of its 10 to 20 ms).

## 4. Rules observed

Every citation below was read on the consensus-specs `master` branch.

| Rule | Detail | Source |
|---|---|---|
| Bootstrap | The header's root is the trusted checkpoint; the current sync committee is proven against its state root. | altair light-client `sync-protocol.md`, `initialize_light_client_store` |
| Update validation | now ≥ signature slot > attested slot ≥ finalized slot; signed by the committee of the store's period or the next; finality and next-committee branches against the attested state root; aggregate signature over the attested header under the domain of the slot before the signature slot. | same, `validate_light_client_update` |
| Applying | The finalized header advances with at least two thirds of the committee; the head is held to the same bar here (stricter than the specification's safety threshold). The next committee is learned from an update whose attested and finalized headers are in the store's period. | same, `process_light_client_update`, `apply_light_client_update` |
| Merkle branches | Generalized indices: finalized root 169, current sync committee 86, next sync committee 87 (Electra, `*_GINDEX_ELECTRA`); execution payload 25 (Capella). | electra and capella light-client `sync-protocol.md`, Constants |
| Header shape | From Capella a light-client header carries the execution payload header and its branch: this is what ties an L1 block hash and number to a verified beacon header. | capella light-client `sync-protocol.md`, Modified `LightClientHeader`, Modified `is_valid_light_client_header` |
| Fork digest | The first four bytes of `hash_tree_root(ForkData)`, and from Fulu XORed with SHA-256 of the blob-parameter entry in force (epoch, max blobs per block). Mainnet: Fulu `0x06000000` with the entry (419072, 21) gives `8c9f62fe`. | fulu `beacon-chain.md`, Modified `compute_fork_digest`; [EIP-7892](https://eips.ethereum.org/EIPS/eip-7892) |
| Record entry | `eth2` is the SSZ `ENRForkID`, an RLP byte string whose first four bytes are the digest. | phase0 `p2p-interface.md`, `eth2` field |
| Wire format | varint SSZ length, snappy frames; response chunks with a result byte, and for light-client data the fork digest as context. | phase0 `p2p-interface.md`, Encoding strategies; altair light-client `p2p-interface.md`, The Req/Resp domain |
| Updates by range | At most `MAX_REQUEST_LIGHT_CLIENT_UPDATES` (128) per request. | altair light-client `p2p-interface.md` |
| Custody | A node custodies at least `CUSTODY_REQUIREMENT` (4) groups; peers check the count in `MetaData` v3. | fulu `das-core.md`; fulu `p2p-interface.md`, GetMetaData v3 |
| Goodbye | Codes above 128 are client-specific. | phase0 `p2p-interface.md`, Goodbye v1 |

## 5. Cost

- No published crate covers light-client types or verification: `helios-ethereum` 0.1.0 is an
  empty placeholder; helios' consensus crates, `ethereum-consensus` and Lighthouse's types are
  not published.
- What the crate uses instead: `ethereum_ssz` + derive (already ours), `tree_hash` +
  `tree_hash_derive` 0.12 and `ssz_types` 0.14 (sigp, Apache-2.0), `blst` 0.3 (already in the
  lockfile through alloy's KZG), libp2p's `identify` and `request-response` features.
  Lockfile: +7 packages (`ethereum_hashing`, `futures-bounded`, `libp2p-identify`,
  `libp2p-request-response`, `ssz_types`, `tree_hash`, `tree_hash_derive`); no git source, no
  new licence; `cargo deny` passes.
- Hand-written: the containers' shapes, the proof and signature checks, the fork digest, the
  state machine. About 1,300 lines for the light client and 1,700 for its network half.

## 6. What stays untested

- **A sync-committee period boundary** (about every 27 hours): rotation to the next committee
  has only been reasoned through, never run.
- **The multi-period catch-up**: a checkpoint older than one period, applied through
  `LightClientUpdatesByRange`, one update per period.
- **A too-old checkpoint**: whether peers refuse its bootstrap clearly enough for
  `CheckpointUnavailable`, and how old a checkpoint Lighthouse still serves.
- **Clients other than Lighthouse**, and **QUIC**: half the beacon dials failed transport
  negotiation over TCP.
- **Inbound** on a public address, for both networks: peers dialing us may change the slot
  picture on L1.
- **A beacon or blob-parameter fork during a run**: the network half subscribes with the
  digest computed at start.
- **The whole chain end to end**: light client → L1 execution fetch from crate code →
  verified game → safe and finalized heads in the pipeline. The L1 execution half has only
  run in the probe; the step from a trusted hash to a game has never run from crate code.
