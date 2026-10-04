# Execution-peer viability test

What a throwaway probe measured on 2026-10-04 about fetching receipts, headers and bodies from
OP Mainnet execution peers over devp2p. It is the evidence behind [el.md](el.md) and the
roadmap's decision to take receipts from execution peers. The probe is not in the repository.

**Short answer.** It works for blocks from Bedrock on: peers at the tip serve receipts that
verify against the header in a few hundred milliseconds, and they keep serving a node that only
asks, for at least 46 minutes. Three limits: peers are few and mostly full, most keep receipts
for hours rather than years, and none of the peers reached serves anything before Bedrock.

## 1. Method

| | |
|---|---|
| Probe | A Rust program outside the repo: discv5 discovery, then per peer TCP, ECIES, p2p hello, eth status, requests. Transport and handshake from reth v2.7.0 (`reth-ecies`, `reth-eth-wire`, `reth-eth-wire-types`, `reth-network-peers`); blocks and receipts decoded with alloy and op-alloy. |
| Vantage | One home connection behind NAT, no port forward. One execution node at a time. |
| Politeness | A few sample blocks per peer, seconds between requests, empty answers to peers' requests, client string saying it only asks. |
| External services | None for chain data. One call to a what-is-my-IP service to advertise the right address. |

Runs, all times UTC:

| Run | Time | Length | Fork id used | Purpose |
|---|---|---|---|---|
| Trial | 00:09 | 4 min | stale (`d53e568f`) | Does it work at all |
| A | 00:15 | 29 min | stale | Peer supply (stopped when the stale fork id was found) |
| B | 00:18 | 25 min | stale | Seeded with op-geth's execution bootnodes; header ranges |
| C | 00:45 | 61 min | current (`c29239af`) | The hour run |
| D | 01:53 | 60 min | current | Retries, session life, tip workload, inbound (planned for 6 h, stopped at 1 h so the real crate could take over) |
| E | 03:02 | 31 min | current | Pre-Bedrock blocks |

Trusted values, and where each came from:

- **Recent block hashes**: our own indexer's gossip (sequencer-signed, validated by `p2p`),
  342 blocks spanning five hours, and a live feed during run D.
- **Genesis hash** `0x7ca38a19…a48b`: recalled, then confirmed by every peer's eth status.
- **Bedrock block 105,235,063** hash `0xdbf6a80f…afd3`: the publicly known value; every peer
  that served it agreed.
- **Fork activations**: `alloy-op-hardforks` 0.5.0 plus one learned from peers (section 5).
- **Everything older than our gossip**: not anchored. Old headers are what peers served,
  checked for internal consistency (header hash, transactions root, receipts root) and for
  agreement between peers. No hash chain was walked from a gossip block to an old one.

## 2. Peer supply

Execution nodes of every chain share one discv5 network. A node record carries an EIP-2124
fork id under `eth` (op-geth) or `opel` (op-reth), which is how peers of one chain and fork are
picked out.

| | Run C (61 min) | Run D (60 min) | Run E (31 min) |
|---|---|---|---|
| Node records seen | 11,821 | 11,465 | 7,848 |
| With OP Mainnet's current fork id | 28 | 25 | 24 |
| Via the `opel` key / the `eth` key | 28 / 0 | 25 / 0 | not split |
| Handshakes completed | 3 | 4 | 5 |
| Peers that then served us | 1 | 2 | 4 |

- New peers of our fork arrived at about one every two minutes; the curve had not flattened
  after an hour.
- Refusals in runs C and D together (about 62 dial attempts): 48 dropped during the encrypted
  handshake, which carries no reason; 5 "too many peers" at hello; 4 "too many peers" right
  after the status exchange; 1 TCP timeout. Every reason ever given was "too many peers".
- Every peer that completed a handshake on the current fork was at the tip (within one block
  of our gossip head).
- Clients that completed a handshake on the current fork: op-reth v2.4.2, v2.4.4 and v2.5.0,
  reth v2.3.0, an op-reth development build, two operators' custom client strings that behave
  like op-reth, and one `Geth/v0.1.0-untagged` build. All negotiated eth/69.
- Seeding discovery with op-geth's execution bootnodes (`V5OPBootnodes` in op-geth's
  `params/bootnodes.go`, 11 enodes) did not help: 6 of the 11 did not answer a record request
  and the yield was the same as with the consensus bootnodes we already use.
- **Inbound**: the probe listened and advertised itself during run D; no connection arrived.
  Behind NAT that proves nothing either way.

## 3. What peers serve

### Receipts at the tip (the product's workload)

Run D asked two peers every 30 seconds for the header and receipts of the newest block our
indexer had just verified on gossip.

| | |
|---|---|
| Requests / answered / receipts root equal to the gossip header's | 167 / 167 / 167 |
| Receipts latency | p50 227 ms, p95 508 ms, max 1,577 ms |
| Header-by-hash latency | p50 226 ms |
| Receipt types seen | legacy, EIP-1559, EIP-7702, deposit |

### Sessions

Two sessions (reth v2.3.0 and op-reth v2.5.0) were still open when run D was stopped, 46 and
42 minutes old. Neither peer asked us for anything, and neither dropped a node that only asks.
Beyond that hour nothing is known.

A session ends at once if our status advertises genesis as our head (seen from op-reth, reth
and the Geth build in the first run of the real crate): a node must know a tip before it
dials. An earlier 138-second session ended with the peer's "ping timeout" because the probe
did not flush its pongs; that was the probe's bug.

### How far back

| Block era | Headers and bodies | Receipts |
|---|---|---|
| Tip to 1 hour | every serving peer | every serving peer |
| 5 hours | every serving peer | some: op-reth v2.5.0 no, reth v2.3.0 yes |
| 1 week to 1 year | every serving peer | some: op-reth v2.4.4 yes up to a year; reth v2.3.0 and op-reth v2.5.0 no |
| After Bedrock (2023), Canyon era (2024) | every serving peer | two op-reth peers in run E, and op-geth on the stale fork; others empty |
| Before Bedrock | **none** (section 4) | none |

- The block range a peer advertises in its eth/69 status covers headers and bodies at best. It
  says nothing about receipts: receipt depth is only **bracketed** by these samples, peer by
  peer. The probe's bisection did not run (it misread one client's "not held" answer).
- "Not held" comes in two forms: op-reth v2.4 and v2.5 answer with an empty list, reth v2.3.0
  with a list holding one empty list.
- Header ranges: 1,024 headers per request in 0.4 to 1.1 seconds (about 640 KB), parent links
  intact, from four peers. Walking a hash chain back from a gossip-verified block therefore
  costs minutes for a week and hours for a year, per peer.
- Transactions roots matched for every body received, in every run.

## 4. Before Bedrock

Run E asked for blocks 1, 2, 1,000,000, 50,000,000, 100,000,000 and 105,235,062 (the last
legacy block), with 105,235,063 and 105,235,064 as controls.

| Peer | Advertised range | Legacy headers | Controls |
|---|---|---|---|
| op-reth v2.4.2 | 105,000,000 to tip | all six empty | served and verified; receipts of 105,235,064 pruned |
| op-reth development build | 0 to tip | all six empty | served and verified, receipts included |
| custom client string, op-reth behaviour | 0 to tip | all six empty | served and verified, receipts included |
| custom client string, op-reth behaviour | 0 to tip | all six empty | served and verified; receipts of 105,235,064 pruned |

- **Three of three peers advertising `earliest 0` serve nothing below 105,235,063.** The peer
  advertising 105,000,000 does not serve 105,235,062 either. The real floor is the Bedrock
  block.
- Pages of 1,024 headers ending at 105,235,062 and at 1,024 were empty on all four, as were 256
  headers from 50,000,000.
- **Block 0 is misleading**: every peer answers header-by-number 0, with a header whose hash is
  not the chain's genesis hash (two different hashes across the four), while sending the real
  genesis hash in its status. "Block 0 answered" must not be read as "legacy history present".
- Because bodies and receipts are requested by hash and no legacy header was obtained, legacy
  bodies, legacy receipts, transaction types, sender recovery, L1-to-L2 message signatures,
  sizes and bulk rates are **unanswered, not answered negatively**.
- This matches op-reth's documented design: op-reth v2.5.0 removed the
  legacy import commands (optimism PR #22942), the recommended setup is
  `init-state --without-ovm`, and pre-Bedrock history is served from a separate l2geth
  instance.
- **Untested**: an op-geth node with the migrated legacy database. None on the current fork
  gave a session.

## 5. Rules observed

| Rule | Detail | Source |
|---|---|---|
| Fork id | Genesis hash, block forks 3,950,000 and 105,235,063, time forks Canyon 1704992401, Ecotone 1710374401, Fjord 1720627201, Granite 1726070401, Holocene 1736445601, Isthmus 1746806401, Jovian 1764691201, and 1783526401 (2026-07-08). Result: hash `c29239af`, next 0. | [EIP-2124](https://eips.ethereum.org/EIPS/eip-2124); activations from `alloy-op-hardforks` 0.5.0 except the last |
| The last activation | Not in any published crate. Learned from node records of nodes that had not upgraded, which announce it as their next fork; adding it yields exactly the hash upgraded nodes report. Its name was not sourced. | observed |
| Stale fork id | With the list one fork short (`d53e568f`) the probe reached only nodes that had not upgraded, days behind the tip, and its own check refused every up-to-date peer. | observed, runs A and B |
| Protocol version | eth/69 on every current-fork peer. Its status carries the earliest and latest block. Up-to-date op-geth offered only eth/69 and snap/1. | [EIP-7642](https://eips.ethereum.org/EIPS/eip-7642) |
| eth/69 receipts | No bloom on the wire; each receipt is `[tx-type, status, cumulative-gas, logs]`, and a deposit receipt carries the deposit nonce and the deposit receipt version after the logs when it has them. op-alloy's `OpReceipt` decodes exactly this; the bloom is rebuilt from the logs. | EIP-7642; op-alloy-consensus 2.0 |
| Receipts root from Canyon | Each receipt hashed in its EIP-2718 consensus encoding, deposit nonce and version included. Matched as received for blocks from early 2024 to the tip. | [OP Stack deposits spec](https://specs.optimism.io/protocol/deposits.html), deposit receipt |
| Receipts root before Canyon | The deposit nonce is on the wire but not in the hashed receipt: the root matched only with it removed (blocks 110,000,000 and 105,235,064, from op-geth and two op-reth peers). | same; observed |
| Status | A status advertising genesis as the head gets the session ended at once. | observed, first run of the crate |

## 6. Cost of the wire code

- reth's network crates exist on crates.io only as 0.0.0 placeholders, so they are a git
  dependency pinned to a release tag. When it was added, that brought 66 packages into the
  lockfile, 12 of them from reth's repository, with no new licence and no advisory.
- No usable published alternative was found: `rlpx` and `devp2p` date from 2018, and
  `ethrex-p2p` depends on the rest of its client, RocksDB included.
- Writing the same parts ourselves would be roughly 2,500 to 3,000 lines of protocol and
  cryptographic code, estimated from reth's line counts.

## 7. What stays unknown

- **Sessions beyond about an hour**, and whether peers tolerate an ask-only node for days.
- **Inbound**: whether peers dial us and serve on those sessions. Needs a host reachable from
  outside.
- **Receipt depth per client**: bracketed only. Which peers keep receipts for a year or back to
  Bedrock, and how many of them there are, decides how far back backfill can go.
- **Pre-Bedrock**: whether any peer serves it (op-geth with the legacy database is the
  candidate), and everything about legacy bodies and receipts.
- **erigon on eth/68**: on the stale fork, one erigon peer returned receipts whose root did not
  match for three of five blocks, while its headers and bodies were fine. Not diagnosed. The
  crate speaks eth/69 only, so it does not meet this case.
- **Peer supply at saturation**: discovery was still finding new peers when each run ended.
- **Time of day and vantage**: one evening, one residential connection.
- **Anchoring of old headers**: measured as practical in speed, never actually done.
