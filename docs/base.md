# Base (chain 8453)

What the code does for Base (chain 8453), and the investigation it rests on. Status
2026-10-05: the chain spec, fork list, execution discovery identity, game format and fork
horizon are built; Base's import is downloaded and being completed (2.57 TB of downloaded
chunks; [roadmap](roadmap.md) #1). No Base node has run yet.

## Spec

What the code does for Base, and where. The investigation below is the record behind it, with
every source.

- **Chain values** (`crates/chainspec`, `BASE`): chain id 8453, genesis `0xf712…73dd` at
  1686789347, Bedrock at block 0 (no legacy chain), 2 s blocks, unsafe signer
  `0xAf6E19BE0F9cE7f8afd49a1824851023A8249e8a`, factory `0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e`.
  Values from `base/base` `crates/common/chains/src/config.rs` (commit `615cf0f`), checked against
  the last registry `base.toml` and L1 (section 1).
- **Forks** (`ChainSpec::time_forks`, a per-chain list of `(Hardfork, time)`): Regolith (at
  genesis) through Jovian at the OP times, then Base's own Azul 1779991200, Beryl 1782410400,
  Cobalt 1790791200. No Karst, no Delta (derivation only), no Denim or Everest (unscheduled).
  The rules by fork read the list (`activation`, `canyon_time()`, `isthmus_time()`, ...); a fork
  a chain does not list is never active on it. The execution fork id comes out `68647e86`,
  what Base nodes report (`eth_config`, 2026-10-04); OP Mainnet's stays `c29239af` and
  Unichain's `1faa456e`.
- **Discovery**: consensus nodes are on the standard discv5 DHT, bootstrapped from the shared
  Superchain list (which holds Base's five consensus records). Execution nodes are on Base's own
  discv5 network, `ChainSpec::execution_discovery_id = *b"basev0"`, with Base's five execution
  bootnodes on their discv5 port 9200 (`execution_bootnodes`; the discv4 entries on 30301 are
  left out). OP Mainnet and Unichain keep one shared network (`discv5`, the same list for both).
- **L1 commitment** (`chainspec/game.rs`, a per-chain table game type → claim format): Base
  reads types 0 and 1 (its fault games before Azul, `create`) and 621, `AggregateVerifier`,
  created with `createWithInitData(uint32,bytes32,bytes,bytes)` (selector `0x1011f377`). Its
  extra data is packed `uint256 l2Block ‖ address parent ‖ bytes32 root × n`; the last root must
  be the root claim, which is the output root of `l2Block` by the usual formula. Checked on the
  live game `0x12d7…895f` (L1 block 26,122,197): block 52,181,760, claim `0x24e371…bbb9`,
  equal to the output root rebuilt from that block's header. A type a chain does not list is
  `UnknownGameType` and is not promoted.
- **Gaps**: Base's consensus client does not serve `payload_by_number`; this node never asks
  for it on any chain (it only serves it), and fills gaps from execution peers (fill and range
  sync, [el.md](el.md)).
- **Forks this build does not know**:
  - **Execution**: when 3 distinct hosts announce the same unknown `next` fork time, it is the
    horizon, and the node warns the operator to upgrade before it. Information only: nothing is
    refused ([el §5](el.md#5-fork-activations)).
  - **Gossip**: 3 distinct sequencer-signed blocks this build cannot read within 600 s stop
    the node (`p2p/src/network/state.rs`, `NetworkError::ProtocolChanged`). It never stores a
    block it cannot verify. EIP-8130 transactions (`0x79`, Everest) count as unreadable until
    supported.
- **Not checked**: an `AggregateVerifier` game's intermediate roots (only the last, the root
  claim, is compared).
- **Import**: HyperSync's Base rows lack header fields, deposit source hashes and mints in
  stretches; they are rebuilt from L1 ([import §10](import.md#10-unichain-and-base)).

# Investigation record (2026-10-04)

The source record behind the spec above, as read on 2026-10-04; what was built from it is
recorded in [decisions.md](decisions.md) (2026-10-04, Base). Every value below was read on 2026-10-04 (Base L2 head
52,183,374; Ethereum head 26,122,243). "L1 read" means a read-only `eth_call` / `eth_getLogs`
against `https://ethereum-rpc.publicnode.com`; "L2 read" means a read against Base's public
RPCs (`base.drpc.org`, `base-rpc.publicnode.com`). No HyperSync call was made.

Base left the OP Stack for its own codebase, [`base/base`](https://github.com/base/base),
announced 2026-02-18 ([CoinDesk](https://www.coindesk.com/business/2026/02/18/coinbase-s-base-moves-away-from-optimism-s-op-stack-in-major-tech-shift)),
and was removed from the Superchain Registry on 2026-04-27
([registry PR #1212](https://github.com/ethereum-optimism/superchain-registry/pull/1212), merged
2026-04-27T20:30Z). Its values now come from `base/base`:
[`crates/common/chains/src/config.rs`](https://github.com/base/base/blob/main/crates/common/chains/src/config.rs)
(last changed 2026-09-25, commit `615cf0f`), cross-checked against the last registry version of
[`base.toml`](https://github.com/ethereum-optimism/superchain-registry/blob/5b055be4d294ce43814a44c8839d89b2436dc8aa/superchain/configs/mainnet/base.toml)
(the parent of PR #1212) and against L1.

## A. History: one chain from genesis

**Same chain.**
- Base mainnet is one chain from block 0 to today, with no regenesis and no migration. L2 read: block 0 is still `0xf712…73dd` at 1686789347, and the parent hashes link without a break across the first blocks of all three Base forks:
  - Azul: 46,600,926 → 46,600,927;
  - Beryl: 47,810,526 → 47,810,527;
  - Cobalt: 52,000,926 → 52,000,927.
- The header has the same fields, and the same Jovian `extraData` (`0x01…4c4b40`), on both sides of each fork.
- The `base/base` client keeps the whole OP ladder from Bedrock in its fork schedule (`forks_for`, [`upgrade.rs`](https://github.com/base/base/blob/main/crates/common/chains/src/upgrade.rs)) and embeds the same genesis.
- Its Azul guide tells op-reth operators "Your existing `./reth-data` directory is fully compatible — no re-sync or snapshot restore is needed" ([Azul node upgrade](https://docs.base.org/base-chain/specs/upgrades/azul/node-upgrade)).

**The switch point is Azul**, 1779991200, first block 46,600,927.
- Base calls it the "first Base-specific network upgrade" ([`UPGRADES.md`](https://github.com/base/base/blob/main/docs/guides/UPGRADES.md)).
- From Azul on, "Only `base-reth-node` (EL) and `base-consensus` (CL) support Azul" (node upgrade guide).
- Before Azul, Base's rules are OP Mainnet's: the OP forks Regolith through Jovian at the times in section 1, Base's values (EIP-1559 6/50/250, no legacy chain), and no Base-only rule.

**Serving the old blocks.**
- Base's execution client, a reth derivative, still syncs and validates from genesis with the OP rules, and serves blocks over eth/69 in the same encodings (section 3).
- Its consensus client does **not** serve `payload_by_number`. "Base does not advertise or implement the legacy op-node `payload_by_number` request-response protocol" ([gossip README](https://github.com/base/base/blob/main/crates/consensus/gossip/README.md); also [`P2P.md`](https://github.com/base/base/blob/main/docs/guides/P2P.md)). This is despite the [P2P spec page](https://docs.base.org/base-chain/specs/protocol/consensus/p2p) listing it.
- Its consensus client "retires" block topics once every pre-fork block is outside the 60 s window, so only the current topic (v3, `/optimism/8453/3/blocks`) carries blocks.
- Old blocks therefore come only from execution peers, and from the importer.

## B. Modelling the Base-only changes

Working model: a fork is an activation time in `ChainSpec`, with per-layer rules switched on by `is_<fork>_active(ts)`.

| Fork | Change | Layer | Fits the model? |
|---|---|---|---|
| Azul | CLZ, P-256, MODEXP costs, 2^24 tx gas cap | EVM only | yes: we do not execute; the fork matters only through its time in the fork id |
| Azul | eth/69 | execution p2p | yes, already supported |
| Azul | discv5 protocol id `basev0`, own execution bootnodes | execution discovery | **no, structural**: not gated by a fork in Base's code; a property of the chain's network |
| Azul | `AggregateVerifier` games (type 621) | L1 commitment | **structural**: a new claim format, keyed by game type |
| Azul | `eth_config`, Engine API, Flashblocks metadata | RPC / client internals | nothing to model |
| Beryl | B20 as precompiles; withdrawal window 7 → 5 days | EVM; bridge | nothing to model: we follow L1 finality, not withdrawals |
| Cobalt | B20 changes; validity transactions (off-chain predicates); TEE registrar | EVM; RPC; L1 contracts we do not read | nothing to model |
| Cobalt | "dynamic node upgrades": fork times stored in an L1 contract | fork schedule | **structural if used**: today metrics-only on mainnet |
| Everest (announced) | EIP-8130 transactions, type `0x79` | transaction and receipt decoding, senders | **structural**: outside `op-alloy` |
| Denim | not published | unknown | unknown |

So no layer has been replaced wholesale. Consensus gossip, payload, header, receipts and the
output root are still OP's; every Base fork so far touches only the EVM, the RPC or the L1
contracts. The structural items became per-chain data in `ChainSpec` (discovery identity and
bootnodes, the game-type table, the fork list) and the unknown-fork handling of the spec above.

## 1. Chain parameters

| | Value | Source |
|---|---|---|
| Chain id | 8453 | `config.rs`, `base.toml` |
| Genesis hash | `0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd` | both |
| Genesis time | 1686789347 (2023-06-15) | both |
| Legacy chain | none: Bedrock at block 0 (`bedrock_block: 0`, Regolith at genesis time) | `config.rs` |
| Block time | 2 s | both |
| Unsafe block signer | `0xAf6E19BE0F9cE7f8afd49a1824851023A8249e8a` | `config.rs`; L1 read `SystemConfig(0x73a7…4072).unsafeBlockSigner()` returns the same |
| SystemConfig / OptimismPortal | `0x73a79Fab69143498Ed3712e519A88a918e1f4072` / `0x49048044D57e1C92A77f79988d21Fa8fAF74E97e` | `config.rs`, [Base contracts](https://docs.base.org/specifications/reference/base-contracts) |
| DisputeGameFactory | `0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e` (unchanged) | `base.toml`, Base contracts, L1 read |
| Gas limit | 400,000,000 at block 52,183,463 | L2 read |

Bootnodes (`config.rs`, `Bootnodes`): Base keeps the two discv5 networks apart, "Execution and
consensus run independent discv5 networks (different protocol IDs and ports)". There are 5
consensus `enr:` records on port 9222 (their `opstack` entry is chain 8453, fork 0, decoded from
the first record). There are 5 execution `enode://` hosts, each on 30301 (discv4) and 9200 (discv5).

Hardforks (`config.rs` `MAINNET`; OP forks also in `base.toml`):

| Fork | Time | What |
|---|---|---|
| Regolith | 1686789347 (genesis) | |
| Canyon | 1704992401 | |
| Delta | 1708560000 | consensus only |
| Ecotone | 1710374401 | |
| Fjord | 1720627201 | |
| Granite | 1726070401 | |
| Holocene | 1736445601 | |
| Isthmus | 1746806401 | |
| Jovian | 1764691201 | |
| **Azul** | 1779991200 (2026-05-28) | First Base-only fork ([Azul overview](https://docs.base.org/base-chain/specs/upgrades/azul/overview)): CLZ opcode (EIP-7939); P-256 precompile at `0x100` (EIP-7951); MODEXP cost and input limits (EIP-7883, 7823); per-transaction gas cap 2^24 (EIP-7825); **eth/69** (EIP-7642); **`basev0` discv5 protocol id**; `eth_config` (EIP-7910); multiproof `AggregateVerifier` |
| **Beryl** | 1782410400 (2026-06-25) | B20 token standard as Rust precompiles; single-proof withdrawal window 7 → 5 days ([Beryl overview](https://docs.base.org/base-chain/specs/upgrades/beryl/overview)) |
| **Cobalt** | 1790791200 (2026-09-30) | B20 changes; "validity transactions"; L1-stored upgrade timestamps ("dynamic node upgrades", metrics-only on mainnet); on-chain AWS Nitro attestation for the TEE signer ([Cobalt overview](https://docs.base.org/base-chain/specs/upgrades/cobalt/overview)) |
| Denim, Everest | unscheduled (`None`) | Everest gates EIP-8130 transactions (type `0x79`) ([`tx_type.rs`](https://github.com/base/base/blob/main/crates/common/consensus/src/transaction/tx_type.rs)) |

Base has no Karst (OP Mainnet's 2026-07-08 fork). "Validity transactions" are not a new type:
"Validity criteria are sent separately from the signed transaction. They do not appear in the
resulting onchain transaction" ([validity transactions](https://docs.base.org/specifications/build-transaction/validity-transactions)).

## 2. Consensus p2p

[Base P2P spec](https://docs.base.org/base-chain/specs/protocol/consensus/p2p) (read 2026-10-04) matches
rollup-node-p2p:
- topics: `/optimism/8453/{0,1,2,3}/blocks` (V1–V4; V4 from Isthmus, nothing newer);
- payloads: snappy-compressed signature ‖ (parent beacon root) ‖ SSZ `ExecutionPayload`;
- signature: secp256k1 over `keccak256(bytes32(0) ‖ chain_id ‖ payload_hash)`;
- validation: the 60 s past / 5 s future window, at most 5 blocks per height, per-version
  withdrawals/blob-gas/beacon-root rules;
- `payload_by_number` v0–v2 on the spec page, but Base's client does not implement it (section A);
- ENR `opstack` = chain id ‖ fork id (unsigned varints).

Consensus discovery uses the standard discv5 protocol id: `basev0` appears only in the
execution node ([`node.rs`](https://github.com/base/base/blob/main/crates/execution/node/src/node.rs)).
The current header has the post-Isthmus field set, Jovian `extraData` (version `0x01`) and the DA
footprint in `blobGasUsed` (L2 read). That is what our validation already expects from Jovian on.

## 3. Execution layer

- **Fork id** `0x68647e86`, next 0. This is what a Base node reports through `eth_config`
  (L2 read, `base-rpc.publicnode.com`, current fork activation 1790791200). It equals the
  EIP-2124 hash we computed from the genesis hash and the time forks Canyon, Ecotone, Fjord,
  Granite, Holocene, Isthmus, Jovian, Azul, Beryl, Cobalt.
- **eth/69** since Azul; we already speak it.
- **ENR key `opel`** for the fork id, on discv5 and discv4 ([`upgrade_signal.rs`](https://github.com/base/base/blob/main/crates/execution/cli/src/upgrade_signal.rs)).
- **discv5 protocol id `basev0`** (`BASE_V0_PROTOCOL_VERSION = *b"basev0"`, `node.rs`), not
  fork-gated. A node on the default `discv5` id cannot decrypt Base's discovery packets. Base
  still runs discv4 unless `disable_discovery_v4` is set.
- **Fork id can change at runtime**: a contract on L1 can schedule an activation time ("dynamic
  node upgrades"), and nodes then republish the `opel` entry (`upgrade_signal.rs`). The L2
  `ActivationRegistry` is at `0x8453…0001` (`eth_config`).
- **Transaction types in blocks**: 0, 1, 2, 4, 0x7e, the same set as OP. In the 4 recent blocks
  read, no other type appeared. The `base/base` envelope adds 0x79 (EIP-8130), gated behind the
  unscheduled Everest.
- **Receipts and header**: the OP format (deposit nonce and version; Isthmus withdrawals root =
  message-passer storage root, checked in section 4).

## 4. L1 commitment

Base still uses the DisputeGameFactory, with a new game ([proof contracts](https://docs.base.org/specifications/base-protocol/proofs/proof-contracts),
[proposer](https://docs.base.org/specifications/base-protocol/proofs/proposer)).

**Game type 621 (`AggregateVerifier`).**
- L1 reads:
  - `gameImpls(621)` = `0xeF9eCeA15265321753047EBF7D54C858D53cB94f`, the AggregateVerifier on Base's contract page;
  - every one of the latest games is type 621;
  - `gameImpls(0)` is unset; `gameImpls(1)` is still set;
  - 23,508 games in all.
- Each game is proven by TEE (AWS Nitro) and/or ZK (SP1) proofs. Finalization takes 5 days with
  one proof, or 1 day with two.

**Created by a plain call to the factory, `createWithInitData(uint32,bytes32,bytes,bytes)`.**
- L1 read: selector `0x1011f377`, `to` = the factory, sent by `0xc136…1cf5`.
- Before Base, our code accepted only `create(uint32,bytes32,bytes)` (`0x82ecf2f6`); both are
  read now.
- The `DisputeGameCreated` event is unchanged.

**`extraData` = `l2BlockNumber (32) ‖ parentGame (20) ‖ intermediateRoots (32 × n)`**, packed.
- On mainnet: 692 bytes, n = 20.
- A game every 600 L2 blocks (about 20 min), with an intermediate root every 30 blocks.
- L1 read of games 23,502–23,507: consecutive games are exactly 600 L2 blocks apart.

**The root claim is our output root.**
- `keccak256(bytes32(0) ‖ stateRoot ‖ messagePasserStorageRoot ‖ blockHash)` for L2 block
  52,181,760 equals the claim `0x24e371…bbb9` of game `0x12d7…895f`.
- Intermediate roots 0 and 9 equal the output roots of blocks 52,181,190 and 52,181,460.
- All from header fields (L2 read), as in `crates/primitives/src/game.rs`.

## 5. Importer

Envio HyperSync serves Base at `https://base.hypersync.xyz` (or `8453.hypersync.xyz`)
([supported networks](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks)), in
the OP formats (sections 1–3), with no legacy blocks. What its rows turned out to lack, and how
the importer completes them: [import §10](import.md#10-unichain-and-base).

## 6. Size

52,183,374 blocks (L2 read).

Downloaded since (2026-10-05): 2.57 TB of HyperSync answers for blocks 0 to 52,166,660.

Archive estimate: **about 2–3.5 TB**, roughly 2–4 times OP Mainnet's 914 GB for 157.7 M blocks.
- Method: 12 blocks sampled evenly across each chain's history (`base.drpc.org`, `optimism.drpc.org`). Block `size` plus receipts in their consensus encoding, rebuilt from `eth_getBlockReceipts`.
- Per block, Base averaged about 11 times OP's bytes.
- That ratio, times the block-count ratio (0.33), scales OP's measured 914 GB.
- One early Base block with 1.5 MB of receipts moves the estimate between 2 and 3.5 TB.
- Recent Base blocks are 64–142 KB, with 130–330 KB of receipts. That is about 5–8 GB a day.

## 7. Divergence risk

- **Cadence**: about six major upgrades a year ([The Defiant](https://thedefiant.io/news/blockchains/base-parts-ways-with-optimism-s-op-stack)); three are already live (Azul, Beryl, Cobalt), one every 1–3 months.
- **Breaking changes already in `base/base`**:
  - Everest's EIP-8130 transaction type `0x79`, which `op-alloy`'s envelope cannot decode, in blocks or receipts;
  - fork activation times that can come from an L1 contract instead of a release, metrics-only on mainnet today;
  - the `Zenith` test gate.
- **Proofs**: AggregateVerifier replaced fault proofs, and its contracts changed in each of Azul, Beryl and Cobalt.
- **Specs**: Base's p2p spec and its `payload_by_number` versions still match OP's. Nothing published says they will stay so.

## Open

- Whether discovery under `basev0` finds peers that serve history (`eth/69` block range), and
  whether Base execution peers accept our `Status` (our fork id matches theirs on paper): no
  Base node has run.
- Whether the L1 "dynamic node upgrades" will ever move a fork time away from the release value on mainnet. They are metrics-only today.
- When Denim and Everest are scheduled, and what Denim changes.
- The 2–3.5 TB size range (12 samples per chain).
