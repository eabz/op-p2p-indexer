//! Static parameters of supported OP Stack chains: everything that differs between chains
//! lives here, so the rest of the workspace reads it from the configured [`ChainSpec`].
//!
//! Values come from the Superchain Registry
//! (<https://github.com/ethereum-optimism/superchain-registry>) and op-node's default bootnodes,
//! and for Base, which left the registry, from its own repository (`docs/base.md`). Add a chain
//! by adding a constant and listing it in [`ChainSpec::ALL`].
//!
//! The fork activations are configuration this project keeps current: the fork id execution
//! peers check ([EIP-2124]) is derived from them, and a node with a stale list only peers with
//! nodes that missed the same upgrade. Add every new hardfork here when it is scheduled.
//!
//! [EIP-2124]: https://eips.ethereum.org/EIPS/eip-2124

mod game;

use alloy_primitives::{Address, B256, BlockNumber, ChainId, address, b256};

use crate::game::{BASE_GAMES, ClaimFormat, OP_STACK_GAMES};
pub use game::{Claim, ClaimError, CreatedGame, created_topic};

/// A hardfork that activates at a timestamp. OP Stack forks first, then Base's own (Base left
/// the OP Stack at Azul and schedules its forks itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Hardfork {
    /// From it on the L1-attributes deposit is not a system transaction and deposit receipts
    /// record the sender's nonce. Not part of the fork id.
    Regolith,
    /// From it on deposit receipts hash with their nonce.
    Canyon,
    /// Ecotone.
    Ecotone,
    /// Fjord.
    Fjord,
    /// Granite.
    Granite,
    /// Holocene.
    Holocene,
    /// From it on the header carries the storage root of the message passer as its withdrawals
    /// root, and the hash of an empty requests list.
    Isthmus,
    /// From it on a block's blob gas used is its data availability footprint.
    Jovian,
    /// Karst (OP Stack, 2026-07).
    Karst,
    /// Base's first own fork (2026-05).
    Azul,
    /// Base's Beryl (2026-06).
    Beryl,
    /// Base's Cobalt (2026-09).
    Cobalt,
}

/// The discv5 protocol id of the OP Stack's discovery networks, and of Ethereum's.
pub const DISCV5_PROTOCOL_ID: [u8; 6] = *b"discv5";

/// Static parameters of one OP Stack chain.
#[derive(Debug)]
pub struct ChainSpec {
    /// L2 chain id.
    pub chain_id: ChainId,
    /// Short lowercase name, used in default paths: [`Self::default_data_dir`].
    pub name: &'static str,
    /// Address of the sequencer key that signs gossiped unsafe blocks.
    pub unsafe_block_signer: Address,
    /// Bootnodes of the consensus discovery network, as `enr:` records or `enode://` URLs
    /// ([`Self::consensus_bootnodes`]).
    consensus_bootnodes: &'static [&'static str],
    /// Bootnodes of the execution discovery network ([`Self::execution_bootnodes`]). The same
    /// list where consensus and execution nodes share one discv5 network (the Superchain's).
    execution_bootnodes: &'static [&'static str],
    /// The discv5 protocol id of the execution discovery network: [`DISCV5_PROTOCOL_ID`],
    /// unless the chain runs a network of its own (Base: `basev0`).
    pub execution_discovery_id: [u8; 6],
    /// Hash of the chain's block 0, which execution peers exchange in the eth status.
    pub genesis_hash: B256,
    /// Timestamp (seconds) of the chain's block 0, or any time before its first time fork
    /// when it is not known: a time fork at or before it is part of the genesis and not of the
    /// fork id ([EIP-2124]), which is all it is used for.
    ///
    /// [EIP-2124]: https://eips.ethereum.org/EIPS/eip-2124
    pub genesis_time: u64,
    /// Blocks at which a hardfork activated, ascending.
    pub fork_blocks: &'static [BlockNumber],
    /// The chain's time forks and their activation times, ascending: every fork it has
    /// scheduled. A fork not listed is not on the chain. The rules by fork read it through
    /// [`Self::activation`] and the named accessors ([`Self::canyon_time`], ...); the fork id
    /// through [`Self::fork_times`].
    pub time_forks: &'static [(Hardfork, u64)],
    /// The Bedrock block: the first block of the current chain format. Blocks before it are
    /// the legacy chain. From it on blocks are [`Self::block_time_secs`] apart. 0 for a chain
    /// born in the Bedrock format.
    pub bedrock_block: BlockNumber,
    /// Timestamp (seconds) of the Bedrock block: [`Self::genesis_time`] for a chain born in
    /// the Bedrock format.
    pub bedrock_time: u64,
    /// Hash of the last legacy block: the parent hash in the Bedrock block's header. `None`
    /// for a chain with no legacy chain ([`Self::has_legacy`]).
    pub last_legacy_hash: Option<B256>,
    /// Seconds between two blocks from Bedrock on.
    pub block_time_secs: u64,
    /// The base fee's EIP-1559 parameters until Holocene, from which a block's `extraData`
    /// carries them.
    pub eip1559: Eip1559,
    /// The chain's `DisputeGameFactory` on L1, whose games claim the chain's output roots.
    pub dispute_game_factory: Address,
    /// The chain's `OptimismPortal` on L1, whose `TransactionDeposited` logs are the chain's
    /// user deposits.
    pub optimism_portal: Address,
    /// The claim format of each game type the chain's factory creates.
    games: &'static [(u32, ClaimFormat)],
}

/// The EIP-1559 parameters of an OP Stack chain's base fee: the gas target is the gas limit
/// over `elasticity`, and the base fee moves by at most one `denominator`-th per block
/// (`denominator_canyon` from Canyon on).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eip1559 {
    /// Gas limit over gas target.
    pub elasticity: u64,
    /// Inverse of the largest change per block, before Canyon.
    pub denominator: u64,
    /// The same from Canyon on.
    pub denominator_canyon: u64,
}

/// The Superchain registry's EIP-1559 parameters, which OP Mainnet, Unichain and Base use
/// (checked for Base on 2026-10-05 against its headers at blocks 1.04 M and 11.5 M).
const SUPERCHAIN_EIP1559: Eip1559 = Eip1559 {
    elasticity: 6,
    denominator: 50,
    denominator_canyon: 250,
};

/// op-node's default bootnodes, shared by every Superchain chain: discovery is one network,
/// and peers are filtered by their `opstack` ENR entry. All are resolved and added at once, so
/// their order has no effect.
const SUPERCHAIN_BOOTNODES: &[&str] = &[
    // OP Labs
    "enode://869d07b5932f17e8490990f75a3f94195e9504ddb6b85f7189e5a9c0a8fff8b00aecf6f3ac450ecba6cdabdb5858788a94bde2b613e0f2d82e9b395355f76d1a@34.65.67.101:30305",
    "enode://2d4e7e9d48f4dd4efe9342706dd1b0024681bd4c3300d021f86fc75eab7865d4e0cbec6fbc883f011cfd6a57423e7e2f6e104baad2b744c3cafaec6bc7dc92c1@34.65.43.171:30305",
    "enode://9d7a3efefe442351217e73b3a593bcb8efffb55b4807699972145324eab5e6b382152f8d24f6301baebbfb5ecd4127bd3faab2842c04cd432bdf50ba092f6645@34.65.109.126:0?discport=30305",
    // Uniswap Labs
    "enode://010800c668896c100e8d64abc388ac5a22a8134a96fb0107c5d0c56d79ba7225c12d9e9e012d3cc0ee2701d7f63dd45f8abf0bbcf6f3c541f91742b1d7a99355@3.134.214.169:9222",
    "enode://b97abcc7011d06299c4bc44742be4a0e631a1a2925a2992adcfe80ed86bec5ff0ddf1b90d015f2dbb5e305560e12c9873b2dad72d84d131ac4be9f2a4c74b763@52.14.30.39:9222",
    "enode://760230a662610620d6d2e4ad846a6dccbceaa4556872dfacf9cdca7c2f5b49e4c66e822ed2e8813debb5fb7391f0519b8d075e565a2a89c79a9e4092e81b3e5b@3.148.100.173:9222",
    // Base
    "enr:-J24QNz9lbrKbN4iSmmjtnr7SjUMk4zB7f1krHZcTZx-JRKZd0kA2gjufUROD6T3sOWDVDnFJRvqBBo62zuF-hYCohOGAYiOoEyEgmlkgnY0gmlwhAPniryHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQKNVFlCxh_B-716tTs-h1vMzZkSs1FTu_OYTNjgufplG4N0Y3CCJAaDdWRwgiQG",
    "enr:-J24QH-f1wt99sfpHy4c0QJM-NfmsIfmlLAMMcgZCUEgKG_BBYFc6FwYgaMJMQN5dsRBJApIok0jFn-9CS842lGpLmqGAYiOoDRAgmlkgnY0gmlwhLhIgb2Hb3BzdGFja4OFQgCJc2VjcDI1NmsxoQJ9FTIv8B9myn1MWaC_2lJ-sMoeCDkusCsk4BYHjjCq04N0Y3CCJAaDdWRwgiQG",
    "enr:-J24QDXyyxvQYsd0yfsN0cRr1lZ1N11zGTplMNlW4xNEc7LkPXh0NAJ9iSOVdRO95GPYAIc6xmyoCCG6_0JxdL3a0zaGAYiOoAjFgmlkgnY0gmlwhAPckbGHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQJwoS7tzwxqXSyFL7g0JM-KWVbgvjfB8JA__T7yY_cYboN0Y3CCJAaDdWRwgiQG",
    "enr:-J24QHmGyBwUZXIcsGYMaUqGGSl4CFdx9Tozu-vQCn5bHIQbR7On7dZbU61vYvfrJr30t0iahSqhc64J46MnUO2JvQaGAYiOoCKKgmlkgnY0gmlwhAPnCzSHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQINc4fSijfbNIiGhcgvwjsjxVFJHUstK9L1T8OTKUjgloN0Y3CCJAaDdWRwgiQG",
    "enr:-J24QG3ypT4xSu0gjb5PABCmVxZqBjVw9ca7pvsI8jl4KATYAnxBmfkaIuEqy9sKvDHKuNCsy57WwK9wTt2aQgcaDDyGAYiOoGAXgmlkgnY0gmlwhDbGmZaHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQIeAK_--tcLEiu7HvoUlbV52MspE0uCocsx1f_rYvRenIN0Y3CCJAaDdWRwgiQG",
];

/// OP Mainnet (chain id 10).
pub const OP_MAINNET: ChainSpec = ChainSpec {
    chain_id: 10,
    name: "op",
    unsafe_block_signer: address!("0xAAAA45d9549EDA09E70937013520214382Ffc4A2"),
    consensus_bootnodes: SUPERCHAIN_BOOTNODES,
    execution_bootnodes: SUPERCHAIN_BOOTNODES,
    execution_discovery_id: DISCV5_PROTOCOL_ID,
    // Sent by every OP Mainnet execution peer in its eth status (observed 2026-10-04).
    genesis_hash: b256!("0x7ca38a1916c42007829c55e69d3e9a73265554b586a499015373241b8a3fa48b"),
    // Not sourced: the registry gives only the Bedrock block's time, and no genesis file of the
    // legacy chain was at hand. Every time fork is years after block 0, so any value before
    // Canyon gives the same fork id (`c29239af`, observed from peers); 0 says "before all".
    genesis_time: 0,
    // Berlin and the Bedrock transition (which also carries London and the merge forks), as in
    // `alloy-op-hardforks` and op-geth's OP Mainnet chain config.
    fork_blocks: &[3_950_000, 105_235_063],
    // The time forks, as in `superchain/configs/mainnet/op.toml` of the superchain registry
    // (read 2026-10-03). Karst's time was first learned from execution peers, whose fork hash
    // `c29239af` it reproduces. Regolith is active from the Bedrock block (registry).
    time_forks: &[
        (Hardfork::Regolith, 0),
        (Hardfork::Canyon, 1_704_992_401),
        (Hardfork::Ecotone, 1_710_374_401),
        (Hardfork::Fjord, 1_720_627_201),
        (Hardfork::Granite, 1_726_070_401),
        (Hardfork::Holocene, 1_736_445_601),
        (Hardfork::Isthmus, 1_746_806_401),
        (Hardfork::Jovian, 1_764_691_201),
        (Hardfork::Karst, 1_783_526_401),
    ],
    bedrock_block: 105_235_063,
    bedrock_time: 1_686_068_903,
    // The parent hash in the header of the Bedrock block (hash `0xdbf6a80f…afd3`), which every
    // execution peer served identically (`docs/el-viability.md`).
    last_legacy_hash: Some(b256!(
        "0x21a168dfa5e727926063a28ba16fd5ee84c814e847c81a699c7a0ea551e4ca50"
    )),
    block_time_secs: 2,
    eip1559: SUPERCHAIN_EIP1559,
    // `DisputeGameFactoryProxy` in `superchain/configs/mainnet/op.toml` of the registry.
    dispute_game_factory: address!("0xe5965Ab5962eDc7477C8520243A95517CD252fA9"),
    // Checked 2026-10-05: the source hashes of blocks 140,000,144 and 140,000,162.
    optimism_portal: address!("0xbEb5Fc579115071764c7423A4f12eDde41f106Ed"),
    games: OP_STACK_GAMES,
};

/// Timestamp of Unichain's block 0, which is also its Bedrock block.
const UNICHAIN_GENESIS_TIME: u64 = 1_730_748_359;

/// Unichain (chain id 130), born in the Bedrock format: no legacy chain, every block fork and
/// every time fork through Granite at its genesis.
///
/// Values from `superchain/configs/mainnet/unichain.toml` of the superchain registry (read
/// 2026-10-04) unless noted. Bootnodes are the shared Superchain list.
pub const UNICHAIN: ChainSpec = ChainSpec {
    chain_id: 130,
    name: "unichain",
    // `unsafeBlockSigner()` of the chain's `SystemConfigProxy`
    // `0xc407398d063f942feBbcC6F80a156b47F3f1BDA6` on L1, read 2026-10-04 (the same call on OP
    // Mainnet's returns its signer above).
    unsafe_block_signer: address!("0x833C6f278474A78658af91aE8edC926FE33a230e"),
    consensus_bootnodes: SUPERCHAIN_BOOTNODES,
    execution_bootnodes: SUPERCHAIN_BOOTNODES,
    execution_discovery_id: DISCV5_PROTOCOL_ID,
    genesis_hash: b256!("0x3425162ddf41a0a1f0106d67b71828c9a9577e6ddeb94e4f33d2cde1fdc3befe"),
    genesis_time: UNICHAIN_GENESIS_TIME,
    // London and every other block fork are at block 0, which is not part of a fork id.
    fork_blocks: &[],
    // The registry lists no Regolith time: a chain born after it has it from genesis.
    time_forks: &[
        (Hardfork::Regolith, 0),
        (Hardfork::Canyon, 0),
        (Hardfork::Ecotone, 0),
        (Hardfork::Fjord, 0),
        (Hardfork::Granite, 0),
        (Hardfork::Holocene, 1_736_445_601),
        (Hardfork::Isthmus, 1_746_806_401),
        (Hardfork::Jovian, 1_764_691_201),
        (Hardfork::Karst, 1_783_526_401),
    ],
    bedrock_block: 0,
    bedrock_time: UNICHAIN_GENESIS_TIME,
    last_legacy_hash: None,
    block_time_secs: 1,
    eip1559: SUPERCHAIN_EIP1559,
    // `DisputeGameFactoryProxy`. Its games are super games (type 9, the portal's respected
    // type), whose super root holds an entry for chain id 130.
    dispute_game_factory: address!("0x2F12d621a16e2d3285929C9996f478508951dFe4"),
    // From the Superchain registry; not checked against Unichain's deposits.
    optimism_portal: address!("0x0bd48f6B86a26D3a217d0Fa6FfE2B491B956A7a2"),
    games: OP_STACK_GAMES,
};

/// Timestamp of Base's block 0, which is also its Bedrock block.
const BASE_GENESIS_TIME: u64 = 1_686_789_347;

/// Base (chain id 8453), born in the Bedrock format. Base left the OP Stack at Azul: from it on
/// it schedules its own forks, runs its own execution discovery network and proves its output
/// roots with its own games (`docs/base.md`).
///
/// Values from `MAINNET` in `crates/common/chains/src/config.rs` of
/// <https://github.com/base/base> (commit `615cf0f`, 2026-09-25, read 2026-10-04), which match
/// the last registry version of `base.toml` for every value both have, unless noted.
pub const BASE: ChainSpec = ChainSpec {
    chain_id: 8453,
    name: "base",
    // Also `unsafeBlockSigner()` of `SystemConfig` `0x73a79Fab69143498Ed3712e519A88a918e1f4072`
    // on L1, read 2026-10-04.
    unsafe_block_signer: address!("0xAf6E19BE0F9cE7f8afd49a1824851023A8249e8a"),
    // Base's consensus nodes run the standard discv5, in the same DHT as the Superchain's (its
    // five consensus records are in the shared list).
    consensus_bootnodes: SUPERCHAIN_BOOTNODES,
    execution_bootnodes: BASE_EXECUTION_BOOTNODES,
    // `BASE_V0_PROTOCOL_VERSION` in `crates/execution/node/src/node.rs` of `base/base`.
    execution_discovery_id: *b"basev0",
    genesis_hash: b256!("0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd"),
    genesis_time: BASE_GENESIS_TIME,
    fork_blocks: &[],
    // Delta is not listed: it changes only derivation. These times give the fork id
    // `68647e86` a Base node reports through `eth_config` (read 2026-10-04). Denim and Everest
    // are not scheduled.
    time_forks: &[
        (Hardfork::Regolith, BASE_GENESIS_TIME),
        (Hardfork::Canyon, 1_704_992_401),
        (Hardfork::Ecotone, 1_710_374_401),
        (Hardfork::Fjord, 1_720_627_201),
        (Hardfork::Granite, 1_726_070_401),
        (Hardfork::Holocene, 1_736_445_601),
        (Hardfork::Isthmus, 1_746_806_401),
        (Hardfork::Jovian, 1_764_691_201),
        (Hardfork::Azul, 1_779_991_200),
        (Hardfork::Beryl, 1_782_410_400),
        (Hardfork::Cobalt, 1_790_791_200),
    ],
    bedrock_block: 0,
    bedrock_time: BASE_GENESIS_TIME,
    last_legacy_hash: None,
    block_time_secs: 2,
    eip1559: SUPERCHAIN_EIP1559,
    // `DisputeGameFactoryProxy` in `base.toml`, unchanged on Base's contract page
    // (<https://docs.base.org/specifications/reference/base-contracts>).
    dispute_game_factory: address!("0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e"),
    // Checked 2026-10-05: the source hashes of blocks 11,500,013 and 11,500,037.
    optimism_portal: address!("0x49048044D57e1C92A77f79988d21Fa8fAF74E97e"),
    games: BASE_GAMES,
};

/// Base's execution bootnodes on its `basev0` discv5 port (9200), from `Bootnodes::execution`
/// in `base/base`'s `config.rs`. The same hosts' port 30301 entries are discv4, which this
/// node does not run.
const BASE_EXECUTION_BOOTNODES: &[&str] = &[
    "enode://87a32fd13bd596b2ffca97020e31aef4ddcc1bbd4b95bb633d16c1329f654f34049ed240a36b449fda5e5225d70fe40bc667f53c304b71f8e68fc9d448690b51@3.231.138.188:9200",
    "enode://ca21ea8f176adb2e229ce2d700830c844af0ea941a1d8152a9513b966fe525e809c3a6c73a2c18a12b74ed6ec4380edf91662778fe0b79f6a591236e49e176f9@184.72.129.189:9200",
    "enode://acf4507a211ba7c1e52cdf4eef62cdc3c32e7c9c47998954f7ba024026f9a6b2150cd3f0b734d9c78e507ab70d59ba61dfe5c45e1078c7ad0775fb251d7735a2@3.220.145.177:9200",
    "enode://8a5a5006159bf079d06a04e5eceab2a1ce6e0f721875b2a9c96905336219dbe14203d38f70f3754686a6324f786c2f9852d8c0dd3adac2d080f4db35efc678c5@3.231.11.52:9200",
    "enode://cdadbe835308ad3557f9a1de8db411da1a260a98f8421d62da90e71da66e55e98aaa8e90aa7ce01b408a54e4bd2253d701218081ded3dbe5efbbc7b41d7cef79@54.198.153.150:9200",
];

impl ChainSpec {
    /// Every supported chain.
    pub const ALL: &'static [&'static Self] = &[&OP_MAINNET, &UNICHAIN, &BASE];

    /// The bootnodes of the consensus discovery network (the global discv5 DHT).
    pub fn consensus_bootnodes(&self) -> impl Iterator<Item = &'static str> {
        self.consensus_bootnodes.iter().copied()
    }

    /// The bootnodes of the execution discovery network.
    pub fn execution_bootnodes(&self) -> impl Iterator<Item = &'static str> {
        self.execution_bootnodes.iter().copied()
    }

    /// When `fork` activates on this chain; `None` if the chain does not have it.
    #[must_use]
    pub const fn activation(&self, fork: Hardfork) -> Option<u64> {
        let mut rest = self.time_forks;
        while let [(listed, time), tail @ ..] = rest {
            if *listed as u8 == fork as u8 {
                return Some(*time);
            }
            rest = tail;
        }
        None
    }

    /// When `fork` activates on this chain: never (`u64::MAX`) if it does not have it.
    const fn activation_or_never(&self, fork: Hardfork) -> u64 {
        match self.activation(fork) {
            Some(time) => time,
            None => u64::MAX,
        }
    }

    /// Activation time of Regolith ([`Hardfork::Regolith`]).
    #[must_use]
    pub const fn regolith_time(&self) -> u64 {
        self.activation_or_never(Hardfork::Regolith)
    }

    /// Activation time of Canyon. Deposit receipts hash differently before it: the deposit
    /// nonce is not part of the hashed receipt.
    #[must_use]
    pub const fn canyon_time(&self) -> u64 {
        self.activation_or_never(Hardfork::Canyon)
    }

    /// Activation time of Ecotone.
    #[must_use]
    pub const fn ecotone_time(&self) -> u64 {
        self.activation_or_never(Hardfork::Ecotone)
    }

    /// Activation time of Holocene ([`Hardfork::Holocene`]).
    #[must_use]
    pub const fn holocene_time(&self) -> u64 {
        self.activation_or_never(Hardfork::Holocene)
    }

    /// Activation time of Isthmus ([`Hardfork::Isthmus`]).
    #[must_use]
    pub const fn isthmus_time(&self) -> u64 {
        self.activation_or_never(Hardfork::Isthmus)
    }

    /// Activation time of Jovian ([`Hardfork::Jovian`]).
    #[must_use]
    pub const fn jovian_time(&self) -> u64 {
        self.activation_or_never(Hardfork::Jovian)
    }

    /// Whether the chain has a legacy chain before its Bedrock block.
    #[must_use]
    pub const fn has_legacy(&self) -> bool {
        self.last_legacy_hash.is_some()
    }

    /// How many of the chain's blocks `secs` seconds span, from Bedrock on.
    #[must_use]
    pub const fn blocks_in(&self, secs: u64) -> u64 {
        match secs.checked_div(self.block_time_secs) {
            Some(blocks) => blocks,
            None => 0,
        }
    }

    /// The activation times of the hardforks that change the fork id, ascending: every time
    /// fork but Regolith, which has no activation of its own in the fork id.
    #[must_use]
    pub fn fork_times(&self) -> Vec<u64> {
        self.time_forks
            .iter()
            .filter(|(fork, _)| *fork != Hardfork::Regolith)
            .map(|(_, time)| *time)
            .collect()
    }

    /// The data directory a node uses for this chain when none is configured, and under
    /// which the importer loads its archive by default: `data-<name>`. Two chains on one host
    /// therefore never share a directory unless told to.
    #[must_use]
    pub fn default_data_dir(&self) -> String {
        format!("data-{}", self.name)
    }

    /// Returns the spec for `chain_id`, if supported.
    pub fn by_chain_id(chain_id: ChainId) -> Option<&'static Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|spec| spec.chain_id == chain_id)
    }
}
