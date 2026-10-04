//! Static parameters of supported OP Stack chains: everything that differs between chains
//! lives here, so the rest of the workspace reads it from the configured [`ChainSpec`].
//!
//! Values come from the Superchain Registry
//! (<https://github.com/ethereum-optimism/superchain-registry>) and op-node's default bootnodes.
//! Add a chain by adding a constant and listing it in [`ChainSpec::ALL`].
//!
//! The fork activations are configuration this project keeps current: the fork id execution
//! peers check ([EIP-2124]) is derived from them, and a node with a stale list only peers with
//! nodes that missed the same upgrade. Add every new hardfork here when it is scheduled.
//!
//! [EIP-2124]: https://eips.ethereum.org/EIPS/eip-2124

mod game;

use alloy_primitives::{Address, B256, BlockNumber, ChainId, address, b256};

pub use game::{Claim, ClaimError, CreatedGame, created_topic};

/// Static parameters of one OP Stack chain.
#[derive(Debug)]
pub struct ChainSpec {
    /// L2 chain id.
    pub chain_id: ChainId,
    /// Short lowercase name, used in default paths: [`Self::default_data_dir`].
    pub name: &'static str,
    /// Address of the sequencer key that signs gossiped unsafe blocks.
    pub unsafe_block_signer: Address,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs ([`Self::bootnodes`]).
    /// Consensus and execution nodes share one discv5 network, so the execution network uses
    /// them too.
    bootnodes: &'static [&'static str],
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
    /// Activation time of Canyon. Deposit receipts hash differently before it: the deposit
    /// nonce is not part of the hashed receipt.
    pub canyon_time: u64,
    /// Activation time of Ecotone.
    pub ecotone_time: u64,
    /// Activation time of Fjord.
    pub fjord_time: u64,
    /// Activation time of Granite.
    pub granite_time: u64,
    /// Activation time of Holocene.
    pub holocene_time: u64,
    /// Activation time of Jovian.
    pub jovian_time: u64,
    /// Activation time of Karst.
    pub karst_time: u64,
    /// Activation time of Regolith. From it on the L1-attributes deposit is not a system
    /// transaction and deposit receipts record the sender's nonce.
    pub regolith_time: u64,
    /// Activation time of Isthmus. From it on the header carries the storage root of the
    /// message passer as its withdrawals root, and the hash of an empty requests list.
    pub isthmus_time: u64,
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
    /// The chain's `DisputeGameFactory` on L1, whose games claim the chain's output roots.
    pub dispute_game_factory: Address,
}

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
    bootnodes: SUPERCHAIN_BOOTNODES,
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
    // `c29239af` it reproduces.
    canyon_time: 1_704_992_401,
    ecotone_time: 1_710_374_401,
    fjord_time: 1_720_627_201,
    granite_time: 1_726_070_401,
    holocene_time: 1_736_445_601,
    jovian_time: 1_764_691_201,
    karst_time: 1_783_526_401,
    // Regolith is active from the Bedrock block on OP Mainnet (superchain registry).
    regolith_time: 0,
    isthmus_time: 1_746_806_401,
    bedrock_block: 105_235_063,
    bedrock_time: 1_686_068_903,
    // The parent hash in the header of the Bedrock block (hash `0xdbf6a80f…afd3`), which every
    // execution peer served identically (`docs/el-viability.md`).
    last_legacy_hash: Some(b256!(
        "0x21a168dfa5e727926063a28ba16fd5ee84c814e847c81a699c7a0ea551e4ca50"
    )),
    block_time_secs: 2,
    // `DisputeGameFactoryProxy` in `superchain/configs/mainnet/op.toml` of the registry.
    dispute_game_factory: address!("0xe5965Ab5962eDc7477C8520243A95517CD252fA9"),
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
    bootnodes: SUPERCHAIN_BOOTNODES,
    genesis_hash: b256!("0x3425162ddf41a0a1f0106d67b71828c9a9577e6ddeb94e4f33d2cde1fdc3befe"),
    genesis_time: UNICHAIN_GENESIS_TIME,
    // London and every other block fork are at block 0, which is not part of a fork id.
    fork_blocks: &[],
    canyon_time: 0,
    ecotone_time: 0,
    fjord_time: 0,
    granite_time: 0,
    holocene_time: 1_736_445_601,
    jovian_time: 1_764_691_201,
    karst_time: 1_783_526_401,
    // The registry lists no Regolith time: a chain born after it has it from genesis.
    regolith_time: 0,
    isthmus_time: 1_746_806_401,
    bedrock_block: 0,
    bedrock_time: UNICHAIN_GENESIS_TIME,
    last_legacy_hash: None,
    block_time_secs: 1,
    // `DisputeGameFactoryProxy`. Its games are super games (type 9, the portal's respected
    // type), whose super root holds an entry for chain id 130.
    dispute_game_factory: address!("0x2F12d621a16e2d3285929C9996f478508951dFe4"),
};

impl ChainSpec {
    /// Every supported chain.
    pub const ALL: &'static [&'static Self] = &[&OP_MAINNET, &UNICHAIN];

    /// The discovery bootnodes.
    pub fn bootnodes(&self) -> impl Iterator<Item = &'static str> {
        self.bootnodes.iter().copied()
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

    /// The hardforks that activate at a timestamp and change the fork id, ascending. Regolith
    /// is not one of them: it has no activation of its own in the fork id.
    #[must_use]
    pub const fn fork_times(&self) -> [u64; 8] {
        [
            self.canyon_time,
            self.ecotone_time,
            self.fjord_time,
            self.granite_time,
            self.holocene_time,
            self.isthmus_time,
            self.jovian_time,
            self.karst_time,
        ]
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
