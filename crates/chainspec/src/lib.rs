//! Static parameters of supported OP Stack chains.
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

use alloy_eip2124::{ForkFilter, ForkFilterKey, ForkId, Head};
use alloy_primitives::{Address, B256, BlockNumber, ChainId, address, b256};

/// Static parameters of one OP Stack chain.
#[derive(Debug)]
pub struct ChainSpec {
    /// L2 chain id.
    pub chain_id: ChainId,
    /// Address of the sequencer key that signs gossiped unsafe blocks.
    pub unsafe_block_signer: Address,
    /// Discovery bootnodes, as `enr:` records or `enode://` URLs, preferred first. Consensus
    /// and execution nodes share one discv5 network, so the execution network uses them too.
    pub bootnodes: &'static [&'static str],
    /// Hash of the chain's block 0, which execution peers exchange in the eth status.
    pub genesis_hash: B256,
    /// Blocks at which a hardfork activated, ascending.
    pub fork_blocks: &'static [BlockNumber],
    /// Timestamps (seconds) at which a hardfork activated, ascending.
    pub fork_times: &'static [u64],
    /// Activation time of Canyon. Deposit receipts hash differently before it: the deposit
    /// nonce is not part of the hashed receipt.
    pub canyon_time: u64,
}

/// OP Mainnet (chain id 10).
///
/// Bootnodes are op-node's defaults: the OP Labs and Uniswap Labs nodes first, then Base nodes
/// as a fallback entry point (the discovery network is shared across Superchain chains, and
/// peers are filtered by their `opstack` ENR entry).
pub const OP_MAINNET: ChainSpec = ChainSpec {
    chain_id: 10,
    unsafe_block_signer: address!("0xAAAA45d9549EDA09E70937013520214382Ffc4A2"),
    bootnodes: &[
        // OP Labs
        "enode://869d07b5932f17e8490990f75a3f94195e9504ddb6b85f7189e5a9c0a8fff8b00aecf6f3ac450ecba6cdabdb5858788a94bde2b613e0f2d82e9b395355f76d1a@34.65.67.101:30305",
        "enode://2d4e7e9d48f4dd4efe9342706dd1b0024681bd4c3300d021f86fc75eab7865d4e0cbec6fbc883f011cfd6a57423e7e2f6e104baad2b744c3cafaec6bc7dc92c1@34.65.43.171:30305",
        "enode://9d7a3efefe442351217e73b3a593bcb8efffb55b4807699972145324eab5e6b382152f8d24f6301baebbfb5ecd4127bd3faab2842c04cd432bdf50ba092f6645@34.65.109.126:0?discport=30305",
        // Uniswap Labs
        "enode://010800c668896c100e8d64abc388ac5a22a8134a96fb0107c5d0c56d79ba7225c12d9e9e012d3cc0ee2701d7f63dd45f8abf0bbcf6f3c541f91742b1d7a99355@3.134.214.169:9222",
        "enode://b97abcc7011d06299c4bc44742be4a0e631a1a2925a2992adcfe80ed86bec5ff0ddf1b90d015f2dbb5e305560e12c9873b2dad72d84d131ac4be9f2a4c74b763@52.14.30.39:9222",
        "enode://760230a662610620d6d2e4ad846a6dccbceaa4556872dfacf9cdca7c2f5b49e4c66e822ed2e8813debb5fb7391f0519b8d075e565a2a89c79a9e4092e81b3e5b@3.148.100.173:9222",
        // Base (fallback)
        "enr:-J24QNz9lbrKbN4iSmmjtnr7SjUMk4zB7f1krHZcTZx-JRKZd0kA2gjufUROD6T3sOWDVDnFJRvqBBo62zuF-hYCohOGAYiOoEyEgmlkgnY0gmlwhAPniryHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQKNVFlCxh_B-716tTs-h1vMzZkSs1FTu_OYTNjgufplG4N0Y3CCJAaDdWRwgiQG",
        "enr:-J24QH-f1wt99sfpHy4c0QJM-NfmsIfmlLAMMcgZCUEgKG_BBYFc6FwYgaMJMQN5dsRBJApIok0jFn-9CS842lGpLmqGAYiOoDRAgmlkgnY0gmlwhLhIgb2Hb3BzdGFja4OFQgCJc2VjcDI1NmsxoQJ9FTIv8B9myn1MWaC_2lJ-sMoeCDkusCsk4BYHjjCq04N0Y3CCJAaDdWRwgiQG",
        "enr:-J24QDXyyxvQYsd0yfsN0cRr1lZ1N11zGTplMNlW4xNEc7LkPXh0NAJ9iSOVdRO95GPYAIc6xmyoCCG6_0JxdL3a0zaGAYiOoAjFgmlkgnY0gmlwhAPckbGHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQJwoS7tzwxqXSyFL7g0JM-KWVbgvjfB8JA__T7yY_cYboN0Y3CCJAaDdWRwgiQG",
        "enr:-J24QHmGyBwUZXIcsGYMaUqGGSl4CFdx9Tozu-vQCn5bHIQbR7On7dZbU61vYvfrJr30t0iahSqhc64J46MnUO2JvQaGAYiOoCKKgmlkgnY0gmlwhAPnCzSHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQINc4fSijfbNIiGhcgvwjsjxVFJHUstK9L1T8OTKUjgloN0Y3CCJAaDdWRwgiQG",
        "enr:-J24QG3ypT4xSu0gjb5PABCmVxZqBjVw9ca7pvsI8jl4KATYAnxBmfkaIuEqy9sKvDHKuNCsy57WwK9wTt2aQgcaDDyGAYiOoGAXgmlkgnY0gmlwhDbGmZaHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQIeAK_--tcLEiu7HvoUlbV52MspE0uCocsx1f_rYvRenIN0Y3CCJAaDdWRwgiQG",
    ],
    // Sent by every OP Mainnet execution peer in its eth status (observed 2026-10-04).
    genesis_hash: b256!("0x7ca38a1916c42007829c55e69d3e9a73265554b586a499015373241b8a3fa48b"),
    // Berlin and the Bedrock transition (which also carries London and the merge forks), as in
    // `alloy-op-hardforks` and op-geth's OP Mainnet chain config.
    fork_blocks: &[3_950_000, 105_235_063],
    // Canyon, Ecotone, Fjord, Granite, Holocene, Isthmus and Jovian from `alloy-op-hardforks`
    // 0.5.0. The last one, 2026-07-08 16:00:01 UTC, is not in that crate: it was learned from
    // execution peers (node records of nodes that had not upgraded announce it as their next
    // fork) and confirmed by the result, fork hash `c29239af`, being the one up-to-date peers
    // report. Its name is not recorded here because no source for it was read.
    fork_times: &[
        1_704_992_401,
        1_710_374_401,
        1_720_627_201,
        1_726_070_401,
        1_736_445_601,
        1_746_806_401,
        1_764_691_201,
        1_783_526_401,
    ],
    canyon_time: 1_704_992_401,
};

impl ChainSpec {
    /// Every supported chain.
    pub const ALL: &'static [&'static Self] = &[&OP_MAINNET];

    /// The EIP-2124 fork filter for a node whose head is at `head_number` and
    /// `head_timestamp` (seconds): it yields our fork id and validates a peer's.
    #[must_use]
    pub fn fork_filter(&self, head_number: BlockNumber, head_timestamp: u64) -> ForkFilter {
        let head = Head {
            number: head_number,
            timestamp: head_timestamp,
            ..Head::default()
        };
        let forks = self
            .fork_blocks
            .iter()
            .map(|block| ForkFilterKey::Block(*block))
            .chain(
                self.fork_times
                    .iter()
                    .map(|time| ForkFilterKey::Time(*time)),
            );
        // OP Mainnet's genesis timestamp is 0; time forks are all later.
        ForkFilter::new(head, self.genesis_hash, 0, forks)
    }

    /// The fork id a node at this head advertises.
    #[must_use]
    pub fn fork_id(&self, head_number: BlockNumber, head_timestamp: u64) -> ForkId {
        self.fork_filter(head_number, head_timestamp).current()
    }

    /// Whether `time` is a hardfork activation this build knows.
    #[must_use]
    pub fn knows_fork_time(&self, time: u64) -> bool {
        self.fork_times.contains(&time)
    }

    /// Returns the spec for `chain_id`, if supported.
    pub fn by_chain_id(chain_id: ChainId) -> Option<&'static Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|spec| spec.chain_id == chain_id)
    }
}
