//! Ethereum mainnet as a devp2p network: what the `el` session code needs to talk to its
//! execution peers.

use alloy_hardforks::{EthereumHardfork, ForkCondition};
use alloy_primitives::{B256, b256};
use op_indexer_chainspec::DISCV5_PROTOCOL_ID;
use op_indexer_el::{ETH_RECORD_KEY, NetworkSpec};

/// The network id of Ethereum mainnet.
const MAINNET_NETWORK_ID: u64 = 1;
/// Hash of Ethereum mainnet's block 0.
const MAINNET_GENESIS_HASH: B256 =
    b256!("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3");

/// Timestamp of Ethereum mainnet's block 0.
const MAINNET_GENESIS_TIME: u64 = 1_438_269_973;

/// Ethereum mainnet's execution network, reached through `bootnodes`.
///
/// The fork schedule, from which the fork id peers check is computed, is `alloy-hardforks`'
/// `EthereumHardfork::mainnet()`: the forks that activate at a block (the merge has no block
/// of its own in the fork id) and those that activate at a timestamp. A fork that crate does
/// not know yet makes up-to-date peers refuse us, which discovery reports. As measured on
/// 2026-10-04 the crate's schedule gives fork id `07c9462e`, the one mainnet peers report.
pub(crate) fn mainnet(bootnodes: Vec<String>) -> NetworkSpec {
    let mut fork_blocks = Vec::new();
    let mut fork_times = Vec::new();
    for (_, condition) in EthereumHardfork::mainnet() {
        match condition {
            ForkCondition::Block(block)
            | ForkCondition::TTD {
                fork_block: Some(block),
                ..
            } => fork_blocks.push(block),
            ForkCondition::Timestamp(time) => fork_times.push(time),
            ForkCondition::TTD {
                fork_block: None, ..
            }
            | ForkCondition::Never => {}
        }
    }
    // Forks at block 0 are the genesis; several forks can share one activation.
    for list in [&mut fork_blocks, &mut fork_times] {
        list.retain(|activation| *activation != 0);
        list.sort_unstable();
        list.dedup();
    }
    NetworkSpec {
        label: "l1",
        network_id: MAINNET_NETWORK_ID,
        genesis_hash: MAINNET_GENESIS_HASH,
        genesis_time: MAINNET_GENESIS_TIME,
        fork_blocks,
        fork_times,
        bootnodes,
        discovery_id: DISCV5_PROTOCOL_ID,
        record_keys: &[ETH_RECORD_KEY],
        // Ethereum's network has no op-p2p-indexers.
        indexers_only_below: None,
    }
}
