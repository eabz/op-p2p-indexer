//! A dispute game read from L1, and the check of its claim against one of our blocks.
//!
//! A game is a claim, posted on L1 by the chain's proposer, that the L2 chain at one block has
//! one output root. Whoever holds that block can check the claim from its header alone. Does
//! not read L1 and does not decode a game's data (the chain specification knows the formats).
//!
//! The output root is `keccak256(version ‖ state_root ‖ withdrawal_storage_root ‖ block_hash)`
//! with version `bytes32(0)` ([L2 output commitment construction]); from Isthmus on the
//! withdrawal storage root is the header's withdrawals root ([storage root in header]).
//!
//! [L2 output commitment construction]: https://specs.optimism.io/protocol/proposals.html#l2-output-commitment-construction
//! [storage root in header]: https://specs.optimism.io/protocol/isthmus/exec-engine.html#l2tol1messagepasser-storage-root-in-header

use alloy_primitives::{Address, B256, BlockNumber, keccak256};

use crate::BlockRef;

/// A dispute game created by the chain's factory in an L1 block that is trusted: its data
/// was read from L1 and verified against that block's hash.
///
/// "Verified" is about L1, not about the claim: whether the claim is the output root of our
/// block is what [`VerifiedGame::check`] tells. A game is a bonded claim, not a proof: it can
/// still be challenged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedGame {
    /// The L1 block that created the game.
    pub l1_block: BlockRef,
    /// The game's contract on L1.
    pub game: Address,
    /// The game's type.
    pub game_type: u32,
    /// The L2 block the game is about.
    pub l2_block: BlockNumber,
    /// The output root the game claims for that block of our chain.
    pub output_root: B256,
    /// The timestamp that block must have, when the game names its block by time (a super
    /// game).
    pub timestamp: Option<u64>,
}

/// The dispute games L1 currently carries for the chain, as far as they have been read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct L1Games {
    /// The newest game created in a block of the L1 chain that is trusted as the head's. Its
    /// L1 block can still be replaced by an L1 reorg, after which this is an older game.
    pub newest: Option<VerifiedGame>,
    /// The newest game created in a finalized L1 block. Only moves forward.
    pub finalized: Option<VerifiedGame>,
}

/// Why a block is not the one a game claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClaimMismatch {
    /// The header has no withdrawals root (a block before Isthmus), so its output root
    /// cannot be computed from the header.
    #[error("block {block} has no withdrawals root in its header, so its output root is unknown")]
    NoStorageRoot {
        /// The L2 block.
        block: BlockNumber,
    },
    /// The game names its block by a time the block does not have.
    #[error("block {block} has timestamp {found}, the game is about the block at {claimed}")]
    Timestamp {
        /// The L2 block.
        block: BlockNumber,
        /// The timestamp in the game.
        claimed: u64,
        /// The block's timestamp.
        found: u64,
    },
    /// The block's output root is not the one claimed.
    #[error("block {block} has output root {computed}, the game claims {claimed}")]
    OutputRoot {
        /// The L2 block.
        block: BlockNumber,
        /// The output root in the game.
        claimed: B256,
        /// The block's output root.
        computed: B256,
    },
}

/// The version-0 output root of a block: `storage_root` is the storage root of the message
/// passer, which from Isthmus on is the header's withdrawals root.
#[must_use]
pub fn output_root(state_root: B256, storage_root: B256, hash: B256) -> B256 {
    keccak256([B256::ZERO, state_root, storage_root, hash].concat())
}

/// Checks that the block `block`, given by its hash and three fields of its header, has the
/// output root `claimed`, and the timestamp `claimed_timestamp` when the claim names one.
///
/// # Errors
///
/// Returns [`ClaimMismatch::Timestamp`] if the claim names another time,
/// [`ClaimMismatch::NoStorageRoot`] if the header has no withdrawals root, and
/// [`ClaimMismatch::OutputRoot`], with both roots, if the output roots differ.
pub fn check_claim(
    block: BlockNumber,
    (claimed, claimed_timestamp): (B256, Option<u64>),
    hash: B256,
    timestamp: u64,
    (state_root, withdrawals_root): (B256, Option<B256>),
) -> Result<(), ClaimMismatch> {
    if let Some(claimed) = claimed_timestamp
        && claimed != timestamp
    {
        return Err(ClaimMismatch::Timestamp {
            block,
            claimed,
            found: timestamp,
        });
    }
    let storage_root = withdrawals_root.ok_or(ClaimMismatch::NoStorageRoot { block })?;
    let computed = output_root(state_root, storage_root, hash);
    if computed != claimed {
        return Err(ClaimMismatch::OutputRoot {
            block,
            claimed,
            computed,
        });
    }
    Ok(())
}

impl VerifiedGame {
    /// Checks that our block [`Self::l2_block`], given by its hash and three fields of its
    /// header, is the block the game claims. `withdrawals_root` is the header's.
    ///
    /// # Errors
    ///
    /// As [`check_claim`].
    pub fn check(
        &self,
        hash: B256,
        timestamp: u64,
        state_root: B256,
        withdrawals_root: Option<B256>,
    ) -> Result<(), ClaimMismatch> {
        check_claim(
            self.l2_block,
            (self.output_root, self.timestamp),
            hash,
            timestamp,
            (state_root, withdrawals_root),
        )
    }
}
