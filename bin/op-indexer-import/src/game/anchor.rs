//! The game that anchors the top of a range, and the check that the downloaded block is the
//! one it claims.
//!
//! Does not find the game (the parent module does) and does not read headers: the caller hands
//! the fields in.

use alloy_primitives::{Address, B256, keccak256};
use serde::{Deserialize, Serialize};

/// The game that anchors the top of the range: what `download` records and `verify` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GameAnchor {
    /// The game's contract on L1.
    pub(crate) game: Address,
    /// The game's type.
    pub(crate) game_type: u32,
    /// The L2 block the game is about: the last block of the range.
    pub(crate) l2_block: u64,
    /// The output root the game claims for that block.
    pub(crate) output_root: B256,
    /// The timestamp that block must have, when the game names its block by time (a super
    /// game).
    pub(crate) timestamp: Option<u64>,
    /// The L1 block the game was created in.
    pub(crate) l1_block: u64,
}

/// Why a downloaded block is not the one a game claims.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AnchorError {
    #[error(
        "block {block} has no withdrawals root in its header (it is before Isthmus), so its \
         output root cannot be computed and game {game} cannot anchor the range"
    )]
    NoStorageRoot { block: u64, game: Address },
    #[error(
        "block {block} has timestamp {found}, and game {game} is about the block at {claimed}; \
         the chain's Bedrock block, its time or the block time is configured wrongly"
    )]
    Timestamp {
        block: u64,
        game: Address,
        claimed: u64,
        found: u64,
    },
    #[error(
        "block {block}: the output root of the downloaded block is {computed}, game {game} on \
         L1 claims {claimed}"
    )]
    Mismatch {
        block: u64,
        game: Address,
        claimed: B256,
        computed: B256,
    },
}

impl GameAnchor {
    /// Checks that the downloaded block [`Self::l2_block`], given by its hash and three fields
    /// of its header, is the block the game claims. `withdrawals_root` is the header's, which
    /// from Isthmus on is the storage root of the message passer.
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError::Timestamp`] if the game names its block by a time the block does
    /// not have, [`AnchorError::NoStorageRoot`] if the header has no withdrawals root, and
    /// [`AnchorError::Mismatch`], with both roots, if the output roots differ.
    pub(crate) fn check(
        &self,
        hash: B256,
        timestamp: u64,
        state_root: B256,
        withdrawals_root: Option<B256>,
    ) -> Result<(), AnchorError> {
        let (block, game) = (self.l2_block, self.game);
        if let Some(claimed) = self.timestamp
            && claimed != timestamp
        {
            return Err(AnchorError::Timestamp {
                block,
                game,
                claimed,
                found: timestamp,
            });
        }
        let storage_root = withdrawals_root.ok_or(AnchorError::NoStorageRoot { block, game })?;
        // The version-0 output root.
        let computed = keccak256([B256::ZERO, state_root, storage_root, hash].concat());
        if computed != self.output_root {
            return Err(AnchorError::Mismatch {
                block,
                game,
                claimed: self.output_root,
                computed,
            });
        }
        Ok(())
    }
}
