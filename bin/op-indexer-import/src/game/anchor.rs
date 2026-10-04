//! The game that anchors the top of a range, and the check that the downloaded block is the
//! one it claims.
//!
//! Does not find the game (the parent module does) and does not read headers: the caller hands
//! the fields in.

use alloy_primitives::{Address, B256};
use op_indexer_primitives::{ClaimMismatch, check_claim};
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

/// A downloaded block is not the one a game claims.
#[derive(Debug, thiserror::Error)]
#[error("game {game} on L1 cannot anchor the range: {mismatch}")]
pub(crate) struct AnchorError {
    game: Address,
    #[source]
    mismatch: ClaimMismatch,
}

impl GameAnchor {
    /// Checks that the downloaded block [`Self::l2_block`], given by its hash and three fields
    /// of its header, is the block the game claims. `withdrawals_root` is the header's, which
    /// from Isthmus on (`isthmus_time`) is the storage root of the message passer.
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError`] with the [`ClaimMismatch`]: the game names its block by a time
    /// the block does not have, the block is before Isthmus, or the output roots differ (both
    /// are in the error).
    pub(crate) fn check(
        &self,
        hash: B256,
        (timestamp, isthmus_time): (u64, u64),
        state_root: B256,
        withdrawals_root: Option<B256>,
    ) -> Result<(), AnchorError> {
        check_claim(
            self.l2_block,
            (self.output_root, self.timestamp),
            hash,
            (timestamp, isthmus_time),
            (state_root, withdrawals_root),
        )
        .map_err(|mismatch| AnchorError {
            game: self.game,
            mismatch,
        })
    }
}
