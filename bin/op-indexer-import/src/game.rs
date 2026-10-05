//! The top anchor of a range that reaches the present: the L2 block claimed by a dispute game
//! on L1, and the check that the downloaded block is the one claimed.
//!
//! The archive service's newest blocks are not yet committed to L1, and nothing in a downloaded
//! range proves that its top is the canonical chain: parent hashes only prove that the range
//! is one chain. A dispute game is a claim, posted on L1 by the chain's proposer, about the L2
//! chain at one block: its root claim is that block's output root. The import therefore ends
//! at the block of the newest game, and `verify` requires the output root computed from that
//! block's downloaded header to equal the claim. With every parent hash checked below it, the
//! whole range is then the chain claimed on L1.
//!
//! What this proves: the range is the chain a game on L1 claims. What it does not: that the
//! claim is right: a game just created is a bonded claim nobody has challenged yet. The
//! proposer proposes blocks that are already safe, so the block is committed to L1 either way.
//! Which game type the chain's portal respects is set on L1 and cannot be read through logs:
//! the newest game the factory created is taken, whatever its type.
//!
//! Does not fetch: the queries are the source's. The check itself is in `anchor`.
//!
//! # Rules and their sources
//!
//! - **Output root**: `keccak256(version ‖ state_root ‖ withdrawal_storage_root ‖
//!   latest_block_hash)` with version `bytes32(0)` ([L2 output commitment construction]).
//! - **The withdrawal storage root is in the header from Isthmus on**, as its withdrawals root
//!   ([`L2ToL1MessagePasser` storage root in header]). Before Isthmus it is not in the header,
//!   so the output root of such a block cannot be computed from what is downloaded.
//! - **Games**: `DisputeGameFactory.create(GameType, Claim rootClaim, bytes extraData)` emits
//!   `DisputeGameCreated(address indexed disputeProxy, GameType indexed gameType, Claim indexed
//!   rootClaim)` ([dispute game interface]). `GameType` is a `uint32`, `Claim` a `bytes32`.
//! - **A game says which L2 block it is about in its extra data**, in a form set by its game
//!   type, per chain, in `op-indexer-chainspec` (read through `ChainSpec::game_claim`); a game
//!   of a type the chain does not list is refused by name, never guessed from the shape of its
//!   data:
//!   - *Fault dispute games* (types 0, 1, 8, ...): one 32-byte word, the L2 block number, and
//!     the root claim is that block's output root (`FaultDisputeGame.l2BlockNumber()` and
//!     `extraData()`).
//!   - *Super fault dispute games* (types 4, 5, 7, 9, ...; OP Mainnet creates type 9 as of
//!     2026-10): the preimage of a super root, `0x01 ‖ uint64 timestamp ‖ (uint256 chainId ‖
//!     bytes32 outputRoot)` once per chain, and the root claim is its keccak
//!     (`SuperFaultDisputeGame`, which rejects a game whose extra data does not hash to its
//!     root claim; `Encoding.encodeSuperRootProof`). The claim for this chain is the output
//!     root next to its chain id, about its block at that timestamp.
//!   - *Aggregate games* (type 621, Base's `AggregateVerifier`, created with
//!     `createWithInitData`): the L2 block number, the parent game and intermediate roots, and
//!     the root claim is that block's output root (`docs/base.md` section 4).
//!
//!   Both read in `packages/contracts-bedrock/src` of the Optimism monorepo, `develop` branch
//!   (`dispute/FaultDisputeGame.sol`, `dispute/SuperFaultDisputeGame.sol`,
//!   `libraries/Encoding.sol`, `dispute/DisputeGameFactory.sol`, `dispute/lib/Types.sol`).
//! - **OP Mainnet's factory** is `0xe5965Ab5962eDc7477C8520243A95517CD252fA9`
//!   (`DisputeGameFactoryProxy` in `superchain/configs/mainnet/op.toml` of the superchain
//!   registry).
//!
//! [L2 output commitment construction]: https://specs.optimism.io/protocol/proposals.html#l2-output-commitment-construction
//! [`L2ToL1MessagePasser` storage root in header]: https://specs.optimism.io/protocol/isthmus/exec-engine.html#l2tol1messagepasser-storage-root-in-header
//! [dispute game interface]: https://specs.optimism.io/fault-proof/stage-one/dispute-game-interface.html

mod anchor;

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, Bytes};
use op_indexer_chainspec::{ChainSpec, ClaimError, CreatedGame, created_topic};

pub(crate) use self::anchor::GameAnchor;
use crate::source::{HyperSync, SourceError};

/// L1 blocks searched back from the head for the newest game: a day of 12-second blocks.
/// OP Mainnet's proposer creates a game every hour or few.
const LOOKBACK_BLOCKS: u64 = 7_200;

/// A log on L1 with the transaction that emitted it.
#[derive(Debug, Clone)]
pub(crate) struct L1Log {
    pub(crate) block_number: u64,
    /// The log's index among all logs of its block.
    pub(crate) log_index: u64,
    pub(crate) topics: [Option<B256>; 4],
    /// The address the transaction was sent to; `None` for a contract creation.
    pub(crate) transaction_to: Option<Address>,
    /// The transaction's calldata.
    pub(crate) transaction_input: Bytes,
}

/// The logs to fetch.
#[derive(Debug, Clone)]
pub(crate) struct LogFilter {
    /// Emitted by any of these contracts.
    pub(crate) addresses: Vec<Address>,
    pub(crate) topic0: B256,
    pub(crate) topic1: Option<B256>,
    pub(crate) topic2: Option<B256>,
    /// From this L1 block to the head.
    pub(crate) from_block: u64,
}

/// Why no game could be used.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GameError {
    #[error("the dispute games on L1 could not be read")]
    L1(#[source] SourceError),
    #[error(
        "{factory} created no dispute game in the last {LOOKBACK_BLOCKS} L1 blocks; give the \
         last block of the range explicitly"
    )]
    NoGame { factory: Address },
    #[error(
        "the newest dispute game, {game} (L1 block {l1_block}), is of type {game_type}, which \
         this build cannot read (types created in the last {LOOKBACK_BLOCKS} L1 blocks, and how \
         many games: {seen}); give the last block of the range explicitly"
    )]
    UnknownGameType {
        game: Address,
        game_type: u32,
        l1_block: u64,
        /// E.g. "9 (5)".
        seen: String,
    },
    #[error(
        "game {game} of type {game_type} (L1 block {l1_block}) cannot be read: {reason}; give \
         the last block of the range explicitly"
    )]
    UnreadableCreation {
        game: Address,
        game_type: u32,
        l1_block: u64,
        reason: ClaimError,
    },
}

/// Finds the newest game `chain`'s factory created on L1, whatever its type.
///
/// # Errors
///
/// Returns [`GameError::L1`] if L1 cannot be read, [`GameError::NoGame`] if the factory
/// created no game within the search window, [`GameError::UnknownGameType`] if the newest
/// game is of a type whose claim this build cannot read, and
/// [`GameError::UnreadableCreation`] if its L2 block cannot be read from the transaction that
/// created it.
pub(crate) async fn newest_game(
    l1: &HyperSync,
    chain: &ChainSpec,
) -> Result<GameAnchor, GameError> {
    let factory = chain.dispute_game_factory;
    let from_block = l1
        .height()
        .await
        .map_err(GameError::L1)?
        .saturating_sub(LOOKBACK_BLOCKS);
    // Every type is fetched, so the error can say which types the chain uses.
    let created = LogFilter {
        addresses: vec![factory],
        topic0: created_topic(),
        topic1: None,
        topic2: None,
        from_block,
    };
    let logs = l1.logs(&created).await.map_err(GameError::L1)?;
    let games: Vec<(CreatedGame, &L1Log)> = logs
        .iter()
        .filter_map(|log| {
            let topics: Vec<B256> = log.topics.iter().copied().flatten().collect();
            Some((CreatedGame::from_topics(&topics)?, log))
        })
        .collect();
    let (created, log) = games
        .iter()
        .max_by_key(|(_, log)| (log.block_number, log.log_index))
        .ok_or(GameError::NoGame { factory })?;
    let (game, game_type, l1_block) = (created.game, created.game_type, log.block_number);
    match chain.game_claim(created, log.transaction_to, &log.transaction_input) {
        Ok(claim) => Ok(GameAnchor {
            game,
            game_type,
            l2_block: claim.l2_block,
            output_root: claim.output_root,
            timestamp: claim.timestamp,
            l1_block,
        }),
        Err(ClaimError::UnknownGameType(_)) => Err(GameError::UnknownGameType {
            game,
            game_type,
            l1_block,
            seen: types_seen(&games),
        }),
        Err(
            reason @ (ClaimError::NotPlainCall
            | ClaimError::ExtraData
            | ClaimError::ChainMissing(_)
            | ClaimError::BeforeBedrock),
        ) => Err(GameError::UnreadableCreation {
            game,
            game_type,
            l1_block,
            reason,
        }),
    }
}

/// The game types among `games` with how many games each has, for an error message.
fn types_seen(games: &[(CreatedGame, &L1Log)]) -> String {
    let mut counts = BTreeMap::<u32, usize>::new();
    for (game, _) in games {
        *counts.entry(game.game_type).or_default() += 1;
    }
    let seen: Vec<String> = counts
        .iter()
        .map(|(game_type, games)| format!("{game_type} ({games})"))
        .collect();
    seen.join(", ")
}
