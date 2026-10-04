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
//! claim is right. A game just created is a bonded claim nobody has challenged yet; with
//! [`GameConfig::resolved_only`] only a game resolved in the proposer's favour is used, which
//! is days older. The proposer proposes blocks that are already safe, so the block is committed
//! to L1 either way.
//!
//! Does not fetch: the L1 side is the [`L1Logs`] trait. Does not read headers: the caller hands
//! the fields in.
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
//!   rootClaim)`; a game emits `Resolved(GameStatus indexed status)`, where `DEFENDER_WINS` is 2
//!   ([dispute game interface]). `GameType` is a `uint32`, `Claim` a `bytes32`.
//! - **The extra data of a fault dispute game is its L2 block number**, one 32-byte word:
//!   `FaultDisputeGame.l2BlockNumber()` and `extraData()` in
//!   `packages/contracts-bedrock/src/dispute/FaultDisputeGame.sol` of the Optimism monorepo
//!   (read on its `develop` branch).
//! - **OP Mainnet's factory** is `0xe5965Ab5962eDc7477C8520243A95517CD252fA9`
//!   (`DisputeGameFactoryProxy` in `superchain/configs/mainnet/op.toml` of the superchain
//!   registry).
//!
//! [L2 output commitment construction]: https://specs.optimism.io/protocol/proposals.html#l2-output-commitment-construction
//! [`L2ToL1MessagePasser` storage root in header]: https://specs.optimism.io/protocol/isthmus/exec-engine.html#l2tol1messagepasser-storage-root-in-header
//! [dispute game interface]: https://specs.optimism.io/fault-proof/stage-one/dispute-game-interface.html

use std::collections::HashSet;
use std::future::Future;

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use serde::{Deserialize, Serialize};

/// `DisputeGameFactory.create`.
const CREATE_SIGNATURE: &str = "create(uint32,bytes32,bytes)";
/// The factory's event for a new game.
const CREATED_SIGNATURE: &str = "DisputeGameCreated(address,uint32,bytes32)";
/// A game's event when it is resolved.
const RESOLVED_SIGNATURE: &str = "Resolved(uint8)";
/// `GameStatus.DEFENDER_WINS`: the root claim stood.
const DEFENDER_WINS: u64 = 2;

/// An ABI word.
const WORD: usize = 32;
/// The calldata of `create` for a fault dispute game: the selector, the game type, the root
/// claim, the offset of the extra data, its length, and its one word.
const CREATE_CALLDATA_BYTES: usize = 4 + 5 * WORD;

/// L1 blocks searched back from the head for the newest game: a day of 12-second blocks.
/// OP Mainnet's proposer creates a game about every hour.
const NEWEST_LOOKBACK_BLOCKS: u64 = 7_200;
/// L1 blocks searched back for the newest resolved game: 30 days. A game resolves after its
/// clocks run out, 3.5 to 7 days after it was created.
const RESOLVED_LOOKBACK_BLOCKS: u64 = 216_000;

/// Which games of which factory anchor the range: chain configuration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GameConfig {
    /// The chain's `DisputeGameFactory` on L1.
    pub(crate) factory: Address,
    /// The game type whose games are used. It must be a fault dispute game, whose extra data
    /// is the L2 block number.
    pub(crate) game_type: u32,
    /// Use only a game resolved in the proposer's favour.
    pub(crate) resolved_only: bool,
}

/// A log on L1 with the transaction that emitted it.
#[derive(Debug, Clone)]
pub(crate) struct L1Log {
    pub(crate) block_number: u64,
    /// The log's index among all logs of its block.
    pub(crate) log_index: u64,
    /// The contract that emitted it.
    pub(crate) address: Address,
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

/// What the lookup needs from L1.
pub(crate) trait L1Logs {
    /// Why a request failed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the number of the newest L1 block the source has.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the source cannot be reached.
    fn height(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Returns every log matching `filter`, each with its transaction, following the source's
    /// pages to the head.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the source cannot be reached or an answer is malformed.
    fn logs(
        &self,
        filter: &LogFilter,
    ) -> impl Future<Output = Result<Vec<L1Log>, Self::Error>> + Send;
}

/// The game that anchors the top of the range: what `download` records and `verify` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GameAnchor {
    /// The game's contract on L1.
    pub(crate) game: Address,
    /// The L2 block the game is about: the last block of the range.
    pub(crate) l2_block: u64,
    /// The output root the game claims for that block.
    pub(crate) root_claim: B256,
    /// The L1 block the game was created in.
    pub(crate) l1_block: u64,
}

/// Why no game could be used.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GameError {
    #[error("the dispute games on L1 could not be read")]
    L1(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(
        "no {wanted} game of type {game_type} was created by {factory} in the last {blocks} L1 \
         blocks; give the last block of the range explicitly"
    )]
    NoGame {
        wanted: &'static str,
        game_type: u32,
        factory: Address,
        blocks: u64,
    },
    #[error(
        "game {game} (L1 block {l1_block}) was not created by a plain call of the factory's \
         `create` with a one-word extra data, so its L2 block cannot be read from the calldata; \
         give the last block of the range explicitly"
    )]
    UnreadableCreation { game: Address, l1_block: u64 },
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
    /// Checks that the downloaded block [`Self::l2_block`], given by its hash and two fields of
    /// its header, is the block the game claims. `withdrawals_root` is the header's, which from
    /// Isthmus on is the storage root of the message passer.
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError::NoStorageRoot`] if the header has no withdrawals root, and
    /// [`AnchorError::Mismatch`], with both roots, if the output roots differ.
    pub(crate) fn check(
        &self,
        hash: B256,
        state_root: B256,
        withdrawals_root: Option<B256>,
    ) -> Result<(), AnchorError> {
        let (block, game) = (self.l2_block, self.game);
        let storage_root = withdrawals_root.ok_or(AnchorError::NoStorageRoot { block, game })?;
        let computed = output_root(state_root, storage_root, hash);
        if computed != self.root_claim {
            return Err(AnchorError::Mismatch {
                block,
                game,
                claimed: self.root_claim,
                computed,
            });
        }
        Ok(())
    }
}

/// The version-0 output root of a block.
fn output_root(state_root: B256, storage_root: B256, hash: B256) -> B256 {
    keccak256([B256::ZERO, state_root, storage_root, hash].concat())
}

/// Finds the newest game of `config` on L1: the newest created, or with
/// [`GameConfig::resolved_only`] the newest that resolved in the proposer's favour.
///
/// # Errors
///
/// Returns [`GameError::L1`] if L1 cannot be read, [`GameError::NoGame`] if no such game was
/// created within the search window, and [`GameError::UnreadableCreation`] if the game's L2
/// block cannot be read from the transaction that created it.
pub(crate) async fn newest_game<S: L1Logs>(
    l1: &S,
    config: &GameConfig,
) -> Result<GameAnchor, GameError> {
    let read = |err: S::Error| GameError::L1(Box::new(err));
    let (wanted, blocks) = if config.resolved_only {
        ("resolved", RESOLVED_LOOKBACK_BLOCKS)
    } else {
        ("new", NEWEST_LOOKBACK_BLOCKS)
    };
    let from_block = l1.height().await.map_err(read)?.saturating_sub(blocks);
    let created = LogFilter {
        addresses: vec![config.factory],
        topic0: keccak256(CREATED_SIGNATURE),
        topic1: None,
        topic2: Some(word(u64::from(config.game_type))),
        from_block,
    };
    let games = l1.logs(&created).await.map_err(read)?;

    let resolved: Option<HashSet<Address>> = if config.resolved_only {
        let filter = LogFilter {
            addresses: games.iter().filter_map(game_address).collect(),
            topic0: keccak256(RESOLVED_SIGNATURE),
            topic1: Some(word(DEFENDER_WINS)),
            topic2: None,
            from_block,
        };
        let logs = l1.logs(&filter).await.map_err(read)?;
        Some(logs.into_iter().map(|log| log.address).collect())
    } else {
        None
    };
    let usable = |game: &Address| resolved.as_ref().is_none_or(|set| set.contains(game));

    let newest = games
        .iter()
        .filter_map(|log| game_address(log).filter(usable).map(|game| (game, log)))
        .max_by_key(|(_, log)| (log.block_number, log.log_index));
    let Some((game, log)) = newest else {
        return Err(GameError::NoGame {
            wanted,
            game_type: config.game_type,
            factory: config.factory,
            blocks,
        });
    };
    created_game(config, game, log).ok_or(GameError::UnreadableCreation {
        game,
        l1_block: log.block_number,
    })
}

/// The game a `DisputeGameCreated` log announces: the address in its first indexed field.
fn game_address(log: &L1Log) -> Option<Address> {
    let [_, Some(proxy), _, _] = log.topics else {
        return None;
    };
    Some(Address::from_word(proxy))
}

/// Reads the game's L2 block from the transaction that created it, which must be a plain call
/// of the factory's `create` for this game: same game type and root claim as the log, and an
/// extra data of one word. `None` for anything else (a game created through another
/// contract, or a game type with other extra data), rather than a block number misread.
fn created_game(config: &GameConfig, game: Address, log: &L1Log) -> Option<GameAnchor> {
    let [_, _, _, Some(root_claim)] = log.topics else {
        return None;
    };
    let input = log.transaction_input.as_ref();
    let selector = keccak256(CREATE_SIGNATURE);
    let plain_call = log.transaction_to == Some(config.factory)
        && input.len() == CREATE_CALLDATA_BYTES
        && input.get(..4) == selector.get(..4)
        && argument(input, 0)? == U256::from(config.game_type)
        && argument(input, 1)? == U256::from_be_bytes(root_claim.0)
        && argument(input, 2)? == U256::from(3 * WORD)
        && argument(input, 3)? == U256::from(WORD);
    if !plain_call {
        return None;
    }
    Some(GameAnchor {
        game,
        l2_block: u64::try_from(argument(input, 4)?).ok()?,
        root_claim,
        l1_block: log.block_number,
    })
}

/// The ABI word at position `index` after the selector.
fn argument(input: &[u8], index: usize) -> Option<U256> {
    let start = index.checked_mul(WORD)?.checked_add(4)?;
    let bytes: [u8; WORD] = input
        .get(start..start.checked_add(WORD)?)?
        .try_into()
        .ok()?;
    Some(U256::from_be_bytes(bytes))
}

/// `value` as an indexed event field: a 32-byte big-endian word.
fn word(value: u64) -> B256 {
    B256::from(U256::from(value))
}
