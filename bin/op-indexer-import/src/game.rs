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
//! - **A game says which L2 block it is about in its extra data**, in one of two forms. Which
//!   one a game type uses is a table in `op-indexer-chainspec` (`claim_format`); a game of a
//!   type that is not in it is refused by name, never guessed from the shape of its data:
//!   - *Fault dispute games* (types 0, 1, 8, ...): one 32-byte word, the L2 block number, and
//!     the root claim is that block's output root (`FaultDisputeGame.l2BlockNumber()` and
//!     `extraData()`).
//!   - *Super fault dispute games* (types 4, 5, 7, 9, ...; OP Mainnet creates type 9 as of
//!     2026-10): the preimage of a super root, `0x01 ‖ uint64 timestamp ‖ (uint256 chainId ‖
//!     bytes32 outputRoot)` once per chain, and the root claim is its keccak
//!     (`SuperFaultDisputeGame`, which rejects a game whose extra data does not hash to its
//!     root claim; `Encoding.encodeSuperRootProof`). The claim for this chain is the output
//!     root next to its chain id, about its block at that timestamp.
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

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use op_indexer_chainspec::{ChainSpec, ClaimFormat, claim_format};

pub(crate) use self::anchor::GameAnchor;
use crate::source::{HyperSync, SourceError};

/// `DisputeGameFactory.create`.
const CREATE_SIGNATURE: &str = "create(uint32,bytes32,bytes)";
/// The factory's event for a new game.
const CREATED_SIGNATURE: &str = "DisputeGameCreated(address,uint32,bytes32)";

/// An ABI word.
const WORD: usize = 32;
/// Bytes of `create`'s calldata before the extra data: the selector, the game type, the root
/// claim, the offset of the extra data and its length.
const CREATE_HEAD_BYTES: usize = 4 + 4 * WORD;
/// The version byte of a super root's preimage.
const SUPER_ROOT_VERSION: u8 = 1;
/// Bytes of a super root's preimage before its chains: the version and the timestamp.
const SUPER_ROOT_HEAD_BYTES: usize = 1 + 8;

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
        reason: Unreadable,
    },
}

/// Why a game's L2 block could not be read from the transaction that created it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Unreadable {
    #[error("it was not created by a plain call of the factory's `create` for this game")]
    NotPlainCall,
    #[error("its extra data is not what a game of its type carries")]
    ExtraData,
    #[error("its super root has no entry for chain {0}")]
    ChainMissing(u64),
    #[error("its timestamp is before the chain's Bedrock block")]
    BeforeBedrock,
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
        topic0: keccak256(CREATED_SIGNATURE),
        topic1: None,
        topic2: None,
        from_block,
    };
    let logs = l1.logs(&created).await.map_err(GameError::L1)?;
    let games: Vec<Created<'_>> = logs.iter().filter_map(Created::from_log).collect();
    let newest = games
        .iter()
        .max_by_key(|game| (game.log.block_number, game.log.log_index))
        .ok_or(GameError::NoGame { factory })?;
    let (game, game_type, l1_block) = (newest.address, newest.game_type, newest.log.block_number);
    let format = claim_format(game_type).ok_or_else(|| GameError::UnknownGameType {
        game,
        game_type,
        l1_block,
        seen: types_seen(&games),
    })?;
    newest
        .anchor(chain, format)
        .map_err(|reason| GameError::UnreadableCreation {
            game,
            game_type,
            l1_block,
            reason,
        })
}

/// A `DisputeGameCreated` log, with its indexed fields read.
#[derive(Debug, Clone, Copy)]
struct Created<'a> {
    address: Address,
    game_type: u32,
    root_claim: B256,
    log: &'a L1Log,
}

impl<'a> Created<'a> {
    /// Reads the event's three indexed fields: the game, its type and its root claim.
    fn from_log(log: &'a L1Log) -> Option<Self> {
        let [_, Some(proxy), Some(game_type), Some(root_claim)] = log.topics else {
            return None;
        };
        Some(Self {
            address: Address::from_word(proxy),
            game_type: u32::try_from(U256::from_be_bytes(game_type.0)).ok()?,
            root_claim,
            log,
        })
    }

    /// Reads the game's L2 block and output root from the transaction that created it, which
    /// must be a plain call of the factory's `create` for this game: same game type and root
    /// claim as the log. Anything else (a game created through another contract, or extra data
    /// that is not of `format`) is an error rather than a block number misread.
    fn anchor(&self, chain: &ChainSpec, format: ClaimFormat) -> Result<GameAnchor, Unreadable> {
        let input = self.log.transaction_input.as_ref();
        let selector = keccak256(CREATE_SIGNATURE);
        let length = argument(input, 3).and_then(|length| usize::try_from(length).ok());
        let plain_call = self.log.transaction_to == Some(chain.dispute_game_factory)
            && input.get(..4) == selector.get(..4)
            && argument(input, 0) == Some(U256::from(self.game_type))
            && argument(input, 1) == Some(U256::from_be_bytes(self.root_claim.0))
            && argument(input, 2) == Some(U256::from(3 * WORD));
        let extra = length
            .filter(|_| plain_call)
            .and_then(|length| input.get(CREATE_HEAD_BYTES..)?.get(..length))
            .ok_or(Unreadable::NotPlainCall)?;

        let (l2_block, output_root, timestamp) = match format {
            // The block number, and the root claim is its output root.
            ClaimFormat::OutputRoot => {
                let number = <[u8; WORD]>::try_from(extra)
                    .ok()
                    .and_then(|word| u64::try_from(U256::from_be_bytes(word)).ok())
                    .ok_or(Unreadable::ExtraData)?;
                (number, self.root_claim, None)
            }
            ClaimFormat::SuperRoot => {
                let (at, root) = super_root_claim(extra, self.root_claim, chain.chain_id)?;
                let blocks = at
                    .checked_sub(chain.bedrock_time)
                    .and_then(|elapsed| elapsed.checked_div(chain.block_time_secs))
                    .ok_or(Unreadable::BeforeBedrock)?;
                // The chain's block at a time is its last block not after it.
                let timestamp = blocks
                    .saturating_mul(chain.block_time_secs)
                    .saturating_add(chain.bedrock_time);
                (
                    chain.bedrock_block.saturating_add(blocks),
                    root,
                    Some(timestamp),
                )
            }
        };
        Ok(GameAnchor {
            game: self.address,
            game_type: self.game_type,
            l2_block,
            output_root,
            timestamp,
            l1_block: self.log.block_number,
        })
    }
}

/// Reads a super root's preimage, which must hash to `root_claim`: its timestamp and the
/// output root it holds for `chain_id`.
fn super_root_claim(
    preimage: &[u8],
    root_claim: B256,
    chain_id: u64,
) -> Result<(u64, B256), Unreadable> {
    let (head, chains) = preimage
        .split_at_checked(SUPER_ROOT_HEAD_BYTES)
        .ok_or(Unreadable::ExtraData)?;
    let (entries, rest) = chains.as_chunks::<{ 2 * WORD }>();
    let Some((&SUPER_ROOT_VERSION, timestamp)) = head.split_first() else {
        return Err(Unreadable::ExtraData);
    };
    let timestamp = <[u8; 8]>::try_from(timestamp).map_err(|_length| Unreadable::ExtraData)?;
    if entries.is_empty() || !rest.is_empty() || keccak256(preimage) != root_claim {
        return Err(Unreadable::ExtraData);
    }
    let wanted = B256::from(U256::from(chain_id));
    entries
        .iter()
        .find_map(|entry| {
            let (chain, root) = entry.split_at_checked(WORD)?;
            (chain == wanted.as_slice()).then(|| B256::try_from(root).ok())?
        })
        .map(|root| (u64::from_be_bytes(timestamp), root))
        .ok_or(Unreadable::ChainMissing(chain_id))
}

/// The game types among `games` with how many games each has, for an error message.
fn types_seen(games: &[Created<'_>]) -> String {
    let mut counts = BTreeMap::<u32, usize>::new();
    for game in games {
        *counts.entry(game.game_type).or_default() += 1;
    }
    let seen: Vec<String> = counts
        .iter()
        .map(|(game_type, games)| format!("{game_type} ({games})"))
        .collect();
    seen.join(", ")
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
