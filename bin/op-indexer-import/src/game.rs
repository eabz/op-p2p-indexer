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
//! to L1 either way. Which game type the chain's portal respects is set on L1 and cannot be
//! read through logs; by default the newest game of any type is taken, and the type can be
//! fixed with [`GameConfig::game_type`].
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
//! - **A game says which L2 block it is about in its extra data**, in one of two forms, told
//!   apart by their shape and not by the game type's number:
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

use std::collections::{BTreeMap, HashSet};
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
/// Bytes of `create`'s calldata before the extra data: the selector, the game type, the root
/// claim, the offset of the extra data and its length.
const CREATE_HEAD_BYTES: usize = 4 + 4 * WORD;
/// The version byte of a super root's preimage.
const SUPER_ROOT_VERSION: u8 = 1;
/// Bytes of a super root's preimage before its chains: the version and the timestamp.
const SUPER_ROOT_HEAD_BYTES: usize = 1 + 8;

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
    /// Use only games of this type; `None` for the newest game of any type.
    pub(crate) game_type: Option<u32>,
    /// Use only a game resolved in the proposer's favour.
    pub(crate) resolved_only: bool,
    /// The L2 chain, to find its claim in a super root and its block at a timestamp.
    pub(crate) l2: L2Chain,
}

/// What locates an L2 block from a timestamp, and the chain in a super root.
#[derive(Debug, Clone, Copy)]
pub(crate) struct L2Chain {
    pub(crate) chain_id: u64,
    /// A block of the chain since which every block is `block_time_secs` after its parent
    /// (OP Mainnet: the Bedrock block), and its timestamp.
    pub(crate) genesis_number: u64,
    pub(crate) genesis_time: u64,
    /// Never zero.
    pub(crate) block_time_secs: u64,
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

/// Why no game could be used.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GameError {
    #[error("the dispute games on L1 could not be read")]
    L1(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(
        "no usable {wanted} game{of_type} was created by {factory} in the last {blocks} L1 \
         blocks. Games created there, by type: {seen}. Pass `--game-type <n>` for a type that \
         has games, or give the last block of the range explicitly"
    )]
    NoGame {
        wanted: &'static str,
        /// " of type N" when a type was asked for, else empty.
        of_type: String,
        factory: Address,
        blocks: u64,
        /// The types created in the window with how many games each, e.g. "9 (5 games)";
        /// "none" if the factory created no game at all.
        seen: String,
    },
    #[error(
        "game {game} of type {game_type} (L1 block {l1_block}) cannot be read: {reason}; give \
         the last block of the range explicitly, or another `--game-type`"
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
    #[error("its extra data is neither an L2 block number nor the preimage of its root claim")]
    ExtraData,
    #[error("its super root has no entry for chain {0}")]
    ChainMissing(u64),
    #[error("its timestamp is before the chain's first block")]
    BeforeGenesis,
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
         the chain's genesis block, genesis time or block time is configured wrongly"
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
        let computed = output_root(state_root, storage_root, hash);
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

/// The version-0 output root of a block.
fn output_root(state_root: B256, storage_root: B256, hash: B256) -> B256 {
    keccak256([B256::ZERO, state_root, storage_root, hash].concat())
}

/// Finds the newest game of `config` on L1: the newest created, or with
/// [`GameConfig::resolved_only`] the newest that resolved in the proposer's favour. Without a
/// game type the newest game whose L2 block can be read is taken, whatever its type.
///
/// # Errors
///
/// Returns [`GameError::L1`] if L1 cannot be read, [`GameError::NoGame`], listing the types
/// that were created, if no such game was created within the search window, and
/// [`GameError::UnreadableCreation`] if a game type was asked for and the L2 block of its
/// newest game cannot be read.
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
    // Every type is fetched, so a wrong type can be told from a chain without games.
    let created = LogFilter {
        addresses: vec![config.factory],
        topic0: keccak256(CREATED_SIGNATURE),
        topic1: None,
        topic2: None,
        from_block,
    };
    let mut games: Vec<Created<'_>> = Vec::new();
    let logs = l1.logs(&created).await.map_err(read)?;
    games.extend(logs.iter().filter_map(Created::from_log));
    games.sort_unstable_by_key(|game| {
        std::cmp::Reverse((game.log.block_number, game.log.log_index))
    });

    let resolved: Option<HashSet<Address>> = if config.resolved_only {
        let filter = LogFilter {
            addresses: games.iter().map(|game| game.address).collect(),
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

    // Newest first.
    let candidates = games.iter().filter(|game| {
        config
            .game_type
            .is_none_or(|wanted| wanted == game.game_type)
            && resolved
                .as_ref()
                .is_none_or(|set| set.contains(&game.address))
    });
    for game in candidates {
        match game.anchor(config) {
            Ok(anchor) => return Ok(anchor),
            // A type was asked for: its newest game must be readable.
            Err(reason) if config.game_type.is_some() => {
                return Err(GameError::UnreadableCreation {
                    game: game.address,
                    game_type: game.game_type,
                    l1_block: game.log.block_number,
                    reason,
                });
            }
            // Any type: a kind of game this tool cannot read is passed over.
            Err(_unreadable) => {}
        }
    }
    Err(GameError::NoGame {
        wanted,
        of_type: config
            .game_type
            .map_or_else(String::new, |game_type| format!(" of type {game_type}")),
        factory: config.factory,
        blocks,
        seen: types_seen(&games),
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
    /// of another form) is an error rather than a block number misread.
    fn anchor(&self, config: &GameConfig) -> Result<GameAnchor, Unreadable> {
        let input = self.log.transaction_input.as_ref();
        let selector = keccak256(CREATE_SIGNATURE);
        let length = argument(input, 3).and_then(|length| usize::try_from(length).ok());
        let plain_call = self.log.transaction_to == Some(config.factory)
            && input.get(..4) == selector.get(..4)
            && argument(input, 0) == Some(U256::from(self.game_type))
            && argument(input, 1) == Some(U256::from_be_bytes(self.root_claim.0))
            && argument(input, 2) == Some(U256::from(3 * WORD));
        let extra = length
            .filter(|_| plain_call)
            .and_then(|length| input.get(CREATE_HEAD_BYTES..)?.get(..length))
            .ok_or(Unreadable::NotPlainCall)?;

        let (l2_block, output_root, timestamp) = if let Ok(number) = <[u8; WORD]>::try_from(extra) {
            // A fault dispute game: the block number, and the root claim is its output root.
            let number = u64::try_from(U256::from_be_bytes(number));
            let number = number.map_err(|_overflow| Unreadable::ExtraData)?;
            (number, self.root_claim, None)
        } else {
            let (at, root) = super_root_claim(extra, self.root_claim, config.l2.chain_id)?;
            let l2 = config.l2;
            let blocks = at
                .checked_sub(l2.genesis_time)
                .and_then(|elapsed| elapsed.checked_div(l2.block_time_secs))
                .ok_or(Unreadable::BeforeGenesis)?;
            // The chain's block at a time is its last block not after it.
            let timestamp = blocks
                .saturating_mul(l2.block_time_secs)
                .saturating_add(l2.genesis_time);
            (
                l2.genesis_number.saturating_add(blocks),
                root,
                Some(timestamp),
            )
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
    let wanted = word(chain_id);
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
    if counts.is_empty() {
        return "none".to_owned();
    }
    let seen: Vec<String> = counts
        .iter()
        .map(|(game_type, games)| format!("{game_type} ({games} games)"))
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

/// `value` as an indexed event field: a 32-byte big-endian word.
fn word(value: u64) -> B256 {
    B256::from(U256::from(value))
}
