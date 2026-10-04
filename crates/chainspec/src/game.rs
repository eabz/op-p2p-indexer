//! Dispute games: how a game created by a chain's `DisputeGameFactory` on L1 states the L2
//! block it is about and the output root it claims for it.
//!
//! Pure decoding of data that was read from L1 by someone else (the importer through an
//! archive service, the `l1` crate from execution peers). Nothing here fetches or verifies
//! where the data came from.
//!
//! # Sources
//!
//! - `DisputeGameFactory.create(GameType, Claim rootClaim, bytes extraData)` emits
//!   `DisputeGameCreated(address indexed disputeProxy, GameType indexed gameType, Claim indexed
//!   rootClaim)` ([dispute game interface]); `GameType` is a `uint32`, `Claim` a `bytes32`.
//! - A fault dispute game's extra data is one 32-byte word, its L2 block number, and its root
//!   claim is that block's output root (`FaultDisputeGame.l2BlockNumber()`, `extraData()`).
//! - A super fault dispute game's extra data is the preimage of a super root, `0x01 ‖ uint64
//!   timestamp ‖ (uint256 chainId ‖ bytes32 outputRoot)` once per chain, and its root claim is
//!   the keccak of that preimage (`SuperFaultDisputeGame`, `Encoding.encodeSuperRootProof`).
//!   The claim for one chain is the output root next to its chain id, about its block at that
//!   timestamp.
//! - The game types are `GameTypes` in `dispute/lib/Types.sol`.
//!
//! All read in `packages/contracts-bedrock/src` of the Optimism monorepo, `develop` branch.
//!
//! [dispute game interface]: https://specs.optimism.io/fault-proof/stage-one/dispute-game-interface.html

use alloy_primitives::{Address, B256, BlockNumber, U256, keccak256};

use crate::ChainSpec;

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

/// How a dispute game on L1 states the L2 block it is about and the output root it claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimFormat {
    /// A fault dispute game: its extra data is one word, the L2 block number, and its root
    /// claim is that block's output root.
    OutputRoot,
    /// A super fault dispute game: its extra data is the preimage of a super root (a timestamp
    /// and one output root per chain) and its root claim is the hash of that preimage.
    SuperRoot,
}

/// What a game claims about one chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Claim {
    /// The L2 block the game is about.
    pub l2_block: BlockNumber,
    /// The output root claimed for that block.
    pub output_root: B256,
    /// The timestamp that block must have, when the game names its block by time (a super
    /// game).
    pub timestamp: Option<u64>,
}

/// Why a game's claim could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClaimError {
    /// The game is of a type whose claim format this build does not know.
    #[error("game type {0} is not known to this build")]
    UnknownGameType(u32),
    /// The transaction is not a plain call of the factory's `create` for this game.
    #[error("it was not created by a plain call of the factory's `create` for this game")]
    NotPlainCall,
    /// The extra data is not of the game type's format.
    #[error("its extra data is not what a game of its type carries")]
    ExtraData,
    /// The super root holds no output root for the chain.
    #[error("its super root has no entry for chain {0}")]
    ChainMissing(u64),
    /// The super root's timestamp is before the chain's Bedrock block.
    #[error("its timestamp is before the chain's Bedrock block")]
    BeforeBedrock,
}

/// The claim format of the dispute games of `game_type`, or `None` for a type this build does
/// not know how to read.
///
/// Game types are the same on every OP Stack chain. The fault dispute games are `CANNON` (0),
/// `PERMISSIONED_CANNON` (1), `ASTERISC` (2), `ASTERISC_KONA` (3) and `CANNON_KONA` (8); the
/// super ones are `SUPER_CANNON` (4), `SUPER_PERMISSIONED` (5), `SUPER_ASTERISC_KONA` (7) and
/// `SUPER_CANNON_KONA` (9), which OP Mainnet creates as of 2026-10. The validity-proof games
/// (6, 10) and the test games are not listed: their claims were not read.
const fn claim_format(game_type: u32) -> Option<ClaimFormat> {
    match game_type {
        0..=3 | 8 => Some(ClaimFormat::OutputRoot),
        4 | 5 | 7 | 9 => Some(ClaimFormat::SuperRoot),
        _ => None,
    }
}

/// Topic 0 of the factory's `DisputeGameCreated` event.
#[must_use]
pub fn created_topic() -> B256 {
    keccak256(CREATED_SIGNATURE)
}

/// A `DisputeGameCreated` event, read from its four topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedGame {
    /// The game's contract.
    pub game: Address,
    /// The game's type.
    pub game_type: u32,
    /// The game's root claim.
    pub root_claim: B256,
}

impl CreatedGame {
    /// Reads the event from a log's topics: the event's signature and its three indexed
    /// fields. `None` if they are not those of `DisputeGameCreated`.
    #[must_use]
    pub fn from_topics(topics: &[B256]) -> Option<Self> {
        let [signature, proxy, game_type, root_claim] = topics else {
            return None;
        };
        if *signature != created_topic() {
            return None;
        }
        Some(Self {
            game: Address::from_word(*proxy),
            game_type: u32::try_from(U256::from_be_bytes(game_type.0)).ok()?,
            root_claim: *root_claim,
        })
    }
}

impl ChainSpec {
    /// Reads what `game` claims about this chain from the transaction that created it: `to`
    /// is the address it was sent to and `input` its calldata.
    ///
    /// The transaction must be a plain call of this chain's factory's `create` for this game:
    /// sent to the factory, with the game type and root claim of the event. Anything else (a
    /// game created through another contract, or extra data that is not of the type's format)
    /// is an error rather than a block number misread.
    ///
    /// # Errors
    ///
    /// Returns [`ClaimError`]: the game type is not known, the transaction is not that call,
    /// or its extra data is not what the type carries.
    pub fn game_claim(
        &self,
        game: &CreatedGame,
        to: Option<Address>,
        input: &[u8],
    ) -> Result<Claim, ClaimError> {
        let CreatedGame {
            game_type,
            root_claim,
            ..
        } = *game;
        let format = claim_format(game_type).ok_or(ClaimError::UnknownGameType(game_type))?;
        let selector = keccak256(CREATE_SIGNATURE);
        let plain_call = to == Some(self.dispute_game_factory)
            && input.get(..4) == selector.get(..4)
            && argument(input, 0) == Some(U256::from(game_type))
            && argument(input, 1) == Some(U256::from_be_bytes(root_claim.0))
            && argument(input, 2) == Some(U256::from(3 * WORD));
        if !plain_call {
            return Err(ClaimError::NotPlainCall);
        }
        let extra = argument(input, 3)
            .and_then(|length| usize::try_from(length).ok())
            .and_then(|length| input.get(CREATE_HEAD_BYTES..)?.get(..length))
            .ok_or(ClaimError::NotPlainCall)?;
        match format {
            // The block number, and the root claim is its output root.
            ClaimFormat::OutputRoot => {
                let l2_block = <[u8; WORD]>::try_from(extra)
                    .ok()
                    .and_then(|word| u64::try_from(U256::from_be_bytes(word)).ok())
                    .ok_or(ClaimError::ExtraData)?;
                Ok(Claim {
                    l2_block,
                    output_root: root_claim,
                    timestamp: None,
                })
            }
            ClaimFormat::SuperRoot => {
                let (at, output_root) = super_root_claim(extra, root_claim, self.chain_id)?;
                let elapsed = at
                    .checked_sub(self.bedrock_time)
                    .ok_or(ClaimError::BeforeBedrock)?;
                let blocks = self.blocks_in(elapsed);
                // The chain's block at a time is its last block not after it.
                let timestamp = blocks
                    .saturating_mul(self.block_time_secs)
                    .saturating_add(self.bedrock_time);
                Ok(Claim {
                    l2_block: self.bedrock_block.saturating_add(blocks),
                    output_root,
                    timestamp: Some(timestamp),
                })
            }
        }
    }
}

/// Reads a super root's preimage, which must hash to `root_claim`: its timestamp and the
/// output root it holds for `chain_id`.
fn super_root_claim(
    preimage: &[u8],
    root_claim: B256,
    chain_id: u64,
) -> Result<(u64, B256), ClaimError> {
    let (head, chains) = preimage
        .split_at_checked(SUPER_ROOT_HEAD_BYTES)
        .ok_or(ClaimError::ExtraData)?;
    let (entries, rest) = chains.as_chunks::<{ 2 * WORD }>();
    let Some((&SUPER_ROOT_VERSION, timestamp)) = head.split_first() else {
        return Err(ClaimError::ExtraData);
    };
    let timestamp = <[u8; 8]>::try_from(timestamp).map_err(|_length| ClaimError::ExtraData)?;
    if entries.is_empty() || !rest.is_empty() || keccak256(preimage) != root_claim {
        return Err(ClaimError::ExtraData);
    }
    let wanted = B256::from(U256::from(chain_id));
    entries
        .iter()
        .find_map(|entry| {
            let (chain, root) = entry.split_at_checked(WORD)?;
            (chain == wanted.as_slice()).then(|| B256::try_from(root).ok())?
        })
        .map(|root| (u64::from_be_bytes(timestamp), root))
        .ok_or(ClaimError::ChainMissing(chain_id))
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
