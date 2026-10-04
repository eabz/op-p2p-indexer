//! Verification of light-client data: what makes an L1 block hash trusted.
//!
//! A [`Store`] starts from a bootstrap whose header hashes to the configured checkpoint, and
//! from then on accepts a header only if the sync committee it knows signed it. Follows the
//! light-client [sync protocol]:
//!
//! - **Bootstrap** ([`initialize_light_client_store`]): the header's root is the trusted
//!   root, and the sync committee is proven against the header's state root.
//! - **Every update** ([`validate_light_client_update`]): the slots are ordered (now ≥
//!   signature slot > attested slot ≥ finalized slot), the signature slot is in the period
//!   of a committee the store knows, the attested and finalized headers each carry an
//!   execution payload header proven against their body root, the finalized header is proven
//!   against the attested state root, the next sync committee (when sent) is proven against
//!   it too, and the committee's aggregate BLS signature over the attested header verifies.
//! - **Applying** ([`process_light_client_update`]): the finalized header moves forward only
//!   when at least two thirds of the committee signed. The head is held to the same
//!   threshold, which is stricter than the specification's safety threshold (half of the
//!   recent best participation): a header signed by fewer is not passed on.
//! - **Rotation**: when the finalized header enters the next period, the next committee
//!   becomes the current one; it must have been learned from an update before.
//!
//! The generalized indices are Electra's (finalized root 169, current sync committee 86, next
//! sync committee 87) and Capella's (execution payload 25). Pure: no I/O, no clock. The
//! signature check is CPU work of about ten milliseconds; callers run it off the runtime.
//!
//! [sync protocol]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md
//! [`initialize_light_client_store`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#initialize_light_client_store
//! [`validate_light_client_update`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#validate_light_client_update
//! [`process_light_client_update`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#process_light_client_update

use std::sync::Arc;

use alloy_primitives::B256;
use blst::BLST_ERROR;
use blst::min_pk::{PublicKey, Signature};
use ssz::Decode;
use tree_hash::TreeHash;

use super::rpc::StatusData;
use super::spec::{BeaconSpec, SLOTS_PER_EPOCH, SLOTS_PER_PERIOD, hash_pair};
use super::types::{
    BeaconBlockHeader, LightClientBootstrap, LightClientFinalityUpdate, LightClientHeader,
    LightClientOptimisticUpdate, LightClientUpdate, SyncAggregate, SyncCommittee,
};
use crate::TrustedL1Block;

/// Generalized index of the execution payload in a beacon block body (Capella).
const EXECUTION_PAYLOAD_GINDEX: u64 = 25;
/// Generalized index of the finalized checkpoint's root in a beacon state (Electra).
const FINALIZED_ROOT_GINDEX: u64 = 169;
/// Generalized index of the current sync committee in a beacon state (Electra).
const CURRENT_SYNC_COMMITTEE_GINDEX: u64 = 86;
/// Generalized index of the next sync committee in a beacon state (Electra).
const NEXT_SYNC_COMMITTEE_GINDEX: u64 = 87;
/// Members of a sync committee.
const SYNC_COMMITTEE_SIZE: usize = 512;
/// The signature scheme's domain separation tag for Ethereum's proof-of-possession scheme.
const BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

/// Why light-client data was refused. Every variant is the sending peer's fault except
/// [`Self::UnknownPeriod`] and [`Self::NotYetValid`], which an honest peer can cause.
#[derive(Debug, thiserror::Error)]
pub(super) enum VerifyError {
    #[error("the data does not decode: {0}")]
    Decode(String),
    #[error("the bootstrap header's root is {got}, the checkpoint is {wanted}")]
    WrongCheckpoint { wanted: B256, got: B256 },
    #[error("slot {slot} is before the fork whose containers this build reads")]
    BeforeElectra { slot: u64 },
    #[error("the {0} is not proven by its merkle branch")]
    Branch(&'static str),
    #[error(
        "slots are out of order: signature {signature}, attested {attested}, finalized {finalized}"
    )]
    SlotOrder {
        signature: u64,
        attested: u64,
        finalized: u64,
    },
    #[error("the update is signed at slot {signature_slot}, which has not come yet (now {now})")]
    NotYetValid { signature_slot: u64, now: u64 },
    #[error("the update is signed in period {signature}, the store knows the committee of {store}")]
    UnknownPeriod { signature: u64, store: u64 },
    #[error("only {0} of 512 sync-committee members signed")]
    Participation(usize),
    #[error("a sync-committee public key or the signature is not a valid curve point")]
    Point,
    #[error("the sync-committee signature does not verify")]
    Signature,
}

impl VerifyError {
    /// Whether the peer that sent the data misbehaved, as opposed to being ahead of us or of
    /// our clock.
    pub(super) const fn is_peer_fault(&self) -> bool {
        !matches!(self, Self::UnknownPeriod { .. } | Self::NotYetValid { .. })
    }
}

/// What the light client holds as verified. Cheap to clone: the committees are shared.
#[derive(Debug, Clone)]
pub(super) struct Store {
    /// Slot of the finalized header.
    finalized_slot: u64,
    /// Root of the finalized header: the block root peers are told we have finalized.
    finalized_root: B256,
    /// Slot of the newest header accepted as head.
    head_slot: u64,
    /// Root of that header.
    head_root: B256,
    /// The committee of the finalized header's period.
    current: Arc<SyncCommittee>,
    /// The committee of the period after, once an update proved it.
    next: Option<Arc<SyncCommittee>>,
}

/// What an accepted update changed, as the execution blocks it vouches for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Accepted {
    /// The execution block of a newer finalized header.
    pub(super) finalized: Option<TrustedL1Block>,
    /// The execution block of a newer head.
    pub(super) head: Option<TrustedL1Block>,
    /// Whether the next period's committee was learned.
    pub(super) next_committee: bool,
    /// Whether the committees rotated: the finalized header entered a new period.
    pub(super) rotated: bool,
}

/// The parts of the three kinds of update that are verified the same way.
struct Update<'a> {
    attested: &'a LightClientHeader,
    aggregate: &'a SyncAggregate,
    signature_slot: u64,
    finalized: Option<(&'a LightClientHeader, &'a [B256])>,
    next_committee: Option<(&'a SyncCommittee, &'a [B256])>,
}

impl Store {
    /// Starts a store from the SSZ of a bootstrap for `checkpoint`.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the bootstrap is not the one of `checkpoint` or its sync
    /// committee is not proven by its header.
    pub(super) fn bootstrap(
        spec: &BeaconSpec,
        checkpoint: B256,
        ssz: &[u8],
    ) -> Result<(Self, TrustedL1Block), VerifyError> {
        let bootstrap = decode::<LightClientBootstrap>(ssz)?;
        let header = &bootstrap.header;
        let root = header.beacon.tree_hash_root();
        if root != checkpoint {
            return Err(VerifyError::WrongCheckpoint {
                wanted: checkpoint,
                got: root,
            });
        }
        check_header(spec, header)?;
        check_branch(
            "current sync committee",
            bootstrap.current_sync_committee.tree_hash_root(),
            &bootstrap.current_sync_committee_branch,
            CURRENT_SYNC_COMMITTEE_GINDEX,
            header.beacon.state_root,
        )?;
        let store = Self {
            finalized_slot: header.beacon.slot,
            finalized_root: root,
            head_slot: header.beacon.slot,
            head_root: root,
            current: Arc::new(bootstrap.current_sync_committee),
            next: None,
        };
        Ok((store, trusted(header, true)))
    }

    /// The sync-committee period of the finalized header: the one whose committee is known.
    pub(super) const fn period(&self) -> u64 {
        self.finalized_slot / SLOTS_PER_PERIOD
    }

    /// Whether the committee of the period after [`Self::period`] is known.
    pub(super) const fn knows_next_committee(&self) -> bool {
        self.next.is_some()
    }

    /// The finalized header's root and epoch, and the head's root and slot: what a `Status`
    /// message reports.
    pub(super) const fn status(&self) -> StatusData {
        StatusData {
            finalized_root: self.finalized_root,
            finalized_epoch: self.finalized_slot / SLOTS_PER_EPOCH,
            head_root: self.head_root,
            head_slot: self.head_slot,
        }
    }

    /// Verifies the SSZ of a `LightClientUpdate` and applies it.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the update does not verify; the store is then unchanged.
    pub(super) fn apply_update(
        &mut self,
        spec: &BeaconSpec,
        ssz: &[u8],
        now_slot: u64,
    ) -> Result<Accepted, VerifyError> {
        let update = decode::<LightClientUpdate>(ssz)?;
        self.apply(
            spec,
            &Update {
                attested: &update.attested_header,
                aggregate: &update.sync_aggregate,
                signature_slot: update.signature_slot,
                finalized: Some((&update.finalized_header, &update.finality_branch)),
                next_committee: Some((
                    &update.next_sync_committee,
                    &update.next_sync_committee_branch,
                )),
            },
            now_slot,
        )
    }

    /// Verifies the SSZ of a `LightClientFinalityUpdate` and applies it.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the update does not verify; the store is then unchanged.
    pub(super) fn apply_finality_update(
        &mut self,
        spec: &BeaconSpec,
        ssz: &[u8],
        now_slot: u64,
    ) -> Result<Accepted, VerifyError> {
        let update = decode::<LightClientFinalityUpdate>(ssz)?;
        self.apply(
            spec,
            &Update {
                attested: &update.attested_header,
                aggregate: &update.sync_aggregate,
                signature_slot: update.signature_slot,
                finalized: Some((&update.finalized_header, &update.finality_branch)),
                next_committee: None,
            },
            now_slot,
        )
    }

    /// Verifies the SSZ of a `LightClientOptimisticUpdate` and applies it.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the update does not verify; the store is then unchanged.
    pub(super) fn apply_optimistic_update(
        &mut self,
        spec: &BeaconSpec,
        ssz: &[u8],
        now_slot: u64,
    ) -> Result<Accepted, VerifyError> {
        let update = decode::<LightClientOptimisticUpdate>(ssz)?;
        self.apply(
            spec,
            &Update {
                attested: &update.attested_header,
                aggregate: &update.sync_aggregate,
                signature_slot: update.signature_slot,
                finalized: None,
                next_committee: None,
            },
            now_slot,
        )
    }

    /// Validates `update` against the store, then moves the store forward by it.
    fn apply(
        &mut self,
        spec: &BeaconSpec,
        update: &Update<'_>,
        now_slot: u64,
    ) -> Result<Accepted, VerifyError> {
        let attested = &update.attested.beacon;
        let finalized_slot = update
            .finalized
            .map_or(attested.slot, |(header, _)| header.beacon.slot);
        if update.signature_slot <= attested.slot || attested.slot < finalized_slot {
            return Err(VerifyError::SlotOrder {
                signature: update.signature_slot,
                attested: attested.slot,
                finalized: finalized_slot,
            });
        }
        if update.signature_slot > now_slot {
            return Err(VerifyError::NotYetValid {
                signature_slot: update.signature_slot,
                now: now_slot,
            });
        }
        // The committee that signs at a slot is the one of that slot's period.
        let (store_period, signature_period) =
            (self.period(), update.signature_slot / SLOTS_PER_PERIOD);
        let committee = if signature_period == store_period {
            &self.current
        } else {
            self.next
                .as_ref()
                .filter(|_| signature_period == store_period.saturating_add(1))
                .ok_or(VerifyError::UnknownPeriod {
                    signature: signature_period,
                    store: store_period,
                })?
        };

        check_header(spec, update.attested)?;
        if let Some((header, branch)) = update.finalized {
            check_header(spec, header)?;
            let root = header.beacon.tree_hash_root();
            check_branch(
                "finalized header",
                root,
                branch,
                FINALIZED_ROOT_GINDEX,
                attested.state_root,
            )?;
        }
        if let Some((next, branch)) = update.next_committee {
            check_branch(
                "next sync committee",
                next.tree_hash_root(),
                branch,
                NEXT_SYNC_COMMITTEE_GINDEX,
                attested.state_root,
            )?;
        }
        let participants = check_signature(spec, committee, attested, update)?;
        // Two thirds of the committee, for the finalized header as the specification has it
        // and for the head by choice.
        if participants.saturating_mul(3) < SYNC_COMMITTEE_SIZE * 2 {
            return Err(VerifyError::Participation(participants));
        }

        let mut accepted = Accepted::default();
        let mut period = store_period;
        if let Some((header, _)) = update.finalized
            && header.beacon.slot > self.finalized_slot
        {
            let finalized_period = header.beacon.slot / SLOTS_PER_PERIOD;
            if finalized_period > period {
                // The finalized header enters the next period: its committee, which must be
                // known and has signed this update, becomes the current one.
                let next = self
                    .next
                    .take()
                    .filter(|_| finalized_period == period.saturating_add(1));
                let Some(next) = next else {
                    return Err(VerifyError::UnknownPeriod {
                        signature: finalized_period,
                        store: period,
                    });
                };
                self.current = next;
                period = finalized_period;
                accepted.rotated = true;
            }
            self.finalized_slot = header.beacon.slot;
            self.finalized_root = header.beacon.tree_hash_root();
            accepted.finalized = Some(trusted(header, true));
        }
        // The next committee an update proves is the one after the attested header's period;
        // it is kept when that is the store's period, after a rotation included, so a run
        // of updates, one per period, each brings the committee the next one is signed by.
        if let Some((next, _)) = update.next_committee
            && self.next.is_none()
            && attested.slot / SLOTS_PER_PERIOD == period
        {
            self.next = Some(Arc::new(next.clone()));
            accepted.next_committee = true;
        }
        if attested.slot > self.head_slot {
            self.head_slot = attested.slot;
            self.head_root = attested.tree_hash_root();
            accepted.head = Some(trusted(update.attested, false));
        }
        Ok(accepted)
    }
}

/// The execution block a verified header carries.
const fn trusted(header: &LightClientHeader, finalized: bool) -> TrustedL1Block {
    TrustedL1Block {
        number: header.execution.block_number,
        hash: header.execution.block_hash,
        finalized,
    }
}

fn decode<T: Decode>(ssz: &[u8]) -> Result<T, VerifyError> {
    T::from_ssz_bytes(ssz).map_err(|err| VerifyError::Decode(format!("{err:?}")))
}

/// Checks that a header is of a fork this build reads and that its execution payload header
/// is the one its beacon block commits to ([`is_valid_light_client_header`]).
///
/// [`is_valid_light_client_header`]: https://github.com/ethereum/consensus-specs/blob/master/specs/capella/light-client/sync-protocol.md#modified-is_valid_light_client_header
fn check_header(spec: &BeaconSpec, header: &LightClientHeader) -> Result<(), VerifyError> {
    let slot = header.beacon.slot;
    if slot / SLOTS_PER_EPOCH < spec.electra_epoch {
        return Err(VerifyError::BeforeElectra { slot });
    }
    check_branch(
        "execution payload header",
        header.execution.tree_hash_root(),
        &header.execution_branch,
        EXECUTION_PAYLOAD_GINDEX,
        header.beacon.body_root,
    )
}

/// Checks a merkle branch: `leaf` at generalized index `gindex` under `root`
/// ([`is_valid_normalized_merkle_branch`]).
///
/// [`is_valid_normalized_merkle_branch`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#is_valid_normalized_merkle_branch
fn check_branch(
    what: &'static str,
    leaf: B256,
    branch: &[B256],
    gindex: u64,
    root: B256,
) -> Result<(), VerifyError> {
    let mut value = leaf;
    let mut index = gindex;
    for node in branch {
        value = if index & 1 == 1 {
            hash_pair(node, &value)
        } else {
            hash_pair(&value, node)
        };
        index >>= 1;
    }
    if index == 1 && value == root {
        Ok(())
    } else {
        Err(VerifyError::Branch(what))
    }
}

/// Verifies the committee's aggregate signature over the attested header and returns how many
/// members signed.
fn check_signature(
    spec: &BeaconSpec,
    committee: &SyncCommittee,
    attested: &BeaconBlockHeader,
    update: &Update<'_>,
) -> Result<usize, VerifyError> {
    let bits = &update.aggregate.sync_committee_bits;
    let keys = committee
        .pubkeys
        .iter()
        .zip(bits.iter())
        .filter(|(_, signed)| *signed)
        .map(|(key, _)| PublicKey::from_bytes(key.as_slice()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_invalid| VerifyError::Point)?;
    if keys.is_empty() {
        return Err(VerifyError::Participation(0));
    }
    let signature = Signature::from_bytes(update.aggregate.sync_committee_signature.as_slice())
        .map_err(|_invalid| VerifyError::Point)?;
    // compute_signing_root(header, domain): the root of the pair.
    let domain = spec.sync_committee_domain(update.signature_slot);
    let signing_root = hash_pair(&attested.tree_hash_root(), &domain);
    let keys_ref: Vec<&PublicKey> = keys.iter().collect();
    let verdict =
        signature.fast_aggregate_verify(true, signing_root.as_slice(), BLS_DST, &keys_ref);
    if verdict == BLST_ERROR::BLST_SUCCESS {
        Ok(keys.len())
    } else {
        Err(VerifyError::Signature)
    }
}
