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
//!   recent best participation): a header signed by fewer is not passed on. Participation
//!   is counted, and an update that cannot move the store is refused, before the signature
//!   is checked.
//! - **Committees**: an update whose attested and finalized headers are in the store's period
//!   proves the next period's committee; when the finalized header enters that period, it
//!   becomes the current one. The public keys of a committee are decompressed and checked
//!   once, when it is learned.
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
    LightClientBootstrap, LightClientFinalityUpdate, LightClientHeader,
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
/// The signature scheme's domain separation tag for Ethereum's proof-of-possession scheme.
const BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

/// Which light-client container a piece of data is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// `LightClientBootstrap`: starts a store.
    Bootstrap,
    /// `LightClientUpdate`: one period's update, with the next sync committee.
    Update,
    /// `LightClientFinalityUpdate`.
    Finality,
    /// `LightClientOptimisticUpdate`.
    Optimistic,
}

/// Why light-client data was refused. Every variant is the sending peer's fault except
/// those [`VerifyError::is_peer_fault`] names, which an honest peer can cause.
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
    #[error("only {0} of the sync-committee members signed")]
    Participation(usize),
    #[error("the update brings nothing newer than what the store holds")]
    Stale,
    #[error("an update arrived before the bootstrap")]
    NotBootstrapped,
    #[error("a sync-committee public key or the signature is not a valid curve point")]
    Point,
    #[error("the sync-committee signature does not verify")]
    Signature,
}

impl VerifyError {
    /// Whether the peer that sent the data misbehaved. It did not if the data is only ahead
    /// of the committees we know or of our clock, is an update few members signed, or is
    /// not news to us: the network passes those on, and this client merely holds its heads
    /// to a higher bar.
    pub(super) const fn is_peer_fault(&self) -> bool {
        !matches!(
            self,
            Self::UnknownPeriod { .. }
                | Self::NotYetValid { .. }
                | Self::Participation(_)
                | Self::Stale
                | Self::NotBootstrapped
        )
    }
}

/// A sync committee's public keys, decompressed and checked once.
#[derive(Debug)]
struct Committee {
    keys: Box<[PublicKey]>,
}

impl Committee {
    /// Decompresses and checks every key of `committee`.
    fn new(committee: &SyncCommittee) -> Result<Self, VerifyError> {
        let keys = committee
            .pubkeys
            .iter()
            .map(|key| PublicKey::key_validate(key.as_slice()))
            .collect::<Result<Box<[_]>, _>>()
            .map_err(|_invalid| VerifyError::Point)?;
        Ok(Self { keys })
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
    current: Arc<Committee>,
    /// The committee of the period after, once an update proved it.
    next: Option<Arc<Committee>>,
}

/// What verified data changed, as the execution blocks it vouches for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Accepted {
    /// The execution block of a newer finalized header (for a bootstrap, the checkpoint's).
    pub(super) finalized: Option<TrustedL1Block>,
    /// The execution block of a newer head.
    pub(super) head: Option<TrustedL1Block>,
}

/// An update of any of the three kinds, as it is verified.
struct Update {
    attested: LightClientHeader,
    aggregate: SyncAggregate,
    signature_slot: u64,
    finalized: Option<(LightClientHeader, Vec<B256>)>,
    next_committee: Option<(SyncCommittee, Vec<B256>)>,
}

impl Update {
    fn decode(kind: Kind, ssz: &[u8]) -> Result<Self, VerifyError> {
        Ok(match kind {
            Kind::Update => {
                let update = decode::<LightClientUpdate>(ssz)?;
                Self {
                    attested: update.attested_header,
                    aggregate: update.sync_aggregate,
                    signature_slot: update.signature_slot,
                    finalized: proven(&update.finality_branch)
                        .map(|branch| (update.finalized_header, branch)),
                    next_committee: proven(&update.next_sync_committee_branch)
                        .map(|branch| (update.next_sync_committee, branch)),
                }
            }
            Kind::Finality => {
                let update = decode::<LightClientFinalityUpdate>(ssz)?;
                Self {
                    attested: update.attested_header,
                    aggregate: update.sync_aggregate,
                    signature_slot: update.signature_slot,
                    finalized: proven(&update.finality_branch)
                        .map(|branch| (update.finalized_header, branch)),
                    next_committee: None,
                }
            }
            Kind::Optimistic | Kind::Bootstrap => {
                let update = decode::<LightClientOptimisticUpdate>(ssz)?;
                Self {
                    attested: update.attested_header,
                    aggregate: update.sync_aggregate,
                    signature_slot: update.signature_slot,
                    finalized: None,
                    next_committee: None,
                }
            }
        })
    }
}

/// A merkle branch, or `None` when it is empty (all zero): the value it would prove is absent
/// ([`is_sync_committee_update`], [`is_finality_update`]), which is not a fault.
///
/// [`is_sync_committee_update`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#is_sync_committee_update
/// [`is_finality_update`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#is_finality_update
fn proven(branch: &[B256]) -> Option<Vec<B256>> {
    branch
        .iter()
        .any(|node| !node.is_zero())
        .then(|| branch.to_vec())
}

/// Verifies `payloads`, data of one `kind`, against `store`, and returns the store after it
/// with what it changed. A bootstrap starts a new store from `checkpoint`; every other kind
/// needs one. Several updates (one per period, oldest first) are applied in turn, each
/// verified by the committee the one before proved; one that brings nothing new is passed
/// over, and when one fails, those before it are kept.
///
/// # Errors
///
/// Returns [`VerifyError`] if the data, or the first of several updates, does not verify.
pub(super) fn verify(
    spec: &BeaconSpec,
    checkpoint: B256,
    store: Option<Store>,
    kind: Kind,
    payloads: &[impl AsRef<[u8]>],
    now_slot: u64,
) -> Result<(Store, Accepted), VerifyError> {
    let first = payloads.first().map(AsRef::as_ref).unwrap_or_default();
    if kind == Kind::Bootstrap {
        return Store::bootstrap(spec, checkpoint, first);
    }
    let mut store = store.ok_or(VerifyError::NotBootstrapped)?;
    let mut accepted = Accepted::default();
    let mut applied = false;
    let mut stale = false;
    for ssz in payloads {
        match store.apply(spec, &Update::decode(kind, ssz.as_ref())?, now_slot) {
            Ok(step) => {
                applied = true;
                accepted.finalized = step.finalized.or(accepted.finalized);
                accepted.head = step.head.or(accepted.head);
            }
            // An update of a period already passed: the next one may still be news.
            Err(VerifyError::Stale) => stale = true,
            Err(err) if !applied => return Err(err),
            // The updates before it verified: keep them.
            Err(_) => break,
        }
    }
    if !applied && stale {
        return Err(VerifyError::Stale);
    }
    Ok((store, accepted))
}

impl Store {
    /// Starts a store from the SSZ of a bootstrap for `checkpoint`.
    fn bootstrap(
        spec: &BeaconSpec,
        checkpoint: B256,
        ssz: &[u8],
    ) -> Result<(Self, Accepted), VerifyError> {
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
            current: Arc::new(Committee::new(&bootstrap.current_sync_committee)?),
            next: None,
        };
        let accepted = Accepted {
            finalized: Some(trusted(header, true)),
            head: None,
        };
        Ok((store, accepted))
    }

    /// The sync-committee period of the finalized header: the one whose committee is known.
    pub(super) const fn period(&self) -> u64 {
        self.finalized_slot / SLOTS_PER_PERIOD
    }

    /// The last period whose committee is known: updates signed after it cannot be verified.
    pub(super) const fn last_known_period(&self) -> u64 {
        let next = if self.next.is_some() { 1 } else { 0 };
        self.period().saturating_add(next)
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

    /// The committee that signs in `period`, if known.
    fn committee(&self, period: u64) -> Result<&Arc<Committee>, VerifyError> {
        let store = self.period();
        let known = if period == store {
            Some(&self.current)
        } else if period == store.saturating_add(1) {
            self.next.as_ref()
        } else {
            None
        };
        known.ok_or(VerifyError::UnknownPeriod {
            signature: period,
            store,
        })
    }

    /// Validates `update` against the store, then moves the store forward by it. The store
    /// is unchanged when it fails.
    fn apply(
        &mut self,
        spec: &BeaconSpec,
        update: &Update,
        now_slot: u64,
    ) -> Result<Accepted, VerifyError> {
        let attested = &update.attested.beacon;
        let finalized_slot = update
            .finalized
            .as_ref()
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
        // The next committee is learned when unknown, and replaced when the update rotates the
        // committees: its finalized header enters the next period, whose next committee it
        // carries ([`apply_light_client_update`]). Either way only from an update with
        // finality, its finalized header in the attested header's period
        // ([`process_light_client_update`], `update_has_finalized_next_sync_committee`).
        //
        // [`apply_light_client_update`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#apply_light_client_update
        // [`process_light_client_update`]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#process_light_client_update
        let has_finality = update.finalized.is_some();
        let finalizes = has_finality && finalized_slot > self.finalized_slot;
        let rotates = finalizes && finalized_slot / SLOTS_PER_PERIOD > self.period();
        let learns_committee =
            has_finality && update.next_committee.is_some() && (self.next.is_none() || rotates);
        let moves = attested.slot > self.head_slot || finalizes;
        if !moves && !learns_committee {
            return Err(VerifyError::Stale);
        }
        let committee = Arc::clone(self.committee(update.signature_slot / SLOTS_PER_PERIOD)?);
        // Two thirds of the committee, for the finalized header as the specification has it
        // and for the head by choice. Counted before any hashing or pairing.
        let bits = &update.aggregate.sync_committee_bits;
        let participants = bits.num_set_bits();
        if participants.saturating_mul(3) < bits.len().saturating_mul(2) {
            return Err(VerifyError::Participation(participants));
        }

        check_header(spec, &update.attested)?;
        let finalized_root = match &update.finalized {
            Some((header, branch)) => {
                check_header(spec, header)?;
                let root = header.beacon.tree_hash_root();
                check_branch(
                    "finalized header",
                    root,
                    branch,
                    FINALIZED_ROOT_GINDEX,
                    attested.state_root,
                )?;
                Some(root)
            }
            None => None,
        };
        if let Some((next, branch)) = &update.next_committee {
            check_branch(
                "next sync committee",
                next.tree_hash_root(),
                branch,
                NEXT_SYNC_COMMITTEE_GINDEX,
                attested.state_root,
            )?;
        }
        let attested_root = attested.tree_hash_root();
        check_signature(spec, &committee, update, attested_root)?;
        // A committee the update proves, decompressed before the store changes, so a bad key
        // leaves it as it was.
        let attested_period = attested.slot / SLOTS_PER_PERIOD;
        let next = match &update.next_committee {
            Some((next, _))
                if learns_committee
                    && attested_period == finalized_slot / SLOTS_PER_PERIOD
                    && attested_period >= self.period() =>
            {
                Some(Committee::new(next)?)
            }
            _ => None,
        };

        let mut accepted = Accepted::default();
        if finalizes && let (Some((header, _)), Some(root)) = (&update.finalized, finalized_root) {
            // The finalized header enters the next period: its committee, known because it
            // signed this update (signature period ≥ finalized period), becomes the current.
            if rotates && let Some(next) = self.next.take() {
                self.current = next;
            }
            self.finalized_slot = header.beacon.slot;
            self.finalized_root = root;
            accepted.finalized = Some(trusted(header, true));
        }
        // The next committee an update proves is the one after its attested header's period;
        // kept when that is the store's period, after a rotation included, so that a run of
        // updates, one per period, each brings the committee the next one is signed by.
        if let Some(next) = next
            && attested_period == self.period()
        {
            self.next = Some(Arc::new(next));
        }
        if attested.slot > self.head_slot {
            self.head_slot = attested.slot;
            self.head_root = attested_root;
            accepted.head = Some(trusted(&update.attested, false));
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
    if !spec.reads_slot(slot) {
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

/// Verifies the committee's aggregate signature over the attested header, whose root is
/// `attested_root`.
fn check_signature(
    spec: &BeaconSpec,
    committee: &Committee,
    update: &Update,
    attested_root: B256,
) -> Result<(), VerifyError> {
    let bits = &update.aggregate.sync_committee_bits;
    let keys: Vec<&PublicKey> = committee
        .keys
        .iter()
        .zip(bits.iter())
        .filter(|(_, signed)| *signed)
        .map(|(key, _)| key)
        .collect();
    if keys.is_empty() {
        // The specification's minimum is one signer: no signer is no signature.
        return Err(VerifyError::Signature);
    }
    let signature = Signature::from_bytes(update.aggregate.sync_committee_signature.as_slice())
        .map_err(|_invalid| VerifyError::Point)?;
    // compute_signing_root(header, domain): the root of the pair.
    let domain = spec.sync_committee_domain(update.signature_slot);
    let signing_root = hash_pair(&attested_root, &domain);
    // The keys were checked when the committee was learned; the signature is checked here.
    let verdict = signature.fast_aggregate_verify(true, signing_root.as_slice(), BLS_DST, &keys);
    if verdict == BLST_ERROR::BLST_SUCCESS {
        Ok(())
    } else {
        Err(VerifyError::Signature)
    }
}
