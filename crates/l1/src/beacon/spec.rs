//! The consensus-layer parameters of a beacon network: its clock, its fork schedule and the
//! values signatures and fork digests are computed from.
//!
//! The fork schedule is configuration this project keeps current, like the execution fork
//! list in `op-indexer-chainspec`: peers only talk to a node whose fork digest is theirs.
//! When most beacon nodes found carry another digest, the build is behind a fork and says so.
//!
//! Does not talk to anyone and verifies nothing: see `verify`.

use alloy_primitives::{B256, FixedBytes, b256};
use sha2::{Digest, Sha256};

/// Slots in an epoch.
pub(super) const SLOTS_PER_EPOCH: u64 = 32;
/// Epochs a sync committee serves: a period of about 27 hours.
const EPOCHS_PER_SYNC_COMMITTEE_PERIOD: u64 = 256;
/// Slots in a sync-committee period.
pub(super) const SLOTS_PER_PERIOD: u64 = SLOTS_PER_EPOCH * EPOCHS_PER_SYNC_COMMITTEE_PERIOD;
/// `DOMAIN_SYNC_COMMITTEE`.
const DOMAIN_SYNC_COMMITTEE: [u8; 4] = [7, 0, 0, 0];

/// A fork version.
pub(super) type Version = [u8; 4];
/// A fork digest: what names a network and fork on the wire.
pub(super) type ForkDigest = [u8; 4];

/// The parameters of one beacon network.
#[derive(Debug)]
pub(super) struct BeaconSpec {
    /// Time of slot 0, in seconds since the Unix epoch.
    pub(super) genesis_time_secs: u64,
    /// Seconds in a slot.
    pub(super) seconds_per_slot: u64,
    /// Root of the validator set at genesis: part of every signing domain and fork digest.
    pub(super) genesis_validators_root: B256,
    /// Root of the genesis block, which a node that has synced nothing reports as its head.
    pub(super) genesis_block_root: B256,
    /// Every fork's activation epoch and version, ascending.
    pub(super) forks: &'static [(u64, Version)],
    /// From Fulu on, the epochs at which the blob limit changed and the limit from then on,
    /// ascending: the fork digest depends on the entry in force ([EIP-7892]).
    ///
    /// [EIP-7892]: https://eips.ethereum.org/EIPS/eip-7892
    pub(super) blob_schedule: &'static [(u64, u64)],
    /// Epoch from which the light-client containers have the shapes of `types`.
    pub(super) electra_epoch: u64,
}

/// Ethereum mainnet. Fork epochs and versions are those of the consensus-specs mainnet
/// configuration; Fulu, with the blob limit of its second parameter-only fork, gives the
/// digest `8c9f62fe` that mainnet nodes carried on 2026-10-04.
pub(super) const MAINNET: BeaconSpec = BeaconSpec {
    genesis_time_secs: 1_606_824_023,
    seconds_per_slot: 12,
    genesis_validators_root: b256!(
        "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
    ),
    genesis_block_root: b256!("0x4d611d5b93fdab69013a7f0a2f961caca0c853f87cfe9595fe50038163079360"),
    forks: &[
        (0, [0, 0, 0, 0]),
        (74_240, [1, 0, 0, 0]),
        (144_896, [2, 0, 0, 0]),
        (194_048, [3, 0, 0, 0]),
        (269_568, [4, 0, 0, 0]),
        (364_032, [5, 0, 0, 0]),
        (411_392, [6, 0, 0, 0]),
    ],
    blob_schedule: &[(412_672, 15), (419_072, 21)],
    electra_epoch: 364_032,
};

impl BeaconSpec {
    /// The slot at `now_secs` (seconds since the Unix epoch); 0 before genesis.
    pub(super) const fn slot_at(&self, now_secs: u64) -> u64 {
        now_secs.saturating_sub(self.genesis_time_secs) / self.seconds_per_slot
    }

    /// The version of the fork in force at `epoch`.
    pub(super) fn fork_version(&self, epoch: u64) -> Version {
        let active = self.forks.iter().rev().find(|(at, _)| *at <= epoch);
        active.map_or([0; 4], |(_, version)| *version)
    }

    /// The fork digest at `epoch`: the first four bytes of the fork data root, and from Fulu
    /// on those bytes combined with the blob parameters in force ([EIP-7892]). Between Fulu's
    /// activation and the first entry of the blob schedule the plain digest is returned,
    /// which was not checked against the network.
    ///
    /// [EIP-7892]: https://eips.ethereum.org/EIPS/eip-7892
    pub(super) fn fork_digest(&self, epoch: u64) -> ForkDigest {
        let root = self.fork_data_root(self.fork_version(epoch));
        let mut digest = [0; 4];
        digest.copy_from_slice(root.get(..4).unwrap_or_default());
        let blobs = self.blob_schedule.iter().rev().find(|(at, _)| *at <= epoch);
        if let Some((at, max_blobs)) = blobs {
            let mask = Sha256::new()
                .chain_update(at.to_le_bytes())
                .chain_update(max_blobs.to_le_bytes())
                .finalize();
            for (byte, mask) in digest.iter_mut().zip(mask) {
                *byte ^= mask;
            }
        }
        digest
    }

    /// The domain sync committees sign under at `signature_slot`: the fork version is that
    /// of the slot before, as the [specification] has it.
    ///
    /// [specification]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#validate_light_client_update
    pub(super) fn sync_committee_domain(&self, signature_slot: u64) -> B256 {
        let epoch = signature_slot.saturating_sub(1) / SLOTS_PER_EPOCH;
        let root = self.fork_data_root(self.fork_version(epoch));
        let mut domain = B256::ZERO;
        let (kind, rest) = domain.split_at_mut(4);
        kind.copy_from_slice(&DOMAIN_SYNC_COMMITTEE);
        rest.copy_from_slice(root.get(..28).unwrap_or_default());
        domain
    }

    /// `hash_tree_root(ForkData(version, genesis_validators_root))`.
    fn fork_data_root(&self, version: Version) -> B256 {
        let mut leaf = FixedBytes::<32>::ZERO;
        leaf.get_mut(..4)
            .unwrap_or_default()
            .copy_from_slice(&version);
        hash_pair(&leaf, &self.genesis_validators_root)
    }
}

/// SHA-256 of two 32-byte values: one step of a merkle tree.
pub(super) fn hash_pair(left: &B256, right: &B256) -> B256 {
    let hash = Sha256::new()
        .chain_update(left)
        .chain_update(right)
        .finalize();
    B256::from_slice(&hash)
}
