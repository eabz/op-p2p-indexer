//! The light-client containers of the consensus layer, as peers send them (SSZ).
//!
//! Only the shapes of the Electra fork and later are defined: the header carries the
//! execution payload header of Deneb, and the finality and sync-committee branches have
//! Electra's depths. Mainnet has been past Electra since 2025, and a light client starts from
//! a recent checkpoint, so older shapes are never met; an answer whose fork digest is not the
//! current one is refused before it is decoded.
//!
//! See the light-client specifications of [Altair] (the containers), [Capella] (the execution
//! header and its branch) and [Electra] (the generalized indices).
//!
//! [Altair]: https://github.com/ethereum/consensus-specs/blob/master/specs/altair/light-client/sync-protocol.md#containers
//! [Capella]: https://github.com/ethereum/consensus-specs/blob/master/specs/capella/light-client/sync-protocol.md#modified-lightclientheader
//! [Electra]: https://github.com/ethereum/consensus-specs/blob/master/specs/electra/light-client/sync-protocol.md#constants

use alloy_primitives::{Address, B256, FixedBytes, U256};
use ssz::BitVector;
use ssz_derive::Decode;
use ssz_types::typenum::{U4, U6, U7, U32, U512};
use ssz_types::{FixedVector, VariableList};
use tree_hash_derive::TreeHash;

/// A compressed BLS12-381 public key.
pub(super) type PublicKeyBytes = FixedBytes<48>;

/// A beacon block header.
#[derive(Debug, Decode, TreeHash)]
pub(super) struct BeaconBlockHeader {
    pub(super) slot: u64,
    pub(super) proposer_index: u64,
    pub(super) parent_root: B256,
    pub(super) state_root: B256,
    pub(super) body_root: B256,
}

/// The header of an execution payload (Deneb form): the L1 execution block of a beacon block.
#[derive(Debug, Decode, TreeHash)]
pub(super) struct ExecutionPayloadHeader {
    pub(super) parent_hash: B256,
    pub(super) fee_recipient: Address,
    pub(super) state_root: B256,
    pub(super) receipts_root: B256,
    pub(super) logs_bloom: FixedBytes<256>,
    pub(super) prev_randao: B256,
    pub(super) block_number: u64,
    pub(super) gas_limit: u64,
    pub(super) gas_used: u64,
    pub(super) timestamp: u64,
    pub(super) extra_data: VariableList<u8, U32>,
    pub(super) base_fee_per_gas: U256,
    pub(super) block_hash: B256,
    pub(super) transactions_root: B256,
    pub(super) withdrawals_root: B256,
    pub(super) blob_gas_used: u64,
    pub(super) excess_blob_gas: u64,
}

/// A beacon header with its execution payload header and the proof that ties the two.
#[derive(Debug, Decode)]
pub(super) struct LightClientHeader {
    pub(super) beacon: BeaconBlockHeader,
    pub(super) execution: ExecutionPayloadHeader,
    /// Proves `execution` against `beacon.body_root`.
    pub(super) execution_branch: FixedVector<B256, U4>,
}

/// The 512 validators that sign every block header for one period of about 27 hours.
#[derive(Debug, Decode, TreeHash)]
pub(super) struct SyncCommittee {
    pub(super) pubkeys: FixedVector<PublicKeyBytes, U512>,
    pub(super) aggregate_pubkey: PublicKeyBytes,
}

/// Which members of the sync committee signed, and their aggregate signature.
#[derive(Debug, Decode)]
pub(super) struct SyncAggregate {
    pub(super) sync_committee_bits: BitVector<U512>,
    pub(super) sync_committee_signature: FixedBytes<96>,
}

/// What a light client starts from: the header of a trusted block and the sync committee of
/// its period, proven against the header's state root.
#[derive(Debug, Decode)]
pub(super) struct LightClientBootstrap {
    pub(super) header: LightClientHeader,
    pub(super) current_sync_committee: SyncCommittee,
    pub(super) current_sync_committee_branch: FixedVector<B256, U6>,
}

/// One period's update: a signed header, the finalized header it proves, and the next
/// period's sync committee.
#[derive(Debug, Decode)]
pub(super) struct LightClientUpdate {
    pub(super) attested_header: LightClientHeader,
    pub(super) next_sync_committee: SyncCommittee,
    pub(super) next_sync_committee_branch: FixedVector<B256, U6>,
    pub(super) finalized_header: LightClientHeader,
    pub(super) finality_branch: FixedVector<B256, U7>,
    pub(super) sync_aggregate: SyncAggregate,
    pub(super) signature_slot: u64,
}

/// A signed header and the finalized header it proves.
#[derive(Debug, Decode)]
pub(super) struct LightClientFinalityUpdate {
    pub(super) attested_header: LightClientHeader,
    pub(super) finalized_header: LightClientHeader,
    pub(super) finality_branch: FixedVector<B256, U7>,
    pub(super) sync_aggregate: SyncAggregate,
    pub(super) signature_slot: u64,
}

/// A signed header: the head as the sync committee attested it.
#[derive(Debug, Decode)]
pub(super) struct LightClientOptimisticUpdate {
    pub(super) attested_header: LightClientHeader,
    pub(super) sync_aggregate: SyncAggregate,
    pub(super) signature_slot: u64,
}
