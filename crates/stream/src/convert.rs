//! Blocks into the protobuf messages: the decoded payload (header fields, transactions with
//! their senders, receipts and logs) or the raw one (the consensus encoding, as stored).
//!
//! A block is prepared once ([`Prepared`]) and each of its messages converted the first time
//! a subscription asks for it, then shared: a block every subscription receives is converted
//! once per payload, not once per subscription. Conversion is CPU work; callers run it off the
//! async runtime.

use std::borrow::Cow;
use std::sync::OnceLock;

use alloy_consensus::{Header, Transaction as _};
use alloy_eips::Typed2718 as _;
use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use op_alloy_consensus::{OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_primitives::{
    ArchivedBlock, BlockRef, BlockSource, DecodedBlock, EncodedBlock, L1Heads, decode_block,
    encode_receipts, encode_transaction, split_body,
};
use op_indexer_storage::InvalidBlockReason;

use crate::proto;

/// Which payload a subscription or a lookup asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Payload {
    Decoded,
    Raw,
}

impl From<proto::Payload> for Payload {
    /// Decoded unless raw is asked for.
    fn from(payload: proto::Payload) -> Self {
        match payload {
            proto::Payload::Raw => Self::Raw,
            proto::Payload::Decoded | proto::Payload::Unspecified => Self::Decoded,
        }
    }
}

/// A block as the stores hold it: decoded in the unsafe store, encoded in the archive.
#[derive(Debug)]
pub(crate) enum StoredBlock {
    Decoded(Box<DecodedBlock>),
    Archived(ArchivedBlock),
}

/// A stored block that cannot be decoded.
#[derive(Debug, Clone, thiserror::Error)]
#[error("block {hash} cannot be decoded: {reason}")]
pub(crate) struct ConvertError {
    hash: B256,
    reason: String,
}

impl ConvertError {
    fn new(hash: B256, err: &impl std::fmt::Display) -> Self {
        Self {
            hash,
            reason: err.to_string(),
        }
    }
}

/// A stored block with its number, hash and parent read once, and its messages converted on
/// first use.
#[derive(Debug)]
pub(crate) struct Prepared {
    block: StoredBlock,
    pub(crate) at: BlockRef,
    pub(crate) parent: B256,
    /// The header, decoded once.
    pub(crate) header: Header,
    /// How many transactions the block holds, counted without decoding them.
    pub(crate) tx_count: usize,
    decoded: OnceLock<Result<proto::Block, ConvertError>>,
    raw: OnceLock<Result<proto::Block, ConvertError>>,
    receipts_decoded: OnceLock<Result<Option<proto::Receipts>, ConvertError>>,
    receipts_raw: OnceLock<Result<Option<proto::Receipts>, ConvertError>>,
}

impl Prepared {
    /// Reads the block's header, and counts its transactions.
    pub(crate) fn new(block: StoredBlock) -> Result<Self, ConvertError> {
        let (hash, header, tx_count) = match &block {
            StoredBlock::Decoded(decoded) => (
                decoded.hash,
                decoded.block.header.clone(),
                decoded.block.body.transactions.len(),
            ),
            StoredBlock::Archived(archived) => {
                let hash = archived.encoded.hash;
                let header: Header = alloy_rlp::decode_exact(&archived.encoded.header)
                    .map_err(|err| ConvertError::new(hash, &err))?;
                let parts = split_body(&archived.encoded.body)
                    .ok_or_else(|| ConvertError::new(hash, &"its body is not a block body"))?;
                (hash, header, parts.transactions.len())
            }
        };
        Ok(Self {
            block,
            at: BlockRef {
                number: header.number,
                hash,
            },
            parent: header.parent_hash,
            header,
            tx_count,
            decoded: OnceLock::new(),
            raw: OnceLock::new(),
            receipts_decoded: OnceLock::new(),
            receipts_raw: OnceLock::new(),
        })
    }

    /// Whether the block's receipts are known.
    pub(crate) const fn has_receipts(&self) -> bool {
        match &self.block {
            StoredBlock::Decoded(block) => block.receipts.is_some(),
            StoredBlock::Archived(block) => block.encoded.receipts.is_some(),
        }
    }

    /// The `Block` message in `payload`, with the status `heads` give it.
    pub(crate) fn message(
        &self,
        payload: Payload,
        heads: &L1Heads,
    ) -> Result<proto::Block, ConvertError> {
        let cell = match payload {
            Payload::Decoded => &self.decoded,
            Payload::Raw => &self.raw,
        };
        let mut message = cell.get_or_init(|| self.convert(payload)).clone()?;
        message.status = status(self.at.number, heads).into();
        Ok(message)
    }

    /// The `Receipts` message in `payload`; `None` if the receipts are not known.
    pub(crate) fn receipts(
        &self,
        payload: Payload,
    ) -> Result<Option<proto::Receipts>, ConvertError> {
        let cell = match payload {
            Payload::Decoded => &self.receipts_decoded,
            Payload::Raw => &self.receipts_raw,
        };
        cell.get_or_init(|| self.convert_receipts(payload)).clone()
    }

    /// The block decoded, with its senders: the unsafe store's as it is, the archive's decoded
    /// (its senders as the archive recorded them). CPU work for an archived block. Checked to
    /// have one sender, and one receipt if any, per transaction, so they pair up by position.
    pub(crate) fn decoded(&self) -> Result<Cow<'_, DecodedBlock>, ConvertError> {
        let block = match &self.block {
            StoredBlock::Decoded(block) => Cow::Borrowed(&**block),
            StoredBlock::Archived(archived) => {
                let (block, receipts) = decode_block(&archived.encoded)
                    .map_err(|err| ConvertError::new(self.at.hash, &err))?;
                Cow::Owned(DecodedBlock {
                    block,
                    hash: archived.encoded.hash,
                    senders: archived.senders.clone(),
                    receipts,
                    source: BlockSource::Sync,
                })
            }
        };
        let transactions = block.block.body.transactions.len();
        if block.senders.len() != transactions {
            return Err(ConvertError::new(
                self.at.hash,
                &InvalidBlockReason::SenderCount,
            ));
        }
        if block
            .receipts
            .as_ref()
            .is_some_and(|receipts| receipts.len() != transactions)
        {
            return Err(ConvertError::new(
                self.at.hash,
                &InvalidBlockReason::ReceiptCount,
            ));
        }
        Ok(block)
    }

    fn convert(&self, payload: Payload) -> Result<proto::Block, ConvertError> {
        let payload = match (payload, &self.block) {
            (Payload::Raw, StoredBlock::Archived(block)) => raw(&block.encoded),
            (Payload::Raw, StoredBlock::Decoded(block)) => raw(&EncodedBlock::from(&**block)),
            (Payload::Decoded, _) => decoded(self.decoded()?.as_ref()),
        };
        Ok(proto::Block {
            number: self.at.number,
            hash: hash(self.at.hash),
            parent_hash: hash(self.parent),
            status: proto::Status::Unspecified.into(),
            payload: Some(payload),
        })
    }

    fn convert_receipts(&self, payload: Payload) -> Result<Option<proto::Receipts>, ConvertError> {
        let receipts = match (payload, &self.block) {
            (Payload::Raw, StoredBlock::Archived(archived)) => archived
                .encoded
                .receipts
                .as_ref()
                .map(|receipts| proto::receipts::Payload::Raw(receipts.0.clone())),
            (Payload::Raw, StoredBlock::Decoded(decoded)) => decoded
                .receipts
                .as_deref()
                .map(|receipts| proto::receipts::Payload::Raw(encode_receipts(receipts).0)),
            (Payload::Decoded, StoredBlock::Decoded(decoded)) => decoded
                .receipts
                .as_deref()
                .map(|receipts| proto::receipts::Payload::Decoded(receipt_list(receipts))),
            (Payload::Decoded, StoredBlock::Archived(archived)) => archived
                .encoded
                .receipts
                .as_ref()
                .map(|receipts| {
                    alloy_rlp::decode_exact::<Vec<OpReceiptEnvelope>>(receipts)
                        .map(|receipts| proto::receipts::Payload::Decoded(receipt_list(&receipts)))
                        .map_err(|err| ConvertError::new(self.at.hash, &err))
                })
                .transpose()?,
        };
        Ok(receipts.map(|payload| proto::Receipts {
            number: self.at.number,
            hash: hash(self.at.hash),
            payload: Some(payload),
        }))
    }
}

/// The `Heads` message.
pub(crate) fn heads_message(
    unsafe_head: Option<BlockRef>,
    heads: L1Heads,
    receipts: bool,
) -> proto::Heads {
    proto::Heads {
        unsafe_head: unsafe_head.map(block_ref),
        safe_head: heads.safe.map(block_ref),
        finalized_head: heads.finalized.map(block_ref),
        receipts,
    }
}

/// The status of block `number` under `heads`.
pub(crate) fn status(number: u64, heads: &L1Heads) -> proto::Status {
    let at_or_below = |head: Option<BlockRef>| head.is_some_and(|head| number <= head.number);
    if at_or_below(heads.finalized) {
        proto::Status::Finalized
    } else if at_or_below(heads.safe) {
        proto::Status::Safe
    } else {
        proto::Status::Unsafe
    }
}

fn block_ref(block: BlockRef) -> proto::BlockRef {
    proto::BlockRef {
        number: block.number,
        hash: hash(block.hash),
    }
}

fn hash(hash: B256) -> Bytes {
    Bytes::copy_from_slice(hash.as_slice())
}

fn address(address: Address) -> Bytes {
    Bytes::copy_from_slice(address.as_slice())
}

fn u256(value: U256) -> Bytes {
    value.to_be_bytes_vec().into()
}

fn raw(encoded: &EncodedBlock) -> proto::block::Payload {
    proto::block::Payload::Raw(proto::RawBlock {
        header: encoded.header.0.clone(),
        body: encoded.body.0.clone(),
        receipts: encoded.receipts.as_ref().map(|receipts| receipts.0.clone()),
    })
}

/// `block` as `decoded()` checked it: one sender per transaction.
fn decoded(block: &DecodedBlock) -> proto::block::Payload {
    let transactions = block
        .block
        .body
        .transactions
        .iter()
        .zip(&block.senders)
        .map(|(transaction, sender)| transaction_proto(transaction, *sender))
        .collect();
    proto::block::Payload::Decoded(proto::DecodedBlock {
        header: Some(header_proto(&block.block.header)),
        transactions,
        receipts: block.receipts.as_deref().map(receipt_list),
    })
}

fn header_proto(header: &Header) -> proto::Header {
    proto::Header {
        parent_hash: hash(header.parent_hash),
        ommers_hash: hash(header.ommers_hash),
        beneficiary: address(header.beneficiary),
        state_root: hash(header.state_root),
        transactions_root: hash(header.transactions_root),
        receipts_root: hash(header.receipts_root),
        logs_bloom: Bytes::copy_from_slice(header.logs_bloom.as_slice()),
        difficulty: u256(header.difficulty),
        number: header.number,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        timestamp: header.timestamp,
        extra_data: header.extra_data.0.clone(),
        mix_hash: hash(header.mix_hash),
        nonce: Bytes::copy_from_slice(header.nonce.as_slice()),
        base_fee_per_gas: header.base_fee_per_gas,
        withdrawals_root: header.withdrawals_root.map(hash),
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_block_root: header.parent_beacon_block_root.map(hash),
        requests_hash: header.requests_hash.map(hash),
    }
}

/// A transaction's nonce; `None` for a deposit, which has none.
pub(crate) fn nonce(transaction: &OpTxEnvelope) -> Option<u64> {
    transaction
        .as_deposit()
        .is_none()
        .then(|| transaction.nonce())
}

/// A dynamic-fee or set-code transaction's fee cap; `None` for the others and deposits.
pub(crate) fn max_fee_per_gas(transaction: &OpTxEnvelope) -> Option<u128> {
    (transaction.as_deposit().is_none() && transaction.is_dynamic_fee())
        .then(|| transaction.max_fee_per_gas())
}

/// A transaction in its consensus encoding, as the block holds it: a legacy transaction
/// signed with all zeros keeps that signature.
pub(crate) fn encoded(transaction: &OpTxEnvelope) -> Vec<u8> {
    let mut encoded = Vec::new();
    encode_transaction(transaction, &mut encoded);
    encoded
}

fn transaction_proto(transaction: &OpTxEnvelope, sender: Address) -> proto::Transaction {
    let deposit = transaction.as_deposit();
    proto::Transaction {
        hash: hash(transaction.tx_hash()),
        r#type: u32::from(transaction.ty()),
        sender: address(sender),
        to: transaction.to().map(address),
        value: u256(transaction.value()),
        input: transaction.input().0.clone(),
        gas_limit: transaction.gas_limit(),
        nonce: nonce(transaction),
        gas_price: transaction.gas_price().map(|price| u256(U256::from(price))),
        max_fee_per_gas: max_fee_per_gas(transaction).map(|fee| u256(U256::from(fee))),
        max_priority_fee_per_gas: transaction
            .max_priority_fee_per_gas()
            .map(|fee| u256(U256::from(fee))),
        source_hash: deposit.map(|deposit| hash(deposit.source_hash)),
        mint: deposit.map(|deposit| u256(U256::from(deposit.mint))),
        is_system_transaction: deposit.map(|deposit| deposit.is_system_transaction),
        encoded: encoded(transaction).into(),
    }
}

fn receipt_list(receipts: &[OpReceiptEnvelope]) -> proto::ReceiptList {
    proto::ReceiptList {
        receipts: receipts.iter().map(receipt_proto).collect(),
    }
}

fn receipt_proto(receipt: &OpReceiptEnvelope) -> proto::Receipt {
    proto::Receipt {
        success: receipt.status(),
        cumulative_gas_used: receipt.cumulative_gas_used(),
        logs: receipt
            .logs()
            .iter()
            .map(|log| proto::Log {
                address: address(log.address),
                topics: log.topics().iter().map(|topic| hash(*topic)).collect(),
                data: log.data.data.0.clone(),
            })
            .collect(),
        deposit_nonce: receipt.deposit_nonce(),
        deposit_receipt_version: receipt.deposit_receipt_version(),
    }
}
