//! Rebuilds one transaction from its downloaded row, for every type an OP Stack chain has.
//!
//! The result is a typed transaction whose consensus encoding `verify` hashes and compares
//! with the reported hash, so nothing here is trusted: a wrong field shows as a hash mismatch.
//!
//! - Legacy, EIP-2930, EIP-1559 and EIP-7702 transactions are rebuilt from their fields and
//!   signature; the sender is recovered from the signature.
//! - A legacy transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client
//!   before Bedrock) has no signer.
//! - A deposit (type `0x7E`) is rebuilt from the source hash and mint the service reports,
//!   when it does; its system-transaction flag, which no service reports, is the one of the
//!   two values that gives the reported hash. Without a reported source hash the block's
//!   deposits are rebuilt by the protocol's rules instead: see `deposit`.
//!
//! Does not check hashes or roots: see `verify`.

use alloy_consensus::transaction::{SignerRecoverable, from_eip155_value};
use alloy_consensus::{Sealed, Signed, TxEip1559, TxEip2930, TxEip7702, TxLegacy};
use alloy_eips::eip2930::AccessList;
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{Address, B256, Signature, TxKind, U256, keccak256, normalize_v};
use op_alloy_consensus::{OpTxEnvelope, TxDeposit};
use op_indexer_primitives::encode_transaction;
use serde::de::DeserializeOwned;

use crate::rows::TransactionRow;
use crate::verify::Check;

/// Transaction type of a deposit.
pub(crate) const DEPOSIT_TYPE: u8 = 0x7e;

/// A rebuilt transaction.
#[derive(Debug)]
pub(crate) struct Rebuilt {
    pub(crate) transaction: OpTxEnvelope,
    /// The sender; `None` for a legacy transaction signed with all zeros, which has none.
    pub(crate) sender: Option<Address>,
}

/// Rebuilds the transaction of `row`, the `index`-th of its block.
///
/// # Errors
///
/// Returns the [`Check`] that names what is missing or invalid in the row.
pub(crate) fn rebuild(index: u64, row: &TransactionRow) -> Result<Rebuilt, Check> {
    let missing = |field| Check::MissingField { index, field };
    let kind = row.kind.unwrap_or_default();
    let to = row.to.map_or(TxKind::Create, TxKind::Call);
    let (nonce, gas_limit, value, input) =
        (row.nonce.to(), row.gas.to(), row.value, row.input.clone());
    if kind == DEPOSIT_TYPE {
        let from = row.from.ok_or_else(|| missing("from"))?;
        let deposit = TxDeposit {
            source_hash: row.source_hash.ok_or_else(|| missing("source_hash"))?,
            from,
            to,
            mint: row.mint.map(|mint| mint.to()).unwrap_or_default(),
            value,
            gas_limit,
            is_system_transaction: false,
            input,
        };
        return Ok(Rebuilt {
            transaction: deposit_with_flag(deposit, row.hash),
            sender: Some(from),
        });
    }

    let chain_id = || {
        row.chain_id
            .map(|id| id.to())
            .ok_or_else(|| missing("chain_id"))
    };
    let access_list = || list::<AccessList>(index, "access_list", row.access_list.as_ref());
    let max_fee_per_gas = || {
        row.max_fee_per_gas
            .map(|fee| fee.to())
            .ok_or_else(|| missing("max_fee_per_gas"))
    };
    let max_priority_fee_per_gas = || {
        row.max_priority_fee_per_gas
            .map(|fee| fee.to())
            .ok_or_else(|| missing("max_priority_fee_per_gas"))
    };
    let gas_price = || {
        row.gas_price
            .map(|price| price.to())
            .ok_or_else(|| missing("gas_price"))
    };
    let (v, r, s) = (
        row.y_parity.or(row.v).ok_or_else(|| missing("v"))?,
        row.r.ok_or_else(|| missing("r"))?,
        row.s.ok_or_else(|| missing("s"))?,
    );
    // Typed transactions sign with the parity alone.
    let typed_signature = || {
        u64::try_from(v)
            .ok()
            .and_then(normalize_v)
            .map(|parity| Signature::new(r, s, parity))
            .ok_or(Check::SignatureV { index })
    };

    let transaction = match kind {
        0 => {
            let v = row.v.ok_or_else(|| missing("v"))?;
            let mut legacy = TxLegacy {
                chain_id: None,
                nonce,
                gas_price: gas_price()?,
                gas_limit,
                to,
                value,
                input,
            };
            if v.is_zero() && r.is_zero() && s.is_zero() {
                let signature = Signature::new(U256::ZERO, U256::ZERO, false);
                return Ok(Rebuilt {
                    transaction: OpTxEnvelope::Legacy(Signed::new_unchecked(
                        legacy, signature, row.hash,
                    )),
                    sender: None,
                });
            }
            // EIP-155: v carries the chain id, which is then part of the signed message.
            let (parity, chain_id) = u128::try_from(v)
                .ok()
                .and_then(from_eip155_value)
                .ok_or(Check::SignatureV { index })?;
            legacy.chain_id = chain_id;
            let signature = Signature::new(r, s, parity);
            OpTxEnvelope::Legacy(Signed::new_unchecked(legacy, signature, row.hash))
        }
        1 => {
            let transaction = TxEip2930 {
                chain_id: chain_id()?,
                nonce,
                gas_price: gas_price()?,
                gas_limit,
                to,
                value,
                access_list: access_list()?,
                input,
            };
            OpTxEnvelope::Eip2930(Signed::new_unchecked(
                transaction,
                typed_signature()?,
                row.hash,
            ))
        }
        2 => {
            let transaction = TxEip1559 {
                chain_id: chain_id()?,
                nonce,
                gas_limit,
                max_fee_per_gas: max_fee_per_gas()?,
                max_priority_fee_per_gas: max_priority_fee_per_gas()?,
                to,
                value,
                access_list: access_list()?,
                input,
            };
            OpTxEnvelope::Eip1559(Signed::new_unchecked(
                transaction,
                typed_signature()?,
                row.hash,
            ))
        }
        4 => {
            let transaction = TxEip7702 {
                chain_id: chain_id()?,
                nonce,
                gas_limit,
                max_fee_per_gas: max_fee_per_gas()?,
                max_priority_fee_per_gas: max_priority_fee_per_gas()?,
                to: row.to.ok_or_else(|| missing("to"))?,
                value,
                access_list: access_list()?,
                authorization_list: list::<Vec<SignedAuthorization>>(
                    index,
                    "authorization_list",
                    row.authorization_list.as_ref(),
                )?,
                input,
            };
            OpTxEnvelope::Eip7702(Signed::new_unchecked(
                transaction,
                typed_signature()?,
                row.hash,
            ))
        }
        kind => return Err(Check::UnsupportedType { index, kind }),
    };

    let recovered = transaction
        .recover_signer()
        .map_err(|_invalid| Check::SenderRecovery { index })?;
    if let Some(reported) = row.from
        && reported != recovered
    {
        return Err(Check::Sender {
            index,
            recovered,
            reported,
        });
    }
    Ok(Rebuilt {
        transaction,
        sender: Some(recovered),
    })
}

/// Wraps a deposit rebuilt elsewhere (see `deposit`), reported with `hash`.
pub(crate) fn deposit(transaction: TxDeposit, hash: B256) -> Rebuilt {
    Rebuilt {
        sender: Some(transaction.from),
        transaction: OpTxEnvelope::Deposit(Sealed::new_unchecked(transaction, hash)),
    }
}

/// Sets the system-transaction flag of `deposit`, which is not reported, to the value that
/// makes it hash to `reported`; left unset if neither does, which the caller's hash check
/// then reports.
fn deposit_with_flag(mut deposit: TxDeposit, reported: B256) -> OpTxEnvelope {
    let hash = |deposit: &TxDeposit| {
        let mut encoding = Vec::new();
        encode_transaction(
            &OpTxEnvelope::Deposit(Sealed::new_unchecked(deposit.clone(), reported)),
            &mut encoding,
        );
        keccak256(&encoding)
    };
    if hash(&deposit) != reported {
        deposit.is_system_transaction = true;
        if hash(&deposit) != reported {
            deposit.is_system_transaction = false;
        }
    }
    OpTxEnvelope::Deposit(Sealed::new_unchecked(deposit, reported))
}

/// Reads a list field the service gives as JSON in the Ethereum RPC's form; absent means
/// empty.
fn list<T: DeserializeOwned + Default>(
    index: u64,
    field: &'static str,
    value: Option<&serde_json::Value>,
) -> Result<T, Check> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(T::default());
    };
    T::deserialize(value).map_err(|err| Check::Field {
        index,
        field,
        reason: err.to_string(),
    })
}
