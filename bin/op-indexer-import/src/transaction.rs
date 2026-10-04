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
use alloy_primitives::{Address, B256, Signature, TxKind, U256, normalize_v};
use op_alloy_consensus::{DEPOSIT_TX_TYPE_ID, OpTxEnvelope, TxDeposit};
use op_indexer_primitives::is_zero_signature;
use serde::de::DeserializeOwned;

use crate::rows::TransactionRow;
use crate::verify::Check;

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
    let fields = Fields { index, row };
    let transaction = match row.kind.unwrap_or_default() {
        0 => {
            let legacy = fields.legacy()?;
            if is_zero_signature(&legacy) {
                return Ok(Rebuilt {
                    transaction: legacy,
                    sender: None,
                });
            }
            legacy
        }
        1 => fields.eip2930()?,
        2 => fields.eip1559()?,
        4 => fields.eip7702()?,
        DEPOSIT_TX_TYPE_ID => return fields.deposit(),
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

/// A transaction row being rebuilt: its fields, each failing with the [`Check`] that names it.
struct Fields<'a> {
    index: u64,
    row: &'a TransactionRow,
}

impl Fields<'_> {
    const fn missing(&self, field: &'static str) -> Check {
        Check::MissingField {
            index: self.index,
            field,
        }
    }

    fn to(&self) -> TxKind {
        self.row.to.into()
    }

    fn chain_id(&self) -> Result<u64, Check> {
        let id = self.row.chain_id.ok_or_else(|| self.missing("chain_id"))?;
        Ok(id.to())
    }

    fn gas_price(&self) -> Result<u128, Check> {
        let price = self.row.gas_price;
        Ok(price.ok_or_else(|| self.missing("gas_price"))?.to())
    }

    fn max_fee_per_gas(&self) -> Result<u128, Check> {
        let fee = self.row.max_fee_per_gas;
        Ok(fee.ok_or_else(|| self.missing("max_fee_per_gas"))?.to())
    }

    fn max_priority_fee_per_gas(&self) -> Result<u128, Check> {
        let fee = self.row.max_priority_fee_per_gas;
        Ok(fee
            .ok_or_else(|| self.missing("max_priority_fee_per_gas"))?
            .to())
    }

    fn access_list(&self) -> Result<AccessList, Check> {
        self.list("access_list", self.row.access_list.as_ref())
    }

    /// Reads a list field the service gives as JSON in the Ethereum RPC's form; absent means
    /// empty.
    fn list<T: DeserializeOwned + Default>(
        &self,
        field: &'static str,
        value: Option<&serde_json::Value>,
    ) -> Result<T, Check> {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            return Ok(T::default());
        };
        T::deserialize(value).map_err(|err| Check::Field {
            index: self.index,
            field,
            reason: err.to_string(),
        })
    }

    fn r_s(&self) -> Result<(U256, U256), Check> {
        Ok((
            self.row.r.ok_or_else(|| self.missing("r"))?,
            self.row.s.ok_or_else(|| self.missing("s"))?,
        ))
    }

    /// The signature of a typed transaction, which signs with the parity alone.
    fn typed_signature(&self) -> Result<Signature, Check> {
        let (r, s) = self.r_s()?;
        let v = self.row.y_parity.or(self.row.v);
        u64::try_from(v.ok_or_else(|| self.missing("v"))?)
            .ok()
            .and_then(normalize_v)
            .map(|parity| Signature::new(r, s, parity))
            .ok_or(Check::SignatureV { index: self.index })
    }

    fn legacy(&self) -> Result<OpTxEnvelope, Check> {
        let row = self.row;
        let v = row.v.ok_or_else(|| self.missing("v"))?;
        let (r, s) = self.r_s()?;
        let mut transaction = TxLegacy {
            chain_id: None,
            nonce: row.nonce.to(),
            gas_price: self.gas_price()?,
            gas_limit: row.gas.to(),
            to: self.to(),
            value: row.value,
            input: row.input.clone(),
        };
        let signature = if v.is_zero() && r.is_zero() && s.is_zero() {
            Signature::new(U256::ZERO, U256::ZERO, false)
        } else {
            // EIP-155: v carries the chain id, which is then part of the signed message.
            let (parity, chain_id) = u128::try_from(v)
                .ok()
                .and_then(from_eip155_value)
                .ok_or(Check::SignatureV { index: self.index })?;
            transaction.chain_id = chain_id;
            Signature::new(r, s, parity)
        };
        Ok(OpTxEnvelope::Legacy(Signed::new_unchecked(
            transaction,
            signature,
            row.hash,
        )))
    }

    fn eip2930(&self) -> Result<OpTxEnvelope, Check> {
        let row = self.row;
        let transaction = TxEip2930 {
            chain_id: self.chain_id()?,
            nonce: row.nonce.to(),
            gas_price: self.gas_price()?,
            gas_limit: row.gas.to(),
            to: self.to(),
            value: row.value,
            access_list: self.access_list()?,
            input: row.input.clone(),
        };
        let signature = self.typed_signature()?;
        Ok(OpTxEnvelope::Eip2930(Signed::new_unchecked(
            transaction,
            signature,
            row.hash,
        )))
    }

    fn eip1559(&self) -> Result<OpTxEnvelope, Check> {
        let row = self.row;
        let transaction = TxEip1559 {
            chain_id: self.chain_id()?,
            nonce: row.nonce.to(),
            gas_limit: row.gas.to(),
            max_fee_per_gas: self.max_fee_per_gas()?,
            max_priority_fee_per_gas: self.max_priority_fee_per_gas()?,
            to: self.to(),
            value: row.value,
            access_list: self.access_list()?,
            input: row.input.clone(),
        };
        let signature = self.typed_signature()?;
        Ok(OpTxEnvelope::Eip1559(Signed::new_unchecked(
            transaction,
            signature,
            row.hash,
        )))
    }

    fn eip7702(&self) -> Result<OpTxEnvelope, Check> {
        let row = self.row;
        let authorization_list: Vec<SignedAuthorization> =
            self.list("authorization_list", row.authorization_list.as_ref())?;
        let transaction = TxEip7702 {
            chain_id: self.chain_id()?,
            nonce: row.nonce.to(),
            gas_limit: row.gas.to(),
            max_fee_per_gas: self.max_fee_per_gas()?,
            max_priority_fee_per_gas: self.max_priority_fee_per_gas()?,
            to: row.to.ok_or_else(|| self.missing("to"))?,
            value: row.value,
            access_list: self.access_list()?,
            authorization_list,
            input: row.input.clone(),
        };
        let signature = self.typed_signature()?;
        Ok(OpTxEnvelope::Eip7702(Signed::new_unchecked(
            transaction,
            signature,
            row.hash,
        )))
    }

    fn deposit(&self) -> Result<Rebuilt, Check> {
        let row = self.row;
        let from = row.from.ok_or_else(|| self.missing("from"))?;
        let deposit = TxDeposit {
            source_hash: row.source_hash.ok_or_else(|| self.missing("source_hash"))?,
            from,
            to: self.to(),
            mint: row.mint.map(|mint| mint.to()).unwrap_or_default(),
            value: row.value,
            gas_limit: row.gas.to(),
            is_system_transaction: false,
            input: row.input.clone(),
        };
        Ok(Rebuilt {
            transaction: deposit_with_flag(deposit, row.hash),
            sender: Some(from),
        })
    }
}

/// Sets the system-transaction flag of `deposit`, which is not reported, to the value that
/// makes it hash to `reported`; left unset if neither does, which the caller's hash check
/// then reports.
fn deposit_with_flag(mut deposit: TxDeposit, reported: B256) -> OpTxEnvelope {
    if deposit.tx_hash() != reported {
        deposit.is_system_transaction = true;
        if deposit.tx_hash() != reported {
            deposit.is_system_transaction = false;
        }
    }
    OpTxEnvelope::Deposit(Sealed::new_unchecked(deposit, reported))
}
