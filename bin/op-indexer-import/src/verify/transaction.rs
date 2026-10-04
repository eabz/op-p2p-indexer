//! Encodes one transaction from its downloaded row, for every type an OP Stack chain has.
//!
//! The result is the transaction's consensus encoding, the bytes a block body and the
//! transactions trie hold. Nothing here is trusted: `verify` puts the trie's root into the
//! rebuilt header, whose hash must be the block's, so a wrong field in any row shows there.
//!
//! - Legacy, EIP-2930, EIP-1559 and EIP-7702 transactions are built from their fields and
//!   signature.
//! - A legacy transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client
//!   before Bedrock) is encoded with those zeros; it has no signer.
//! - A deposit (type `0x7E`) is built from the source hash and mint the service reports. Its
//!   system-transaction flag, which no service reports, follows the protocol's rule: only the
//!   L1-attributes deposit before Regolith has it ([L1 attributes deposited transaction],
//!   [Regolith]).
//!
//! No signature is checked and no sender recovered: one recovery per transaction would be most
//! of the work of `verify`, and the bytes served to peers do not contain senders. The sender
//! recorded for the optional database rows is the one the service reports.
//!
//! [L1 attributes deposited transaction]: https://specs.optimism.io/protocol/deposits.html#l1-attributes-deposited-transaction
//! [Regolith]: https://specs.optimism.io/protocol/regolith/overview.html

use alloy_consensus::transaction::from_eip155_value;
use alloy_consensus::{Signed, TxEip1559, TxEip2930, TxEip7702, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_eips::eip2930::AccessList;
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{Address, Signature, TxKind, U256, normalize_v};
use op_alloy_consensus::{DEPOSIT_TX_TYPE_ID, OpTxEnvelope, TxDeposit};
use op_indexer_primitives::encode_transaction;
use serde::de::DeserializeOwned;

use super::Check;
use crate::rows::TransactionRow;

/// Appends the consensus encoding of the transaction of `row`, the `index`-th of its block,
/// to `out`, and returns its sender as the service reports it: the zero address, with `true`,
/// for a legacy transaction signed with all zeros. `before_regolith` is whether the block is
/// before the chain's Regolith fork.
///
/// # Errors
///
/// Returns the [`Check`] that names what is missing or invalid in the row.
pub(super) fn encode(
    index: u64,
    row: &TransactionRow,
    before_regolith: bool,
    out: &mut Vec<u8>,
) -> Result<(Address, bool), Check> {
    let fields = Fields { index, row };
    let transaction = match row.kind.unwrap_or_default() {
        0 => {
            let (legacy, zero_signature) = fields.legacy()?;
            if zero_signature {
                encode_transaction(&legacy, out);
                return Ok((Address::ZERO, true));
            }
            legacy
        }
        1 => fields.eip2930()?,
        2 => fields.eip1559()?,
        4 => fields.eip7702()?,
        DEPOSIT_TX_TYPE_ID => {
            let deposit = fields.deposit(before_regolith && index == 0)?;
            deposit.encode_2718(out);
            return Ok((deposit.from, false));
        }
        kind => return Err(Check::UnsupportedType { index, kind }),
    };
    encode_transaction(&transaction, out);
    Ok((row.from.ok_or_else(|| fields.missing("from"))?, false))
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

    /// The legacy transaction, and whether it is signed with all zeros.
    fn legacy(&self) -> Result<(OpTxEnvelope, bool), Check> {
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
        let zero_signature = v.is_zero() && r.is_zero() && s.is_zero();
        let signature = if zero_signature {
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
        let signed = Signed::new_unhashed(transaction, signature);
        Ok((OpTxEnvelope::Legacy(signed), zero_signature))
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
        let signed = Signed::new_unhashed(transaction, self.typed_signature()?);
        Ok(OpTxEnvelope::Eip2930(signed))
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
        let signed = Signed::new_unhashed(transaction, self.typed_signature()?);
        Ok(OpTxEnvelope::Eip1559(signed))
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
        let signed = Signed::new_unhashed(transaction, self.typed_signature()?);
        Ok(OpTxEnvelope::Eip7702(signed))
    }

    fn deposit(&self, is_system_transaction: bool) -> Result<TxDeposit, Check> {
        let row = self.row;
        Ok(TxDeposit {
            // Without it the deposit cannot be rebuilt: it comes from the deposit's event on
            // L1, which this tool does not read.
            source_hash: row.source_hash.ok_or_else(|| self.missing("source_hash"))?,
            from: row.from.ok_or_else(|| self.missing("from"))?,
            to: self.to(),
            mint: row.mint.map(|mint| mint.to()).unwrap_or_default(),
            value: row.value,
            gas_limit: row.gas.to(),
            is_system_transaction,
            input: row.input.clone(),
        })
    }
}
