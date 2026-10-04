//! The fields a downloaded chunk's rows lack that rebuilding its blocks needs, all of them at
//! once. `verify` stops at the first block it cannot rebuild, which would show them one run at
//! a time; `download` lists them across the whole range before anything is verified.
//!
//! A field is listed where the rebuild needs it (it fails without it) and where it falls back
//! to a default the header hash then has to prove (a header field of a fork, a deposit
//! receipt's nonce and version, a receipt's status). A field `fill` supplied is not missing.
//! Nothing here is verified.

use super::Forks;
use crate::rows::{BlockRow, Rows, TransactionRow};

/// What lacks a field: the header or a transaction type.
pub(crate) type Row = &'static str;

/// A field a row lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Missing {
    pub(crate) row: Row,
    pub(crate) field: &'static str,
}

/// Calls `found` with every field a row of `rows` lacks, and the row's block.
pub(crate) fn missing(forks: &Forks, rows: &Rows, mut found: impl FnMut(Missing, u64)) {
    for block in &rows.blocks {
        for field in header(forks, block) {
            found(
                Missing {
                    row: "header",
                    field,
                },
                block.number,
            );
        }
    }
    for tx in &rows.transactions {
        let timestamp = rows
            .blocks
            .binary_search_by_key(&tx.block_number, |block| block.number)
            .ok()
            .and_then(|at| rows.blocks.get(at))
            .map(|block| block.timestamp.to::<u64>());
        let canyon = timestamp.is_some_and(|timestamp| timestamp >= forks.canyon);
        let (row, fields) = transaction(tx, canyon);
        for field in fields {
            found(Missing { row, field }, tx.block_number);
        }
    }
}

/// The header fields of `block`'s forks that its row lacks.
fn header(forks: &Forks, block: &BlockRow) -> Vec<&'static str> {
    let timestamp: u64 = block.timestamp.to();
    let bedrock = block.number >= forks.bedrock_block;
    let ecotone = timestamp >= forks.ecotone;
    [
        ("mix_hash", bedrock && block.mix_hash.is_none()),
        (
            "base_fee_per_gas",
            bedrock && block.base_fee_per_gas.is_none(),
        ),
        (
            "withdrawals_root",
            timestamp >= forks.canyon && block.withdrawals_root.is_none(),
        ),
        ("blob_gas_used", ecotone && block.blob_gas_used.is_none()),
        (
            "excess_blob_gas",
            ecotone && block.excess_blob_gas.is_none(),
        ),
        (
            "parent_beacon_block_root",
            ecotone && block.parent_beacon_block_root.is_none(),
        ),
    ]
    .into_iter()
    .filter_map(|(field, missing)| missing.then_some(field))
    .collect()
}

/// The transaction type of `tx` and the fields its row lacks, as `transaction::encode` and
/// `receipt::rebuild` read them. `canyon` is whether its block is at or after Canyon.
fn transaction(tx: &TransactionRow, canyon: bool) -> (Row, Vec<&'static str>) {
    let signature = [("r", tx.r.is_none()), ("s", tx.s.is_none())];
    let typed_v = ("v", tx.y_parity.is_none() && tx.v.is_none());
    let from = ("from", tx.from.is_none());
    let chain_id = ("chain_id", tx.chain_id.is_none());
    let gas_price = ("gas_price", tx.gas_price.is_none());
    let fees = [
        ("max_fee_per_gas", tx.max_fee_per_gas.is_none()),
        (
            "max_priority_fee_per_gas",
            tx.max_priority_fee_per_gas.is_none(),
        ),
    ];
    // A receipt from before Byzantium has a state root in place of the status.
    let status = ("status", tx.status.is_none() && tx.root.is_none());
    let (row, fields): (Row, Vec<(&'static str, bool)>) = match tx.kind.unwrap_or_default() {
        0 => {
            let zero_signature = [tx.v, tx.r, tx.s]
                .iter()
                .all(|value| value.is_some_and(|value| value.is_zero()));
            let from = ("from", from.1 && !zero_signature);
            let mut fields = vec![("v", tx.v.is_none()), gas_price, from];
            fields.extend(signature);
            ("legacy", fields)
        }
        1 => {
            let mut fields = vec![chain_id, gas_price, typed_v, from];
            fields.extend(signature);
            ("eip2930", fields)
        }
        2 => {
            let mut fields = vec![chain_id, typed_v, from];
            fields.extend(signature.into_iter().chain(fees));
            ("eip1559", fields)
        }
        4 => {
            // EIP-7702 refuses an empty list, so none at all is one the service left out.
            let authorizations = tx.filled_authorization_list.is_none()
                && tx
                    .authorization_list
                    .as_ref()
                    .is_none_or(|list| list.is_empty());
            let mut fields = vec![
                chain_id,
                typed_v,
                from,
                ("to", tx.to.is_none()),
                ("authorization_list", authorizations),
            ];
            fields.extend(signature.into_iter().chain(fees));
            ("eip7702", fields)
        }
        op_alloy_consensus::DEPOSIT_TX_TYPE_ID => (
            "deposit",
            vec![
                ("source_hash", tx.source_hash.is_none()),
                from,
                ("deposit_nonce", canyon && tx.deposit_nonce.is_none()),
                (
                    "deposit_receipt_version",
                    canyon && tx.deposit_receipt_version.is_none(),
                ),
            ],
        ),
        // `verify` names the type itself.
        _ => ("unknown type", Vec::new()),
    };
    let fields = fields
        .into_iter()
        .chain([status])
        .filter_map(|(field, missing)| missing.then_some(field))
        .collect();
    (row, fields)
}
