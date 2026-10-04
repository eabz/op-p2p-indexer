//! Rebuilds one receipt from its transaction's row and logs.
//!
//! The bloom is computed from the logs. A deposit receipt carries the sender's nonce and the
//! receipt version only from Canyon on, when they became part of the hashed receipt ([deposit
//! receipt]); before Canyon the service's values are not stored, because no root covers them.
//!
//! [deposit receipt]: https://specs.optimism.io/protocol/deposits.html#deposit-receipt

use alloy_consensus::{Eip658Value, Receipt, ReceiptWithBloom};
use alloy_primitives::{Log, LogData, logs_bloom};
use op_alloy_consensus::{DEPOSIT_TX_TYPE_ID, OpDepositReceipt, OpReceiptEnvelope};

use super::Check;
use crate::rows::{LogRow, TransactionRow};

/// Version of a deposit receipt from Canyon on, unless the service reports another.
const DEPOSIT_RECEIPT_VERSION: u64 = 1;

/// Rebuilds the receipt of `tx`, the `index`-th transaction of its block, from its row and
/// its logs. `canyon` is whether the block is at or after the chain's Canyon fork.
///
/// # Errors
///
/// Returns [`Check::UnsupportedType`] for a transaction type this tool does not know.
pub(super) fn rebuild(
    index: u64,
    tx: &TransactionRow,
    logs: &[LogRow],
    canyon: bool,
) -> Result<OpReceiptEnvelope, Check> {
    let receipt = Receipt {
        status: tx.root.map_or_else(
            || Eip658Value::Eip658(tx.status == Some(1)),
            Eip658Value::PostState,
        ),
        cumulative_gas_used: tx.cumulative_gas_used.to(),
        logs: logs.iter().map(rebuild_log).collect(),
    };
    Ok(match tx.kind.unwrap_or_default() {
        0 => OpReceiptEnvelope::Legacy(receipt.with_bloom()),
        1 => OpReceiptEnvelope::Eip2930(receipt.with_bloom()),
        2 => OpReceiptEnvelope::Eip1559(receipt.with_bloom()),
        4 => OpReceiptEnvelope::Eip7702(receipt.with_bloom()),
        DEPOSIT_TX_TYPE_ID => {
            let version = tx.deposit_receipt_version.map(|version| version.to());
            OpReceiptEnvelope::Deposit(ReceiptWithBloom {
                logs_bloom: logs_bloom(&receipt.logs),
                receipt: OpDepositReceipt {
                    inner: receipt,
                    deposit_nonce: canyon.then(|| tx.deposit_nonce.unwrap_or(tx.nonce).to()),
                    deposit_receipt_version: canyon
                        .then(|| version.unwrap_or(DEPOSIT_RECEIPT_VERSION)),
                },
            })
        }
        kind => return Err(Check::UnsupportedType { index, kind }),
    })
}

fn rebuild_log(row: &LogRow) -> Log {
    let topics = [row.topic0, row.topic1, row.topic2, row.topic3]
        .into_iter()
        .flatten()
        .collect();
    Log {
        address: row.address,
        data: LogData::new_unchecked(topics, row.data.clone()),
    }
}
