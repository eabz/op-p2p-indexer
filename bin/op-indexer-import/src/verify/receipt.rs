//! Rebuilds one receipt from its transaction's row and logs.
//!
//! The bloom is computed from the logs: every log's address and topics are hashed into it.
//! The same contracts and event signatures appear in log after log, so their hashes are kept
//! for the length of a chunk ([`BloomHashes`]) and each is computed once. A deposit receipt carries the sender's nonce and the
//! receipt version only from Canyon on, when they became part of the hashed receipt ([deposit
//! receipt]); before Canyon the service's values are not stored, because no root covers them.
//!
//! [deposit receipt]: https://specs.optimism.io/protocol/deposits.html#deposit-receipt

use alloy_primitives::map::HashMap;

use alloy_consensus::{Eip658Value, Receipt, ReceiptWithBloom};
use alloy_primitives::{Address, B256, Bloom, Log, LogData, keccak256};
use op_alloy_consensus::{DEPOSIT_TX_TYPE_ID, OpDepositReceipt, OpReceiptEnvelope};

use super::Check;
use crate::rows::{LogRow, TransactionRow};

/// Version of a deposit receipt from Canyon on, unless the service reports another.
const DEPOSIT_RECEIPT_VERSION: u64 = 1;

/// The keccak of every address and topic seen so far in a chunk's logs, which is what a bloom
/// is built from. Hashing them was a fifth of `verify`'s time before they were kept.
#[derive(Debug, Default)]
pub(super) struct BloomHashes {
    addresses: HashMap<Address, B256>,
    topics: HashMap<B256, B256>,
}

impl BloomHashes {
    /// The bloom of `logs`, as `alloy_primitives::logs_bloom` computes it.
    fn bloom(&mut self, logs: &[Log]) -> Bloom {
        let mut bloom = Bloom::ZERO;
        for log in logs {
            let address = self
                .addresses
                .entry(log.address)
                .or_insert_with(|| keccak256(log.address));
            bloom.m3_2048_hashed(address);
            for topic in log.topics() {
                let topic = self
                    .topics
                    .entry(*topic)
                    .or_insert_with(|| keccak256(topic));
                bloom.m3_2048_hashed(topic);
            }
        }
        bloom
    }
}

/// Rebuilds the receipt of `tx`, the `index`-th transaction of its block, from its row and
/// its logs. `canyon` is whether the block is at or after the chain's Canyon fork; `hashes`
/// is the chunk's.
///
/// # Errors
///
/// Returns [`Check::UnsupportedType`] for a transaction type this tool does not know.
pub(super) fn rebuild(
    index: u64,
    tx: &TransactionRow,
    logs: &[LogRow],
    canyon: bool,
    hashes: &mut BloomHashes,
) -> Result<OpReceiptEnvelope, Check> {
    let receipt = Receipt {
        status: tx.root.map_or_else(
            || Eip658Value::Eip658(tx.status == Some(1)),
            Eip658Value::PostState,
        ),
        cumulative_gas_used: tx.cumulative_gas_used.to(),
        logs: logs.iter().map(rebuild_log).collect(),
    };
    let logs_bloom = hashes.bloom(&receipt.logs);
    let with_bloom = |receipt| ReceiptWithBloom {
        receipt,
        logs_bloom,
    };
    Ok(match tx.kind.unwrap_or_default() {
        0 => OpReceiptEnvelope::Legacy(with_bloom(receipt)),
        1 => OpReceiptEnvelope::Eip2930(with_bloom(receipt)),
        2 => OpReceiptEnvelope::Eip1559(with_bloom(receipt)),
        4 => OpReceiptEnvelope::Eip7702(with_bloom(receipt)),
        DEPOSIT_TX_TYPE_ID => {
            let version = tx.deposit_receipt_version.map(|version| version.to());
            OpReceiptEnvelope::Deposit(ReceiptWithBloom {
                logs_bloom,
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
