//! The fields a downloaded chunk's rows lack that rebuilding its blocks needs, all of them at
//! once. `verify` stops at the first block it cannot rebuild, which would show them one run at
//! a time; `download` lists them across the whole range before anything is verified.
//!
//! A field is listed where the rebuild needs it (it fails without it) and where it falls back
//! to a default the header hash then has to prove (a header field of a fork, a deposit
//! receipt's nonce and version, a receipt's status). A field `fill` supplied is not missing.
//! Nothing here is verified.

use super::Forks;
use crate::rows::{BlockRow, L1Info, Rows, TransactionRow};

/// What lacks a field: the header or a transaction type.
pub(crate) type Row = &'static str;

/// A field a row lacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Missing {
    pub(crate) row: Row,
    pub(crate) field: &'static str,
}

/// The blocks of `rows` the service sent without transactions: every block after the Bedrock
/// block has at least one, the L1-attributes deposit.
pub(crate) fn holes<'a>(forks: &Forks, rows: &'a Rows) -> impl Iterator<Item = &'a BlockRow> {
    let bedrock = forks.bedrock_block;
    rows.blocks
        .iter()
        .filter(move |block| block.number > bedrock && !rows.has_transactions(block.number))
}

/// Calls `found` with every field a row of `rows` lacks, and the row's block; a block without
/// its transactions lacks `transactions`.
pub(crate) fn missing(forks: &Forks, rows: &Rows, mut found: impl FnMut(Missing, u64)) {
    for block in holes(forks, rows) {
        let hole = Missing {
            row: "block",
            field: "transactions",
        };
        found(hole, block.number);
    }
    for block in &rows.blocks {
        header(forks, block, |field| {
            found(
                Missing {
                    row: "header",
                    field,
                },
                block.number,
            );
        });
    }
    // Whether the block of the transactions being read starts an epoch: only then are
    // deposits after the L1-attributes one users', which mint (an upgrade's mints nothing).
    let mut epoch_start = false;
    for tx in &rows.transactions {
        if tx.transaction_index == 0 {
            epoch_start = tx.kind == Some(op_alloy_consensus::DEPOSIT_TX_TYPE_ID)
                && L1Info::of(&tx.input).is_some_and(|info| info.sequence == 0);
        }
        let block = rows.block(tx.block_number);
        let canyon = block.is_some_and(|block| block.timestamp.to::<u64>() >= forks.canyon);
        transaction(tx, canyon, epoch_start, |row, field| {
            found(Missing { row, field }, tx.block_number);
        });
    }
}

/// Calls `found` with each header field of `block`'s forks that its row lacks.
fn header(forks: &Forks, block: &BlockRow, mut found: impl FnMut(&'static str)) {
    let timestamp: u64 = block.timestamp.to();
    // The Bedrock block's own `mix_hash` and base fee are rebuilt by `verify` (zero, and
    // EIP-1559's initial base fee); every later block needs them.
    let after_bedrock = block.number > forks.bedrock_block;
    let canyon = timestamp >= forks.canyon;
    let ecotone = timestamp >= forks.ecotone;
    let fields = [
        ("mix_hash", after_bedrock && block.mix_hash.is_none()),
        (
            "base_fee_per_gas",
            after_bedrock && block.base_fee_per_gas.is_none(),
        ),
        (
            "withdrawals_root",
            canyon && block.withdrawals_root.is_none(),
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
    ];
    for (field, lacks) in fields {
        if lacks {
            found(field);
        }
    }
}

/// Calls `found` with the transaction type of `tx` and each field its row lacks, as
/// `transaction::encode` and `receipt::rebuild` read them. `canyon` is whether its block is at
/// or after Canyon, `epoch_start` whether it starts an epoch.
fn transaction(
    tx: &TransactionRow,
    canyon: bool,
    epoch_start: bool,
    mut found: impl FnMut(Row, &'static str),
) {
    let r = ("r", tx.r.is_none());
    let s = ("s", tx.s.is_none());
    let typed_v = ("v", tx.y_parity.is_none() && tx.v.is_none());
    let from = ("from", tx.from.is_none());
    let chain_id = ("chain_id", tx.chain_id.is_none());
    let gas_price = ("gas_price", tx.gas_price.is_none());
    let max_fee = ("max_fee_per_gas", tx.max_fee_per_gas.is_none());
    let max_priority_fee = (
        "max_priority_fee_per_gas",
        tx.max_priority_fee_per_gas.is_none(),
    );
    // A receipt from before Byzantium has a state root in place of the status.
    let status = ("status", tx.status.is_none() && tx.root.is_none());
    let mut emit = |row: Row, fields: &[(&'static str, bool)]| {
        for &(field, lacks) in fields {
            if lacks {
                found(row, field);
            }
        }
    };
    match tx.kind.unwrap_or_default() {
        0 => {
            // A legacy transaction signed with all zeros has no sender.
            let zero_signature = [tx.v, tx.r, tx.s]
                .iter()
                .all(|value| value.is_some_and(|value| value.is_zero()));
            let from = ("from", from.1 && !zero_signature);
            let v = ("v", tx.v.is_none());
            emit("legacy", &[v, r, s, gas_price, from, status]);
        }
        1 => emit(
            "eip2930",
            &[chain_id, typed_v, r, s, gas_price, from, status],
        ),
        2 => emit(
            "eip1559",
            &[
                chain_id,
                typed_v,
                r,
                s,
                max_fee,
                max_priority_fee,
                from,
                status,
            ],
        ),
        4 => emit(
            "eip7702",
            &[
                chain_id,
                typed_v,
                r,
                s,
                max_fee,
                max_priority_fee,
                from,
                ("to", tx.to.is_none()),
                ("authorization_list", tx.lacks_authorization_list()),
                status,
            ],
        ),
        op_alloy_consensus::DEPOSIT_TX_TYPE_ID => emit(
            "deposit",
            &[
                ("source_hash", tx.source_hash.is_none()),
                // In an epoch's first block: an upgrade deposit there, which mints nothing, is
                // listed too.
                ("mint", epoch_start && tx.lacks_mint()),
                from,
                ("deposit_nonce", canyon && tx.deposit_nonce.is_none()),
                (
                    "deposit_receipt_version",
                    canyon && tx.deposit_receipt_version.is_none(),
                ),
                status,
            ],
        ),
        // `verify` names the type itself.
        _ => {}
    }
}
