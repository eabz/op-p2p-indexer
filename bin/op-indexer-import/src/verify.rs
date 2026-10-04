//! The `verify` step: rebuilds every downloaded block and checks it, offline.
//!
//! For each block of a chunk, from the rows the service returned:
//!
//! 1. each transaction is rebuilt and encoded; the keccak of that encoding must be the
//!    reported transaction hash, and its sender must be recoverable from the signature;
//! 2. the transactions root over those encodings must be the header's;
//! 3. each receipt is rebuilt, its bloom computed from its logs; the receipts root must be the
//!    header's;
//! 4. the header is rebuilt, its bloom the union of the receipts' blooms; the keccak of its
//!    RLP must be the reported block hash;
//! 5. its parent hash must be the previous block's hash.
//!
//! The bytes that passed these checks are what is written (see `chunk`): nothing is encoded
//! again later. Once every chunk is verified, the chunks are linked to each other and the last
//! block's hash is compared with the trusted anchor, which proves the whole range by the chain
//! of parent hashes.
//!
//! A transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client before
//! Bedrock) has no signer: it gets the zero address, and is counted.
//!
//! Transactions are rebuilt in `transaction`. Deposits whose source hash the service does not
//! report are rebuilt by the protocol's rules in `deposit`; of those, user deposits need the
//! deposit contract's logs on L1, which are not downloaded yet, so such a block fails with a
//! named check and is not written.
//!
//! Before Canyon a deposit receipt's nonce is not part of the hashed receipt, so the receipts
//! root is computed without it, while the stored receipt keeps the reported nonce (see the
//! [deposit receipt] rules). That nonce is then not proven by any root.
//!
//! [deposit receipt]: https://specs.optimism.io/protocol/deposits.html#deposit-receipt

use std::fmt;
use std::io;
use std::path::Path;

use alloy_consensus::proofs::{calculate_receipt_root, ordered_trie_root_with_encoder};
use alloy_consensus::{
    EMPTY_OMMER_ROOT_HASH, Eip658Value, Header, Receipt, ReceiptWithBloom, TxReceipt,
};
use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
use alloy_primitives::{Address, B256, Bloom, Bytes, Log, LogData, keccak256, logs_bloom};
use alloy_rlp::Encodable;
use op_alloy_consensus::{OpDepositReceipt, OpReceiptEnvelope};
use op_indexer_primitives::{EncodedBlock, encode_receipts, encode_transaction};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::chunk::{self, Link, VerifiedBlock};
use crate::deposit::{
    DepositEvent, DepositForks, L1DepositSource, L1Origin, ReportedDeposit, Rule, rebuild_deposits,
};
use crate::rows::{self, BlockRow, LogRow, TransactionRow};
use crate::state::{Chunk, Forks, Plan, State};
use crate::transaction::{self, DEPOSIT_TYPE};

/// A rule a block failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Check {
    #[error("the block is not in the downloaded chunk")]
    Missing,
    #[error("transaction indexes are not 0, 1, 2, ...: found {found} at position {position}")]
    TransactionIndex { position: u64, found: u64 },
    #[error("transaction {index} has type {kind}, which is not known")]
    UnsupportedType { index: u64, kind: u8 },
    #[error("transaction {index}: the field `{field}` is not in the expected form: {reason}")]
    Field {
        index: u64,
        field: &'static str,
        reason: String,
    },
    #[error("transaction {index} (deposit): {rule}")]
    Deposit {
        index: usize,
        #[source]
        rule: Rule,
    },
    #[error("transaction {index} lacks the field `{field}`")]
    MissingField { index: u64, field: &'static str },
    #[error("transaction {index} has an invalid signature value v")]
    SignatureV { index: u64 },
    #[error("transaction {index} hashes to {computed}, reported {reported}")]
    TransactionHash {
        index: u64,
        computed: B256,
        reported: B256,
    },
    #[error("the sender of transaction {index} cannot be recovered")]
    SenderRecovery { index: u64 },
    #[error("transaction {index} is signed by {recovered}, reported sender {reported}")]
    Sender {
        index: u64,
        recovered: Address,
        reported: Address,
    },
    #[error("transactions root is {computed}, the header has {header}")]
    TransactionsRoot { computed: B256, header: B256 },
    #[error("receipts root is {computed}, the header has {header}")]
    ReceiptsRoot { computed: B256, header: B256 },
    #[error("the header has ommers hash {0}; blocks with ommers are not rebuilt")]
    Ommers(B256),
    #[error("the header hashes to {computed}, reported {reported}")]
    HeaderHash { computed: B256, reported: B256 },
    #[error("parent hash is {parent}, the block before has hash {previous}")]
    ParentLink { parent: B256, previous: B256 },
}

/// Why a chunk was not verified.
#[derive(Debug, thiserror::Error)]
enum ChunkError {
    #[error("failed to read or write the chunk: {0}")]
    Io(#[from] io::Error),
    #[error("block {number}: {check}")]
    Block { number: u64, check: Check },
}

/// What a verified chunk held.
#[derive(Debug, Clone, Copy, Default)]
struct Stats {
    blocks: u64,
    transactions: u64,
    /// Transactions signed with all zeros.
    zero_signatures: u64,
}

impl Stats {
    const fn add(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.zero_signatures = self.zero_signatures.saturating_add(other.zero_signatures);
    }
}

/// How far the range is verified, as the linking pass found it.
#[derive(Debug)]
struct Linked {
    /// Chunks with a verified file.
    verified: usize,
    /// Chunks of the plan.
    total: usize,
    /// The first broken link, if any.
    broken: Option<String>,
}

impl fmt::Display for Linked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {} chunks verified", self.verified, self.total)?;
        if let Some(broken) = &self.broken {
            write!(f, "; {broken}")?;
        }
        Ok(())
    }
}

/// Verifies every downloaded chunk of `plan` that is not verified yet, `threads` at a time,
/// then links the chunks up to the anchor.
///
/// # Errors
///
/// Returns an error naming the block and the check if a chunk fails, and an error if the
/// range is not verified completely when the step ends (chunks not downloaded, cancelled, or
/// a broken link).
pub(crate) async fn run(
    state: &State,
    plan: &Plan,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let todo = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || {
            plan.chunks()
                .filter(|chunk| {
                    state.raw_path(*chunk).exists() && !state.verified_path(*chunk).exists()
                })
                .collect::<Vec<_>>()
        })
        .await?
    };
    info!(chunks = todo.len(), threads, "verify starting");

    let mut queue = todo.into_iter();
    let mut tasks = JoinSet::new();
    let mut totals = Stats::default();
    let mut failure = None;
    loop {
        while failure.is_none()
            && !cancel.is_cancelled()
            && tasks.len() < threads
            && let Some(chunk) = queue.next()
        {
            let (raw, verified) = (state.raw_path(chunk), state.verified_path(chunk));
            let forks = plan.forks;
            tasks.spawn_blocking(move || (chunk, verify_chunk(&forks, chunk, &raw, &verified)));
        }
        // Chunks being verified run to their end: blocking work cannot be interrupted.
        match tasks.join_next().await {
            Some(Ok((_, Ok(chunk)))) => totals.add(chunk),
            Some(Ok((chunk, Err(err)))) => {
                let file = state.raw_path(chunk);
                error!(%err, file = %file.display(), "chunk failed verification");
                failure.get_or_insert_with(|| {
                    format!(
                        "{err} (chunk {}; delete it and download again if the data is wrong)",
                        file.display()
                    )
                });
            }
            Some(Err(err)) => {
                failure.get_or_insert_with(|| format!("verify task failed: {err}"));
            }
            None => break,
        }
    }
    info!(
        blocks = totals.blocks,
        transactions = totals.transactions,
        zero_signature_transactions = totals.zero_signatures,
        "chunks verified in this run"
    );
    if let Some(failure) = failure {
        eyre::bail!("verification failed: {failure}");
    }

    let linked = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || link_chunks(&state, &plan)).await??
    };
    if linked.broken.is_none() && linked.verified == linked.total {
        info!(
            chunks = linked.total,
            first = plan.first,
            last = plan.last,
            anchor = %plan.anchor,
            "range verified up to the anchor"
        );
        return Ok(());
    }
    eyre::bail!("range not verified: {linked}")
}

/// Checks that each verified chunk continues the one before and that the last block of the
/// range has the anchor's hash. Blocking.
fn link_chunks(state: &State, plan: &Plan) -> io::Result<Linked> {
    let mut linked = Linked {
        verified: 0,
        total: 0,
        broken: None,
    };
    let mut previous: Option<Link> = None;
    for chunk in plan.chunks() {
        linked.total = linked.total.saturating_add(1);
        let link = match chunk::read_link(&state.verified_path(chunk)) {
            Ok(link) => link,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                previous = None;
                continue;
            }
            Err(err) => return Err(err),
        };
        linked.verified = linked.verified.saturating_add(1);
        if let Some(previous) = previous
            && previous.last_hash != link.first_parent
            && linked.broken.is_none()
        {
            linked.broken = Some(format!(
                "block {}: parent hash is {}, the block before has hash {}",
                chunk.from, link.first_parent, previous.last_hash
            ));
        }
        if chunk.to > plan.last && link.last_hash != plan.anchor && linked.broken.is_none() {
            linked.broken = Some(format!(
                "block {}: hash is {}, the anchor is {}",
                plan.last, link.last_hash, plan.anchor
            ));
        }
        previous = Some(link);
    }
    Ok(linked)
}

/// Verifies the downloaded chunk at `raw` and writes it to `verified`. Blocking, CPU-bound.
fn verify_chunk(
    forks: &Forks,
    chunk: Chunk,
    raw: &Path,
    verified: &Path,
) -> Result<Stats, ChunkError> {
    let rows = rows::read(raw)?;
    let mut block_rows = rows.blocks.iter().peekable();
    let mut transactions = rows.transactions.as_slice();
    let mut logs = rows.logs.as_slice();

    let mut stats = Stats::default();
    let mut blocks = Vec::new();
    let mut link: Option<Link> = None;
    for number in chunk.from..chunk.to {
        let failed = |check| ChunkError::Block { number, check };
        // Rows of blocks outside the chunk, if the service sent any, are not used.
        while block_rows.next_if(|row| row.number < number).is_some() {}
        let _before = take_while(&mut transactions, |tx| tx.block_number < number);
        let _before = take_while(&mut logs, |log| log.block_number < number);
        let row = block_rows
            .next_if(|row| row.number == number)
            .ok_or_else(|| failed(Check::Missing))?;
        let block_transactions = take_while(&mut transactions, |tx| tx.block_number == number);
        let block_logs = take_while(&mut logs, |log| log.block_number == number);

        let (block, zero_signatures) =
            verify_block(forks, row, block_transactions, block_logs).map_err(failed)?;
        if let Some(link) = &link
            && link.last_hash != row.parent_hash
        {
            return Err(failed(Check::ParentLink {
                parent: row.parent_hash,
                previous: link.last_hash,
            }));
        }
        link = Some(Link {
            first_parent: link.map_or(row.parent_hash, |link| link.first_parent),
            last_hash: row.hash,
        });
        stats.add(Stats {
            blocks: 1,
            transactions: u64::try_from(block.senders.len()).unwrap_or(u64::MAX),
            zero_signatures,
        });
        blocks.push(block);
    }
    if let Some(link) = link {
        chunk::write(verified, link, &blocks)?;
    }
    Ok(stats)
}

/// Rebuilds one block from its rows and checks it against the reported hashes. Returns the
/// verified encodings and the number of transactions signed with all zeros.
fn verify_block(
    forks: &Forks,
    row: &BlockRow,
    transactions: &[TransactionRow],
    logs: &[LogRow],
) -> Result<(VerifiedBlock, u64), Check> {
    let timestamp: u64 = row.timestamp.to();
    let body = rebuild_body(forks, row, transactions, logs)?;

    let transactions_root = ordered_trie_root_with_encoder(&body.encodings, |encoding, out| {
        out.extend_from_slice(encoding);
    });
    if transactions_root != row.transactions_root {
        return Err(Check::TransactionsRoot {
            computed: transactions_root,
            header: row.transactions_root,
        });
    }
    let receipts_root = if timestamp >= forks.canyon_time {
        calculate_receipt_root(&body.receipts)
    } else {
        let hashed: Vec<_> = body.receipts.iter().map(without_deposit_fields).collect();
        calculate_receipt_root(&hashed)
    };
    if receipts_root != row.receipts_root {
        return Err(Check::ReceiptsRoot {
            computed: receipts_root,
            header: row.receipts_root,
        });
    }
    // The body is written without ommers, which this hash must agree with.
    if row.sha3_uncles != EMPTY_OMMER_ROOT_HASH {
        return Err(Check::Ommers(row.sha3_uncles));
    }

    let mut logs_bloom = Bloom::ZERO;
    for receipt in &body.receipts {
        logs_bloom.accrue_bloom(receipt.logs_bloom());
    }
    let header = encode_header(forks, row, logs_bloom);
    let computed = keccak256(&header);
    if computed != row.hash {
        return Err(Check::HeaderHash {
            computed,
            reported: row.hash,
        });
    }

    let block = VerifiedBlock {
        encoded: EncodedBlock {
            hash: row.hash,
            header: header.into(),
            body: encode_body(&body.encodings, row.withdrawals_root.is_some()),
            receipts: Some(encode_receipts(&body.receipts)),
        },
        senders: body.senders,
    };
    Ok((block, body.zero_signatures))
}

/// The transactions and receipts of a block, rebuilt; each transaction's hash is checked.
#[derive(Debug)]
struct Body {
    /// Consensus encoding of each transaction.
    encodings: Vec<Vec<u8>>,
    senders: Vec<Address>,
    receipts: Vec<OpReceiptEnvelope>,
    /// Transactions signed with all zeros.
    zero_signatures: u64,
}

/// Rebuilds the transactions and receipts of a block and checks each transaction's hash.
fn rebuild_body(
    forks: &Forks,
    row: &BlockRow,
    transactions: &[TransactionRow],
    mut logs: &[LogRow],
) -> Result<Body, Check> {
    let timestamp: u64 = row.timestamp.to();
    let mut by_rules = deposits_by_rules(forks, row, transactions)?.into_iter();
    let mut body = Body {
        encodings: Vec::with_capacity(transactions.len()),
        senders: Vec::with_capacity(transactions.len()),
        receipts: Vec::with_capacity(transactions.len()),
        zero_signatures: 0,
    };
    for (index, tx) in (0_u64..).zip(transactions) {
        if tx.transaction_index != index {
            return Err(Check::TransactionIndex {
                position: index,
                found: tx.transaction_index,
            });
        }
        let (rebuilt, deposit_fields) = match by_rules.next() {
            Some(deposit) => (
                transaction::deposit(deposit.tx, tx.hash),
                (deposit.deposit_nonce, deposit.deposit_receipt_version),
            ),
            None => (
                transaction::rebuild(index, tx)?,
                reported_deposit_fields(forks, timestamp, tx),
            ),
        };
        let mut encoding = Vec::new();
        encode_transaction(&rebuilt.transaction, &mut encoding);
        let computed = keccak256(&encoding);
        if computed != tx.hash {
            return Err(Check::TransactionHash {
                index,
                computed,
                reported: tx.hash,
            });
        }
        if rebuilt.sender.is_none() {
            body.zero_signatures = body.zero_signatures.saturating_add(1);
        }
        body.encodings.push(encoding);
        body.senders.push(rebuilt.sender.unwrap_or(Address::ZERO));

        let _before = take_while(&mut logs, |log| log.transaction_index < index);
        let tx_logs = take_while(&mut logs, |log| log.transaction_index == index);
        body.receipts
            .push(rebuild_receipt(index, tx, tx_logs, deposit_fields)?);
    }
    Ok(body)
}

/// Rebuilds the block's leading deposits by the protocol's rules, when the service does not
/// report their source hashes. Empty when it does, or when the block has no deposits: they
/// are then rebuilt one by one from their rows.
fn deposits_by_rules(
    forks: &Forks,
    row: &BlockRow,
    transactions: &[TransactionRow],
) -> Result<Vec<crate::deposit::RebuiltDeposit>, Check> {
    let leading = transactions
        .iter()
        .take_while(|tx| tx.kind == Some(DEPOSIT_TYPE));
    if leading.clone().all(|tx| tx.source_hash.is_some()) {
        return Ok(Vec::new());
    }
    let reported = (0_u64..)
        .zip(leading)
        .map(|(index, tx)| {
            Ok(ReportedDeposit {
                hash: tx.hash,
                from: tx.from.ok_or(Check::MissingField {
                    index,
                    field: "from",
                })?,
                to: tx.to,
                value: tx.value,
                gas: tx.gas.to(),
                input: &tx.input,
                nonce: Some(tx.deposit_nonce.unwrap_or(tx.nonce).to()),
            })
        })
        .collect::<Result<Vec<_>, Check>>()?;
    let forks = DepositForks {
        regolith_time: Some(forks.regolith_time),
        canyon_time: Some(forks.canyon_time),
    };
    rebuild_deposits(&forks, row.number, row.timestamp.to(), &reported, &NoL1Logs).map_err(|err| {
        Check::Deposit {
            index: err.index,
            rule: err.rule,
        }
    })
}

/// The L1 side of deposit reconstruction until the deposit contract's logs are downloaded:
/// it has none, so a block with user deposits and no reported source hashes fails.
#[derive(Debug)]
struct NoL1Logs;

/// The deposit contract's logs on L1 are not available.
#[derive(Debug, thiserror::Error)]
#[error("the deposit contract's L1 logs are not downloaded by this version")]
struct L1LogsUnavailable;

impl L1DepositSource for NoL1Logs {
    type Error = L1LogsUnavailable;

    fn deposits(&self, _origin: &L1Origin) -> Result<Vec<DepositEvent>, Self::Error> {
        Err(L1LogsUnavailable)
    }
}

/// The deposit nonce and receipt version of a deposit rebuilt from its row: the reported
/// ones, or by the forks' rules the sender's nonce from Regolith and version 1 from Canyon.
/// `None` twice for any other transaction.
fn reported_deposit_fields(
    forks: &Forks,
    timestamp: u64,
    tx: &TransactionRow,
) -> (Option<u64>, Option<u64>) {
    if tx.kind != Some(DEPOSIT_TYPE) {
        return (None, None);
    }
    let nonce = tx
        .deposit_nonce
        .or_else(|| (timestamp >= forks.regolith_time).then_some(tx.nonce));
    let version = tx.deposit_receipt_version.map(|version| version.to());
    (
        nonce.map(|nonce| nonce.to()),
        version.or_else(|| (timestamp >= forks.canyon_time).then_some(1)),
    )
}

/// Rebuilds the receipt of `tx` from its row and logs, its bloom computed from the logs.
fn rebuild_receipt(
    index: u64,
    tx: &TransactionRow,
    logs: &[LogRow],
    (deposit_nonce, deposit_receipt_version): (Option<u64>, Option<u64>),
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
        DEPOSIT_TYPE => OpReceiptEnvelope::Deposit(ReceiptWithBloom {
            logs_bloom: logs_bloom(&receipt.logs),
            receipt: OpDepositReceipt {
                inner: receipt,
                deposit_nonce,
                deposit_receipt_version,
            },
        }),
        kind => return Err(Check::UnsupportedType { index, kind }),
    })
}

/// The receipt as it is hashed before Canyon: a deposit receipt without its nonce and version.
fn without_deposit_fields(receipt: &OpReceiptEnvelope) -> OpReceiptEnvelope {
    let mut receipt = receipt.clone();
    if let OpReceiptEnvelope::Deposit(deposit) = &mut receipt {
        deposit.receipt.deposit_nonce = None;
        deposit.receipt.deposit_receipt_version = None;
    }
    receipt
}

/// Encodes the header of `row` with the bloom computed from the block's logs.
fn encode_header(forks: &Forks, row: &BlockRow, logs_bloom: Bloom) -> Vec<u8> {
    let timestamp: u64 = row.timestamp.to();
    alloy_rlp::encode(Header {
        parent_hash: row.parent_hash,
        ommers_hash: row.sha3_uncles,
        beneficiary: row.miner,
        state_root: row.state_root,
        transactions_root: row.transactions_root,
        receipts_root: row.receipts_root,
        logs_bloom,
        difficulty: row.difficulty,
        number: row.number,
        gas_limit: row.gas_limit.to(),
        gas_used: row.gas_used.to(),
        timestamp,
        extra_data: row.extra_data.clone(),
        mix_hash: row.mix_hash,
        nonce: row.nonce,
        base_fee_per_gas: row.base_fee_per_gas.map(|fee| fee.to()),
        withdrawals_root: row.withdrawals_root,
        blob_gas_used: row.blob_gas_used.map(|gas| gas.to()),
        excess_blob_gas: row.excess_blob_gas.map(|gas| gas.to()),
        parent_beacon_block_root: row.parent_beacon_block_root,
        // OP Stack blocks have no execution requests; the service has no column for the hash.
        requests_hash: (row.withdrawals_root.is_some() && timestamp >= forks.isthmus_time)
            .then_some(EMPTY_REQUESTS_HASH),
        ..Default::default()
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

/// Encodes a block body as the eth protocol's `BlockBodies` holds it: the transactions, each
/// in its verified encoding (a legacy transaction is an RLP list, a typed one a byte string),
/// no ommers, and an empty withdrawals list from the fork that added the withdrawals root.
fn encode_body(transactions: &[Vec<u8>], with_withdrawals: bool) -> Bytes {
    let mut list = Vec::new();
    for encoding in transactions {
        if encoding
            .first()
            .is_some_and(|byte| *byte >= alloy_rlp::EMPTY_LIST_CODE)
        {
            list.extend_from_slice(encoding);
        } else {
            encoding.as_slice().encode(&mut list);
        }
    }
    let empty_lists = if with_withdrawals { 2 } else { 1 };
    let transactions = alloy_rlp::Header {
        list: true,
        payload_length: list.len(),
    };
    let mut out = Vec::new();
    alloy_rlp::Header {
        list: true,
        payload_length: transactions
            .length()
            .saturating_add(list.len())
            .saturating_add(empty_lists),
    }
    .encode(&mut out);
    transactions.encode(&mut out);
    out.extend_from_slice(&list);
    out.extend(std::iter::repeat_n(alloy_rlp::EMPTY_LIST_CODE, empty_lists));
    out.into()
}

/// Splits off the leading rows that satisfy `belongs`.
fn take_while<'a, T>(rows: &mut &'a [T], belongs: impl Fn(&T) -> bool) -> &'a [T] {
    let count = rows.iter().take_while(|row| belongs(row)).count();
    let (taken, rest) = rows.split_at(count);
    *rows = rest;
    taken
}
