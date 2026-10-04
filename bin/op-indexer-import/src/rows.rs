//! The rows of a downloaded chunk, as `HyperSync`'s JSON gives them.
//!
//! Quantities are hex strings; block numbers and indexes are JSON numbers; a field the block
//! or transaction does not have is absent or null. Fields this tool does not read (the blooms,
//! the L1 fee fields) are skipped. Nothing here is verified: see `verify`.

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{Address, B64, B256, Bytes, U64, U128, U256};
use flate2::bufread::MultiGzDecoder;
use serde::Deserialize;

use crate::source::Encoding;

/// Bytes of text reserved per stored byte of a compressed chunk.
const TEXT_PER_STORED_BYTE: usize = 16;

/// One answer of the service.
#[derive(Debug, Deserialize)]
struct Response {
    data: Vec<Batch>,
}

/// One batch of an answer.
#[derive(Debug, Deserialize)]
struct Batch {
    #[serde(default)]
    blocks: Vec<BlockRow>,
    #[serde(default)]
    transactions: Vec<TransactionRow>,
    #[serde(default)]
    logs: Vec<LogRow>,
}

/// A block header.
#[derive(Debug, Deserialize)]
pub(crate) struct BlockRow {
    pub(crate) number: u64,
    pub(crate) hash: B256,
    pub(crate) parent_hash: B256,
    pub(crate) sha3_uncles: B256,
    pub(crate) miner: Address,
    pub(crate) state_root: B256,
    pub(crate) difficulty: U256,
    pub(crate) gas_limit: U64,
    pub(crate) gas_used: U64,
    pub(crate) timestamp: U64,
    pub(crate) extra_data: Bytes,
    /// Absent from some pre-Bedrock rows the service gives (seen on OP Mainnet around block
    /// 47,705,000); `verify` then takes zero, the value of every legacy block, and the header
    /// hash decides.
    pub(crate) mix_hash: Option<B256>,
    pub(crate) nonce: B64,
    pub(crate) base_fee_per_gas: Option<U64>,
    pub(crate) withdrawals_root: Option<B256>,
    pub(crate) blob_gas_used: Option<U64>,
    pub(crate) excess_blob_gas: Option<U64>,
    pub(crate) parent_beacon_block_root: Option<B256>,
}

/// A transaction with the fields of its receipt.
#[derive(Debug, Deserialize)]
pub(crate) struct TransactionRow {
    pub(crate) block_number: u64,
    pub(crate) transaction_index: u64,
    /// The sender. Not proven by any check: see `verify`.
    pub(crate) from: Option<Address>,
    pub(crate) to: Option<Address>,
    pub(crate) gas: U64,
    pub(crate) gas_price: Option<U128>,
    pub(crate) input: Bytes,
    pub(crate) value: U256,
    pub(crate) nonce: U64,
    pub(crate) v: Option<U256>,
    pub(crate) r: Option<U256>,
    pub(crate) s: Option<U256>,
    /// The transaction type; absent means legacy.
    #[serde(rename = "type")]
    pub(crate) kind: Option<u8>,
    pub(crate) status: Option<u8>,
    /// State root of a receipt from before Byzantium, in place of the status.
    pub(crate) root: Option<B256>,
    pub(crate) cumulative_gas_used: U64,
    pub(crate) chain_id: Option<U64>,
    pub(crate) max_fee_per_gas: Option<U128>,
    pub(crate) max_priority_fee_per_gas: Option<U128>,
    /// Parity of a typed transaction's signature, where the service gives it apart from `v`.
    pub(crate) y_parity: Option<U256>,
    /// The access list: the bytes of the service's binary column, which it sends as a hex
    /// string. Decoded when the transaction is rebuilt (`verify::lists`), so an unexpected
    /// content fails that transaction's check, not the whole file.
    pub(crate) access_list: Option<Bytes>,
    /// The EIP-7702 authorizations, as the access list.
    pub(crate) authorization_list: Option<Bytes>,
    /// The EIP-7702 authorizations from the chunk's fill (`fill`), for a row the service sent
    /// without them; not in the service's answer.
    #[serde(skip)]
    pub(crate) filled_authorization_list: Option<Vec<SignedAuthorization>>,
    /// Deposit transactions: the hash that identifies the deposit's origin.
    pub(crate) source_hash: Option<B256>,
    /// Deposit transactions: ETH minted on L2.
    pub(crate) mint: Option<U128>,
    /// Deposit receipts: the sender's nonce before the transaction.
    pub(crate) deposit_nonce: Option<U64>,
    /// Deposit receipts: 1 from Canyon on.
    pub(crate) deposit_receipt_version: Option<U64>,
}

impl TransactionRow {
    /// Whether this is an EIP-7702 transaction whose authorization list the service left out:
    /// the fill has none, and the row has none, no bytes, or a list of zero entries (the
    /// service's encoding starts with the count). EIP-7702 refuses an empty list.
    pub(crate) fn lacks_authorization_list(&self) -> bool {
        self.kind == Some(4)
            && self.filled_authorization_list.is_none()
            && self
                .authorization_list
                .as_ref()
                .is_none_or(|list| list.get(..8).is_none_or(|count| count == [0_u8; 8]))
    }
}

/// A log.
#[derive(Debug, Deserialize)]
pub(crate) struct LogRow {
    pub(crate) block_number: u64,
    pub(crate) transaction_index: u64,
    pub(crate) log_index: u64,
    pub(crate) address: Address,
    pub(crate) data: Bytes,
    pub(crate) topic0: Option<B256>,
    pub(crate) topic1: Option<B256>,
    pub(crate) topic2: Option<B256>,
    pub(crate) topic3: Option<B256>,
}

/// An L1 transaction, as far as the lookup of dispute games reads it.
#[derive(Debug, Deserialize)]
pub(crate) struct L1TransactionRow {
    pub(crate) block_number: u64,
    pub(crate) transaction_index: u64,
    pub(crate) to: Option<Address>,
    pub(crate) input: Bytes,
}

/// The rows of a chunk, each kind in block order.
#[derive(Debug, Default)]
pub(crate) struct Rows {
    /// By block number.
    pub(crate) blocks: Vec<BlockRow>,
    /// By block number, then index in the block.
    pub(crate) transactions: Vec<TransactionRow>,
    /// By block number, then transaction index, then log index.
    pub(crate) logs: Vec<LogRow>,
}

impl Rows {
    /// The block numbered `number`, if the chunk has it.
    pub(crate) fn block(&self, number: u64) -> Option<&BlockRow> {
        let at = self
            .blocks
            .binary_search_by_key(&number, |block| block.number);
        at.ok().and_then(|at| self.blocks.get(at))
    }
}

/// Why a downloaded chunk could not be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RowsError {
    #[error("failed to read the chunk: {0}")]
    Io(#[from] io::Error),
    #[error("a downloaded row is not in the expected form: {0}")]
    Row(#[from] serde_json::Error),
}

/// Reads a downloaded chunk: one byte naming the content encoding, then one or more answers
/// in that encoding, one after the other, as `download` received them. Blocking.
///
/// # Errors
///
/// Returns [`RowsError::Io`] if the file cannot be read or decompressed, and
/// [`RowsError::Row`] if its content is not the expected JSON.
pub(crate) fn read(path: &Path) -> Result<Rows, RowsError> {
    let mut rows = Rows::default();
    {
        // The text is dropped before the rows are sorted: both are large.
        let file = File::open(path)?;
        // Room for the whole text at once: growing the buffer step by step copies it again
        // and again. The service's JSON compresses about tenfold.
        let stored = usize::try_from(file.metadata()?.len()).unwrap_or(usize::MAX);
        let mut file = BufReader::new(file);
        let mut tag = [0_u8];
        file.read_exact(&mut tag)?;
        let [tag] = tag;
        let mut data = match Encoding::from_tag(tag) {
            Some(Encoding::Identity) => Vec::with_capacity(stored),
            Some(Encoding::Gzip | Encoding::Zstd) => {
                Vec::with_capacity(stored.saturating_mul(TEXT_PER_STORED_BYTE))
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown content encoding {tag}"),
                )
                .into());
            }
        };
        match Encoding::from_tag(tag) {
            Some(Encoding::Gzip) => io::copy(&mut MultiGzDecoder::new(file), &mut data)?,
            Some(Encoding::Zstd) => {
                io::copy(&mut zstd::stream::Decoder::with_buffer(file)?, &mut data)?
            }
            Some(Encoding::Identity) | None => io::copy(&mut file, &mut data)?,
        };
        // Checked as text once, so the parser does not check every string again.
        let text = String::from_utf8(data)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.utf8_error()))?;
        for response in serde_json::Deserializer::from_str(&text).into_iter::<Response>() {
            for batch in response?.data {
                rows.blocks.extend(batch.blocks);
                rows.transactions.extend(batch.transactions);
                rows.logs.extend(batch.logs);
            }
        }
    }
    rows.blocks.sort_unstable_by_key(|block| block.number);
    rows.transactions
        .sort_unstable_by_key(|tx| (tx.block_number, tx.transaction_index));
    rows.logs
        .sort_unstable_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    // Answers of one chunk do not overlap, but nothing downstream should depend on that.
    rows.blocks.dedup_by_key(|block| block.number);
    rows.transactions
        .dedup_by_key(|tx| (tx.block_number, tx.transaction_index));
    rows.logs
        .dedup_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    Ok(rows)
}
