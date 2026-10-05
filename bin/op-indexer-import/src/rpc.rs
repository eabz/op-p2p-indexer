//! The chain's JSON-RPC endpoint, read-only: what the archive service leaves out of some rows
//! (see `fill`). Headers with `eth_getBlockByNumber` without transactions; whole blocks with
//! it and full transactions (public endpoints do not all allow the per-transaction methods;
//! Unichain's does not) and their receipts with `eth_getBlockReceipts` (else
//! `eth_getTransactionReceipt` per transaction); several calls per request as one JSON-RPC
//! batch, of a size the operator sets ([`Rpc::new`]).
//!
//! Nothing read here is trusted: it goes into the rebuilt block, and the block's header hash
//! proves it or `verify` fails. The answer's block hash is compared with the one the service
//! gave, so an endpoint of another chain is caught here, with a clear error.

use std::time::Duration;

use alloy_eips::BlockNumHash;
use alloy_eips::eip2930::AccessList;
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{Address, B256, Bytes, U8, U64, U128, U256};
use reqwest::header::{CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::backoff::Backoff;

/// Limit for one request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Limit for connecting to the endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest wait a `Retry-After` is taken for.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(600);

/// The chain's public JSON-RPC endpoint, where `download` may need one and this build knows it.
pub(crate) const fn default_endpoint(chain_id: u64) -> Option<&'static str> {
    match chain_id {
        130 => Some("https://mainnet.unichain.org"),
        8453 => Some("https://mainnet.base.org"),
        _ => None,
    }
}

/// A block whose transactions `indexes` lack their authorization list.
#[derive(Debug)]
pub(crate) struct Wanted {
    pub(crate) number: u64,
    /// The block's hash in the download, which the endpoint's must equal.
    pub(crate) hash: B256,
    pub(crate) indexes: Vec<u64>,
}

/// The header fields of a block the service may leave out, in the RPC's form; each absent
/// where the block's fork has none.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcHeader {
    pub(crate) number: U64,
    /// Checked against the download's when fetched; not kept in the fill.
    #[serde(default, skip_serializing)]
    hash: B256,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mix_hash: Option<B256>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) base_fee_per_gas: Option<U64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) withdrawals_root: Option<B256>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) blob_gas_used: Option<U64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) excess_blob_gas: Option<U64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) parent_beacon_block_root: Option<B256>,
}

/// A whole block's transactions and receipts, in the RPC's form: the parts the rebuild reads.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FilledBlock {
    pub(crate) number: u64,
    pub(crate) transactions: Vec<RpcTransaction>,
    pub(crate) receipts: Vec<RpcReceipt>,
}

/// A transaction as the RPC gives it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcTransaction {
    pub(crate) hash: B256,
    pub(crate) transaction_index: U64,
    pub(crate) from: Address,
    pub(crate) to: Option<Address>,
    pub(crate) gas: U64,
    pub(crate) gas_price: Option<U128>,
    pub(crate) input: Bytes,
    pub(crate) value: U256,
    pub(crate) nonce: U64,
    pub(crate) v: Option<U256>,
    pub(crate) r: Option<U256>,
    pub(crate) s: Option<U256>,
    #[serde(rename = "type")]
    pub(crate) kind: U8,
    pub(crate) chain_id: Option<U64>,
    pub(crate) max_fee_per_gas: Option<U128>,
    pub(crate) max_priority_fee_per_gas: Option<U128>,
    pub(crate) y_parity: Option<U256>,
    pub(crate) access_list: Option<AccessList>,
    pub(crate) authorization_list: Option<Vec<SignedAuthorization>>,
    pub(crate) source_hash: Option<B256>,
    pub(crate) mint: Option<U128>,
}

/// A receipt as the RPC gives it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcReceipt {
    pub(crate) status: Option<U8>,
    pub(crate) root: Option<B256>,
    pub(crate) cumulative_gas_used: U64,
    pub(crate) deposit_nonce: Option<U64>,
    pub(crate) deposit_receipt_version: Option<U64>,
    pub(crate) logs: Vec<RpcLog>,
}

/// A log as the RPC gives it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcLog {
    pub(crate) address: Address,
    pub(crate) data: Bytes,
    pub(crate) topics: Vec<B256>,
    pub(crate) log_index: U64,
}

/// Why a block could not be read from the endpoint.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RpcError {
    #[error("request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("the endpoint is limiting the request rate")]
    RateLimited {
        /// How long the endpoint asks to wait (`Retry-After`, in seconds), if it says.
        retry_after: Option<Duration>,
    },
    #[error("the endpoint answered HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("the endpoint refused the call: {message} (code {code})")]
    Refused { code: i64, message: String },
    #[error("malformed answer: {0}")]
    Malformed(String),
    #[error("the endpoint does not have block {0}")]
    NoBlock(u64),
    #[error(
        "block {number} has hash {got} at the endpoint, {expected} in the download: the \
         endpoint is not on this chain"
    )]
    OtherBlock {
        number: u64,
        expected: B256,
        got: B256,
    },
    #[error("transaction {index} of block {number} has no authorization list at the endpoint")]
    NoAuthorizations { number: u64, index: u64 },
}

impl RpcError {
    /// Whether the same request may succeed if sent again: the endpoint is busy or limiting,
    /// or the connection failed. A refused call, an unknown block or another chain fail every
    /// time.
    const fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::RateLimited { .. } | Self::Malformed(_) => true,
            Self::Status { status, .. } => *status == 408 || *status >= 500,
            Self::Refused { .. }
            | Self::NoBlock(_)
            | Self::OtherBlock { .. }
            | Self::NoAuthorizations { .. } => false,
        }
    }
}

/// A JSON-RPC endpoint of the chain.
#[derive(Debug, Clone)]
pub(crate) struct Rpc {
    client: Client,
    url: String,
    /// Calls in one request; at least two (a whole block takes two).
    batch_calls: usize,
}

impl Rpc {
    /// Builds the client for `url`, sending `batch` calls per request (Unichain's public
    /// endpoint refuses more than 10).
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub(crate) fn new(url: &str, batch: usize) -> eyre::Result<Self> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        Ok(Self {
            client,
            url: url.to_owned(),
            batch_calls: batch.max(2),
        })
    }

    /// The URL, for messages.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Calls in one request.
    pub(crate) const fn batch_calls(&self) -> usize {
        self.batch_calls
    }

    /// The headers of `blocks`, in their order, read with one batch request: at most
    /// [`Self::batch_calls`] blocks.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError`] once the attempts are spent or the error cannot pass.
    pub(crate) async fn headers(
        &self,
        blocks: &[BlockNumHash],
    ) -> Result<Vec<RpcHeader>, RpcError> {
        let calls: Vec<_> = blocks
            .iter()
            .map(|block| {
                (
                    "eth_getBlockByNumber",
                    json!([format!("{:#x}", block.number), false]),
                )
            })
            .collect();
        let mut headers = Vec::with_capacity(blocks.len());
        for (wanted, answer) in blocks.iter().zip(self.batch(&calls).await?) {
            let header =
                parse::<Option<RpcHeader>>(answer?)?.ok_or(RpcError::NoBlock(wanted.number))?;
            if header.hash != wanted.hash {
                return Err(RpcError::OtherBlock {
                    number: wanted.number,
                    expected: wanted.hash,
                    got: header.hash,
                });
            }
            headers.push(header);
        }
        Ok(headers)
    }

    /// The authorization lists `blocks` want, in their order (block by block, index by index),
    /// read with one batch request: at most [`Self::batch_calls`] blocks.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError`] once the attempts are spent or the error cannot pass.
    pub(crate) async fn authorization_lists(
        &self,
        blocks: &[Wanted],
    ) -> Result<Vec<Vec<SignedAuthorization>>, RpcError> {
        let calls: Vec<_> = blocks
            .iter()
            .map(|block| block_call(block.number))
            .collect();
        let mut lists = Vec::new();
        for (wanted, answer) in blocks.iter().zip(self.batch(&calls).await?) {
            let number = wanted.number;
            let block: Block<AuthorizedTransaction> = block(answer?, number, wanted.hash)?;
            let mut transactions = block.transactions;
            for &index in &wanted.indexes {
                // Transactions come in block order; the header hash proves what is taken.
                let list = usize::try_from(index)
                    .ok()
                    .and_then(|at| transactions.get_mut(at))
                    .and_then(|transaction| transaction.authorization_list.take())
                    .ok_or(RpcError::NoAuthorizations { number, index })?;
                lists.push(list);
            }
        }
        Ok(lists)
    }

    /// The transactions and receipts of the blocks `holes`, in their order, read with one batch
    /// request (two calls a block: at most half of [`Self::batch_calls`] blocks). A block whose receipts
    /// the endpoint will not give at once is read again receipt by receipt.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError`] once the attempts are spent or the error cannot pass.
    pub(crate) async fn whole_blocks(
        &self,
        holes: &[BlockNumHash],
    ) -> Result<Vec<FilledBlock>, RpcError> {
        let calls: Vec<_> = holes
            .iter()
            .flat_map(|hole| [block_call(hole.number), receipts_call(hole.number)])
            .collect();
        let mut answers = self.batch(&calls).await?.into_iter();
        let mut filled = Vec::new();
        for hole in holes {
            let (Some(found), Some(receipts)) = (answers.next(), answers.next()) else {
                return Err(RpcError::Malformed("fewer answers than calls".to_owned()));
            };
            let block: Block<RpcTransaction> = block(found?, hole.number, hole.hash)?;
            let receipts = match receipts {
                Ok(receipts) => parse::<Option<Vec<RpcReceipt>>>(receipts)?
                    .ok_or(RpcError::NoBlock(hole.number))?,
                Err(err) => {
                    debug!(number = hole.number, %err, "no block receipts; reading them one by one");
                    self.receipts(&block.transactions).await?
                }
            };
            if receipts.len() != block.transactions.len() {
                return Err(RpcError::Malformed(format!(
                    "block {}: {} receipts for {} transactions",
                    hole.number,
                    receipts.len(),
                    block.transactions.len()
                )));
            }
            filled.push(FilledBlock {
                number: hole.number,
                transactions: block.transactions,
                receipts,
            });
        }
        Ok(filled)
    }

    /// The receipts of `transactions`, by hash, [`Self::batch_calls`] per request.
    async fn receipts(&self, transactions: &[RpcTransaction]) -> Result<Vec<RpcReceipt>, RpcError> {
        let mut receipts = Vec::new();
        for part in transactions.chunks(self.batch_calls) {
            let calls: Vec<_> = part
                .iter()
                .map(|tx| ("eth_getTransactionReceipt", json!([tx.hash])))
                .collect();
            for (tx, answer) in part.iter().zip(self.batch(&calls).await?) {
                let receipt = parse::<Option<RpcReceipt>>(answer?)?;
                receipts.push(receipt.ok_or_else(|| {
                    RpcError::Malformed(format!("no receipt for transaction {}", tx.hash))
                })?);
            }
        }
        Ok(receipts)
    }

    /// Sends `calls` as one batch and returns each call's result, in order: a call the
    /// endpoint refused is an error of its own. Retries what may pass, with the importer's
    /// backoff, slower for a rate limit.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError`] once the attempts are spent or the request cannot pass.
    async fn batch(&self, calls: &[Call]) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        let (mut backoff, mut limited) = (Backoff::new(), Backoff::rate_limited());
        loop {
            match self.attempt(calls).await {
                Err(RpcError::RateLimited { retry_after }) => {
                    let Some(wait) = limited.next() else {
                        return Err(RpcError::RateLimited { retry_after });
                    };
                    // What the endpoint asks for, within reason.
                    let wait = retry_after.map_or(wait, |after| after.min(MAX_RETRY_AFTER));
                    warn!(endpoint = %self.url, ?wait, "the RPC endpoint is rate limiting; waiting");
                    sleep(wait).await;
                }
                Err(err) if err.is_retryable() => {
                    let Some(wait) = backoff.next() else {
                        return Err(err);
                    };
                    debug!(%err, ?wait, "RPC request failed, retrying");
                    sleep(wait).await;
                }
                result => return result,
            }
        }
    }

    async fn attempt(&self, calls: &[Call]) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        let calls: Vec<Value> = (0_usize..)
            .zip(calls)
            .map(|(id, (method, params))| {
                json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
            })
            .collect();
        let count = calls.len();
        let response = self
            .client
            .post(&self.url)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(Value::Array(calls).to_string())
            .send()
            .await?;
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()?.trim().parse().ok())
                .map(Duration::from_secs);
            return Err(RpcError::RateLimited { retry_after });
        }
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(RpcError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(body.get(..body.len().min(200)).unwrap_or_default())
                    .into_owned(),
            });
        }
        let answers: Answers =
            serde_json::from_slice(&body).map_err(|err| RpcError::Malformed(err.to_string()))?;
        let mut answers = match answers {
            Answers::Batch(answers) => answers,
            // One error for the whole batch: the rate, or the call refused (too large, say).
            Answers::One(Answer {
                error: Some(error), ..
            }) if error.is_rate_limit() => {
                return Err(RpcError::RateLimited { retry_after: None });
            }
            Answers::One(Answer {
                error: Some(error), ..
            }) => return Err(error.into()),
            Answers::One(_) => {
                return Err(RpcError::Malformed("one answer to a batch".to_owned()));
            }
        };
        // A batch may be answered in any order.
        answers.sort_unstable_by_key(|answer| answer.id);
        if answers.len() != count {
            return Err(RpcError::Malformed(format!(
                "{} answers to {count} calls",
                answers.len()
            )));
        }
        // A call refused for the rate is the whole request's to wait for, not that call's.
        if answers
            .iter()
            .any(|answer| answer.error.as_ref().is_some_and(CallError::is_rate_limit))
        {
            return Err(RpcError::RateLimited { retry_after: None });
        }
        Ok(answers
            .into_iter()
            .map(|answer| match answer.error {
                Some(error) => Err(error.into()),
                None => Ok(answer.result.unwrap_or(Value::Null)),
            })
            .collect())
    }
}

/// A call: the method and its parameters; its id is set when it is sent.
type Call = (&'static str, Value);

/// The call for block `number` with its full transactions.
fn block_call(number: u64) -> Call {
    (
        "eth_getBlockByNumber",
        json!([format!("{number:#x}"), true]),
    )
}

/// The call for the receipts of block `number`.
fn receipts_call(number: u64) -> Call {
    ("eth_getBlockReceipts", json!([format!("{number:#x}")]))
}

/// Parses a call's result.
fn parse<T: DeserializeOwned>(result: Value) -> Result<T, RpcError> {
    serde_json::from_value(result).map_err(|err| RpcError::Malformed(err.to_string()))
}

/// Parses block `number` from a call's result, checking that its hash is `hash`.
fn block<T: DeserializeOwned>(
    result: Value,
    number: u64,
    hash: B256,
) -> Result<Block<T>, RpcError> {
    let block: Block<T> = parse::<Option<Block<T>>>(result)?.ok_or(RpcError::NoBlock(number))?;
    if block.hash != hash {
        return Err(RpcError::OtherBlock {
            number,
            expected: hash,
            got: block.hash,
        });
    }
    Ok(block)
}

/// What a batch is answered with: an answer per call, or one error for all of them.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Answers {
    Batch(Vec<Answer>),
    One(Answer),
}

/// A JSON-RPC answer.
#[derive(Debug, Deserialize)]
struct Answer {
    /// Null in an error about the whole batch.
    id: Option<usize>,
    result: Option<Value>,
    error: Option<CallError>,
}

#[derive(Debug, Deserialize)]
struct CallError {
    code: i64,
    message: String,
}

impl CallError {
    /// Whether the call was refused for the request rate: some endpoints say so in the answer
    /// rather than with HTTP 429 (Base's public endpoint: -32016 "over rate limit"). Told by the
    /// message, not the code: -32005 is a rate limit with some providers and "too many results"
    /// with others.
    fn is_rate_limit(&self) -> bool {
        let message = self.message.to_ascii_lowercase();
        message.contains("rate limit") || message.contains("too many requests")
    }
}

impl From<CallError> for RpcError {
    fn from(error: CallError) -> Self {
        Self::Refused {
            code: error.code,
            message: error.message,
        }
    }
}

/// The parts of a block read here.
#[derive(Debug, Deserialize)]
#[serde(bound = "T: DeserializeOwned")]
struct Block<T> {
    hash: B256,
    transactions: Vec<T>,
}

/// The part of a transaction an authorization list is read from.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizedTransaction {
    authorization_list: Option<Vec<SignedAuthorization>>,
}
