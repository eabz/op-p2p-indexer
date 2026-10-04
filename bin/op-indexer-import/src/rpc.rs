//! The chain's JSON-RPC endpoint, read-only: what the archive service leaves out of some rows
//! (see `fill`). One method, `eth_getBlockByNumber` with full transactions (public endpoints
//! do not all allow the per-transaction methods; Unichain's does not), several blocks per
//! request as one JSON-RPC batch.
//!
//! Nothing read here is trusted: it goes into the rebuilt transaction, and the block's header
//! hash proves it or `verify` fails. The answer's block hash is compared with the one the
//! service gave, so an endpoint of another chain is caught here, with a clear error.

use std::time::Duration;

use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::B256;
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tracing::debug;

use crate::backoff::Backoff;

/// Blocks asked for in one request: a batch of that many calls, each answered with every
/// transaction of its block.
pub(crate) const BATCH_BLOCKS: usize = 20;
/// Limit for one request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Limit for connecting to the endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The chain's public JSON-RPC endpoint, where `download` may need one and this build knows it.
pub(crate) const fn default_endpoint(chain_id: u64) -> Option<&'static str> {
    match chain_id {
        130 => Some("https://mainnet.unichain.org"),
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

/// Why a block could not be read from the endpoint.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RpcError {
    #[error("request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("the endpoint is rate limiting (HTTP 429)")]
    RateLimited,
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
            Self::Transport(_) | Self::RateLimited | Self::Malformed(_) => true,
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
}

impl Rpc {
    /// Builds the client for `url`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub(crate) fn new(url: &str) -> eyre::Result<Self> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        Ok(Self {
            client,
            url: url.to_owned(),
        })
    }

    /// The URL, for messages.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// The authorization lists `blocks` want, in their order (block by block, index by index),
    /// read with one batch request. Retries what may pass, with the importer's backoff.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError`] once the attempts are spent or the error cannot pass.
    pub(crate) async fn authorization_lists(
        &self,
        blocks: &[Wanted],
    ) -> Result<Vec<Vec<SignedAuthorization>>, RpcError> {
        let mut backoff = Backoff::new();
        loop {
            match self.attempt(blocks).await {
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

    async fn attempt(&self, blocks: &[Wanted]) -> Result<Vec<Vec<SignedAuthorization>>, RpcError> {
        let calls: Vec<_> = (0_usize..)
            .zip(blocks)
            .map(|(id, block)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "eth_getBlockByNumber",
                    "params": [format!("{:#x}", block.number), true],
                })
            })
            .collect();
        let response = self
            .client
            .post(&self.url)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(serde_json::Value::Array(calls).to_string())
            .send()
            .await?;
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(RpcError::RateLimited);
        }
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(RpcError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(body.get(..body.len().min(200)).unwrap_or_default())
                    .into_owned(),
            });
        }
        let mut answers: Vec<Answer> =
            serde_json::from_slice(&body).map_err(|err| RpcError::Malformed(err.to_string()))?;
        // A batch may be answered in any order.
        answers.sort_unstable_by_key(|answer| answer.id);
        if answers.len() != blocks.len() {
            return Err(RpcError::Malformed(format!(
                "{} answers to {} calls",
                answers.len(),
                blocks.len()
            )));
        }
        let mut lists = Vec::new();
        for (wanted, answer) in blocks.iter().zip(answers) {
            if let Some(error) = answer.error {
                return Err(RpcError::Refused {
                    code: error.code,
                    message: error.message,
                });
            }
            let number = wanted.number;
            let block = answer.result.ok_or(RpcError::NoBlock(number))?;
            if block.hash != wanted.hash {
                return Err(RpcError::OtherBlock {
                    number,
                    expected: wanted.hash,
                    got: block.hash,
                });
            }
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
}

/// A JSON-RPC answer.
#[derive(Debug, Deserialize)]
struct Answer {
    id: usize,
    result: Option<Block>,
    error: Option<CallError>,
}

#[derive(Debug, Deserialize)]
struct CallError {
    code: i64,
    message: String,
}

/// The parts of a block read here.
#[derive(Debug, Deserialize)]
struct Block {
    hash: B256,
    transactions: Vec<Transaction>,
}

/// The parts of a transaction read here.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Transaction {
    authorization_list: Option<Vec<SignedAuthorization>>,
}
