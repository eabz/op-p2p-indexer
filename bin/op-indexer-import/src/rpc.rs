//! The chain's JSON-RPC endpoint, read-only: what the archive service leaves out of some rows
//! (see `fill`). One method, `eth_getBlockByNumber` with full transactions (public endpoints
//! do not all allow the per-transaction methods; Unichain's does not), one block per request.
//!
//! Nothing read here is trusted: it goes into the rebuilt transaction, and the block's header
//! hash proves it or `verify` fails. The answer's block hash is compared with the one the
//! service gave, so an endpoint of another chain is caught here, with a clear error.

use std::time::Duration;

use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{B256, U64};
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tracing::debug;

/// The chain's public JSON-RPC endpoint, by chain id, where `download` may need one.
const ENDPOINTS: &[(u64, &str)] = &[(130, "https://mainnet.unichain.org")];

/// Limit for one request, a block with every transaction included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Limit for connecting to the endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Attempts per block before the step stops.
const MAX_ATTEMPTS: u32 = 6;
/// Wait before the second attempt; doubled for each further one.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
/// Longest wait between two attempts.
const BACKOFF_CAP: Duration = Duration::from_secs(20);

/// The chain's public JSON-RPC endpoint, if this build knows one.
pub(crate) fn default_endpoint(chain_id: u64) -> Option<&'static str> {
    ENDPOINTS
        .iter()
        .find(|(id, _)| *id == chain_id)
        .map(|(_, endpoint)| *endpoint)
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

    /// The authorization lists of transactions `indexes` of block `number`, whose hash must be
    /// `hash`, in the order of `indexes`. Retries what may pass, with capped, jittered backoff.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError`] once the attempts are spent or the error cannot pass.
    pub(crate) async fn authorization_lists(
        &self,
        number: u64,
        hash: B256,
        indexes: &[u64],
    ) -> Result<Vec<Vec<SignedAuthorization>>, RpcError> {
        let mut backoff = BACKOFF_BASE;
        let mut attempt = 1;
        loop {
            match self.attempt(number, hash, indexes).await {
                Err(err) if err.is_retryable() && attempt < MAX_ATTEMPTS => {
                    // Up to half of the wait is random, so parallel requests do not retry together.
                    let wait = backoff.mul_f64(1.0 - fastrand::f64() / 2.0);
                    debug!(number, %err, ?wait, "RPC request failed, retrying");
                    sleep(wait).await;
                    backoff = backoff.saturating_mul(2).min(BACKOFF_CAP);
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    async fn attempt(
        &self,
        number: u64,
        hash: B256,
        indexes: &[u64],
    ) -> Result<Vec<Vec<SignedAuthorization>>, RpcError> {
        let call = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("{number:#x}"), true],
        });
        let response = self
            .client
            .post(&self.url)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(call.to_string())
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
        let answer: Answer =
            serde_json::from_slice(&body).map_err(|err| RpcError::Malformed(err.to_string()))?;
        if let Some(error) = answer.error {
            return Err(RpcError::Refused {
                code: error.code,
                message: error.message,
            });
        }
        let block = answer.result.ok_or(RpcError::NoBlock(number))?;
        if block.hash != hash {
            return Err(RpcError::OtherBlock {
                number,
                expected: hash,
                got: block.hash,
            });
        }
        let mut transactions = block.transactions;
        indexes
            .iter()
            .map(|&index| {
                transactions
                    .iter_mut()
                    .find(|transaction| transaction.transaction_index.to::<u64>() == index)
                    .and_then(|transaction| transaction.authorization_list.take())
                    .ok_or(RpcError::NoAuthorizations { number, index })
            })
            .collect()
    }
}

/// A JSON-RPC answer.
#[derive(Debug, Deserialize)]
struct Answer {
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
    transaction_index: U64,
    authorization_list: Option<Vec<SignedAuthorization>>,
}
