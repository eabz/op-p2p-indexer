//! Where blocks are downloaded from: the [`Source`] trait and its `HyperSync` implementation,
//! which also reads logs of the L1 chain for the lookup of dispute games (see `game`).
//!
//! A source answers "blocks `from..to`" with one page: a JSON document in the row schema of
//! `rows`, kept on disk exactly as received, and the block the next request must start at (a
//! page may cover less than was asked). Nothing is verified here.
//!
//! The API token goes into the request header and nowhere else: not into errors, logs or
//! files.

use std::future::Future;
use std::time::Duration;

use alloy_primitives::B256;
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

use crate::cli::ApiToken;
use crate::game::{L1Log, L1Logs, LogFilter};
use crate::rows::{L1TransactionRow, LogRow};

/// Limit for one request, response body included. A full chunk answered in about a second
/// when measured.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Limit for connecting to the service.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Block fields requested: every header field of every fork `HyperSync` has a column for, except
/// the bloom, which is recomputed from the logs and proven by the header hash.
const BLOCK_FIELDS: &[&str] = &[
    "number",
    "hash",
    "parent_hash",
    "sha3_uncles",
    "miner",
    "state_root",
    "transactions_root",
    "receipts_root",
    "difficulty",
    "gas_limit",
    "gas_used",
    "timestamp",
    "extra_data",
    "mix_hash",
    "nonce",
    "base_fee_per_gas",
    "withdrawals_root",
    "blob_gas_used",
    "excess_blob_gas",
    "parent_beacon_block_root",
];

/// Transaction fields requested: what the transaction and its receipt are rebuilt from (the
/// names are those of `HyperSync`'s schema, `hypersync-schema` 0.4; a field a transaction does
/// not have costs nothing in the answer), and
/// the L1 fee fields of the client before Bedrock, which no root covers and which only stay in
/// the downloaded chunks.
const TRANSACTION_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
    "hash",
    "from",
    "to",
    "gas",
    "gas_price",
    "input",
    "value",
    "nonce",
    "v",
    "r",
    "s",
    "type",
    "status",
    "root",
    "cumulative_gas_used",
    "chain_id",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "y_parity",
    "access_list",
    "authorization_list",
    "source_hash",
    "mint",
    "deposit_nonce",
    "deposit_receipt_version",
    "l1_fee",
    "l1_gas_price",
    "l1_gas_used",
    "l1_fee_scalar",
];

/// Log fields requested.
const LOG_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
    "log_index",
    "address",
    "data",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
];

/// L1 transaction fields requested with the logs of a lookup on L1.
const L1_TRANSACTION_FIELDS: &[&str] = &["block_number", "transaction_index", "to", "input"];

/// One answer of a source.
#[derive(Debug)]
pub(crate) struct Page {
    /// The answer as received: a JSON document `rows::Response` parses.
    pub(crate) body: Bytes,
    /// First block the answer does not cover.
    pub(crate) next_block: u64,
}

/// Why a request to the source failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SourceError {
    /// The service refuses requests for now: the phase stops and is run again later.
    #[error("the service is rate limiting (HTTP 429)")]
    RateLimited,
    /// The token was refused: retrying cannot help.
    #[error("the service refused the API token (HTTP {0})")]
    Unauthorized(u16),
    #[error("the service answered HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("malformed answer: {0}")]
    Malformed(String),
    /// A lookup on L1 failed: which request, and why.
    #[error("{request}: {source}")]
    Lookup { request: String, source: Box<Self> },
}

impl SourceError {
    /// Whether the same request may succeed if sent again.
    pub(crate) const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Status { .. } | Self::Transport(_) | Self::Malformed(_)
        )
    }
}

/// An archive that serves blocks, their transactions and their logs by block range.
pub(crate) trait Source: Send + Sync + 'static {
    /// Fetches blocks `from..to` (`to` excluded), or a prefix of them.
    fn fetch(&self, from: u64, to: u64) -> impl Future<Output = Result<Page, SourceError>> + Send;
}

/// Envio `HyperSync`'s JSON query API.
///
/// See <https://docs.envio.dev/docs/HyperSync/hypersync-query>.
#[derive(Debug, Clone)]
pub(crate) struct HyperSync {
    client: Client,
    query_url: String,
    height_url: String,
}

impl HyperSync {
    /// Builds the client for `endpoint` (the chain's `HyperSync` URL, without a path).
    ///
    /// # Errors
    ///
    /// Returns an error if the token is not a valid header value or the HTTP client cannot be
    /// built. Neither message contains the token.
    pub(crate) fn new(endpoint: &str, token: &ApiToken) -> eyre::Result<Self> {
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
            .map_err(|_invalid| eyre::eyre!("the API token contains invalid characters"))?;
        bearer.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, bearer);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .default_headers(headers)
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        let endpoint = endpoint.trim_end_matches('/');
        Ok(Self {
            client,
            query_url: format!("{endpoint}/query"),
            height_url: format!("{endpoint}/height"),
        })
    }
}

impl HyperSync {
    /// Sends one query and returns the answer's body.
    async fn query(&self, query: &serde_json::Value) -> Result<Bytes, SourceError> {
        let request = self.client.post(&self.query_url).body(query.to_string());
        Self::answer(request.send().await?).await
    }

    /// Maps the status of `response` to an error, or returns its body.
    async fn answer(response: reqwest::Response) -> Result<Bytes, SourceError> {
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(SourceError::RateLimited);
        }
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(SourceError::Unauthorized(status.as_u16()));
        }
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(SourceError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(body.get(..body.len().min(200)).unwrap_or_default())
                    .into_owned(),
            });
        }
        Ok(body)
    }
}

impl Source for HyperSync {
    async fn fetch(&self, from: u64, to: u64) -> Result<Page, SourceError> {
        let query = json!({
            "from_block": from,
            "to_block": to,
            "include_all_blocks": true,
            "transactions": [{}],
            "logs": [{}],
            "field_selection": {
                "block": BLOCK_FIELDS,
                "transaction": TRANSACTION_FIELDS,
                "log": LOG_FIELDS,
            },
        });
        let body = self.query(&query).await?;
        // Megabytes of JSON are scanned to find the cursor: off the runtime.
        let (body, cursor) = tokio::task::spawn_blocking(move || {
            let cursor = serde_json::from_slice::<Cursor>(&body);
            (body, cursor)
        })
        .await
        .map_err(|err| SourceError::Malformed(err.to_string()))?;
        let cursor = cursor.map_err(|err| SourceError::Malformed(err.to_string()))?;
        Ok(Page {
            body,
            next_block: cursor.next_block,
        })
    }
}

/// The L1 chain's endpoint: logs by contract and topics, each with the transaction that
/// emitted it. Answers are a few logs, so they are parsed in place.
impl L1Logs for HyperSync {
    type Error = SourceError;

    async fn height(&self) -> Result<u64, SourceError> {
        let height = async {
            let body = Self::answer(self.client.get(&self.height_url).send().await?).await?;
            serde_json::from_slice::<Height>(&body)
                .map(|answer| answer.height)
                .map_err(|err| SourceError::Malformed(err.to_string()))
        };
        height.await.map_err(|err| SourceError::Lookup {
            request: format!("GET {}", self.height_url),
            source: Box::new(err),
        })
    }

    async fn logs(&self, filter: &LogFilter) -> Result<Vec<L1Log>, SourceError> {
        // No address would mean every contract.
        if filter.addresses.is_empty() {
            return Ok(Vec::new());
        }
        // A position without a topic matches any.
        let topic = |topic: Option<B256>| topic.into_iter().collect::<Vec<_>>();
        let mut logs = Vec::new();
        let mut from = filter.from_block;
        loop {
            let query = json!({
                "from_block": from,
                "logs": [{
                    "address": filter.addresses,
                    "topics": [[filter.topic0], topic(filter.topic1), topic(filter.topic2)],
                }],
                "field_selection": {
                    "log": LOG_FIELDS,
                    "transaction": L1_TRANSACTION_FIELDS,
                },
            });
            let answer = async {
                let body = self.query(&query).await?;
                serde_json::from_slice::<L1Answer>(&body)
                    .map_err(|err| SourceError::Malformed(err.to_string()))
            };
            let answer = answer.await.map_err(|err| SourceError::Lookup {
                request: format!("POST {} {query}", self.query_url),
                source: Box::new(err),
            })?;
            for batch in answer.data {
                logs.extend(batch.logs.iter().map(|log| {
                    let transaction = batch.transactions.iter().find(|tx| {
                        (tx.block_number, tx.transaction_index)
                            == (log.block_number, log.transaction_index)
                    });
                    L1Log {
                        block_number: log.block_number,
                        log_index: log.log_index,
                        address: log.address,
                        topics: [log.topic0, log.topic1, log.topic2, log.topic3],
                        transaction_to: transaction.and_then(|tx| tx.to),
                        transaction_input: transaction
                            .map(|tx| tx.input.clone())
                            .unwrap_or_default(),
                    }
                }));
            }
            let at_head = answer
                .archive_height
                .is_none_or(|height| answer.next_block > height);
            if at_head || answer.next_block <= from {
                return Ok(logs);
            }
            from = answer.next_block;
        }
    }
}

/// The answer to the height request.
#[derive(Debug, Deserialize)]
struct Height {
    height: u64,
}

/// An answer to a log query on L1.
#[derive(Debug, Deserialize)]
struct L1Answer {
    data: Vec<L1Batch>,
    next_block: u64,
    /// Newest block the service has; absent means the answer reached it.
    archive_height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct L1Batch {
    #[serde(default)]
    logs: Vec<LogRow>,
    #[serde(default)]
    transactions: Vec<L1TransactionRow>,
}

/// The part of an answer the downloader reads; the rest is parsed by `verify`.
#[derive(Debug, Deserialize)]
struct Cursor {
    next_block: u64,
}
