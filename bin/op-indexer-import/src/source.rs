//! Where blocks are downloaded from: the [`Source`] trait and its `HyperSync` implementation.
//!
//! A source answers "blocks `from..to`" with one page: a JSON document in the row schema of
//! `rows`, kept on disk exactly as received, and the block the next request must start at (a
//! page may cover less than was asked). Nothing is verified here.
//!
//! The API token goes into the request header and nowhere else: not into errors, logs or
//! files.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

use crate::cli::ApiToken;

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
    url: String,
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
        Ok(Self {
            client,
            url: format!("{}/query", endpoint.trim_end_matches('/')),
        })
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
        let response = self
            .client
            .post(&self.url)
            .body(query.to_string())
            .send()
            .await?;
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

/// The part of an answer the downloader reads; the rest is parsed by `verify`.
#[derive(Debug, Deserialize)]
struct Cursor {
    next_block: u64,
}
