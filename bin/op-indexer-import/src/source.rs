//! Where blocks are downloaded from: Envio `HyperSync`'s JSON query API, for the chain's
//! blocks and for logs of the L1 chain (the lookup of dispute games, see `game`).
//!
//! A query for blocks `from..to` is answered with one JSON document in the row schema of
//! `rows`, which may cover less than was asked; it ends with the block the next query must
//! start at. The answer is passed on exactly as it travels, in the content encoding the
//! service chose, piece by piece and never held whole: nothing is decompressed to be
//! compressed again. The only work per byte is decoding the stream once, into nothing, to
//! read that last number. Nothing is verified here.
//!
//! Requests use HTTP/1.1, one connection each: over HTTP/2 every request in flight would
//! share one connection, so one congestion window and one core for its decryption.
//!
//! The API token goes into the request header and nowhere else: not into errors, logs or
//! files.

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use bytes::Bytes;
use reqwest::header::{
    ACCEPT_ENCODING as ACCEPT_ENCODING_HEADER, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE,
    HeaderMap, HeaderValue,
};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::cli::Secret;
use crate::game::{L1Log, LogFilter};
use crate::rows::{L1TransactionRow, LogRow};

/// Limit for one request, response body included. A legacy chunk answers in about a second;
/// a chunk of full blocks is hundreds of megabytes written to disk as it arrives.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// Limit for the service to send nothing while an answer is being read.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Limit for connecting to the service.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Block fields requested: what the header is rebuilt from, of every fork `HyperSync` has a
/// column for. Left out because `verify` computes them and the header hash proves them: the
/// bloom (from the logs), the transactions root and the receipts root.
const BLOCK_FIELDS: &[&str] = &[
    "number",
    "hash",
    "parent_hash",
    "sha3_uncles",
    "miner",
    "state_root",
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

/// Transaction fields requested: what the transaction and its receipt are rebuilt from (not
/// its hash: it is the keccak of the encoding `verify` rebuilds and proves). The
/// names are those of `HyperSync`'s schema (`hypersync-schema` 0.4); a field a transaction
/// does not have costs nothing in the answer.
const TRANSACTION_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
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

/// Bytes kept from the end of an answer to read its cursor from. The cursor and the two
/// numbers next to it follow the rows; at the chain's tip a rollback guard of a few hundred
/// bytes follows them.
const TAIL_BYTES: usize = 4096;

/// The key of the cursor in an answer.
const CURSOR_KEY: &[u8] = b"\"next_block\":";

/// The content encodings asked for, cheapest to decode first.
const ACCEPT_ENCODING: &str = "zstd, gzip;q=0.5";
/// The service's endpoint of each chain this build knows, by chain id. Unichain's is the
/// host the service's naming gives; it has not been reached from here.
const ENDPOINTS: &[(u64, &str)] = &[
    (10, "https://optimism.hypersync.xyz"),
    (130, "https://unichain.hypersync.xyz"),
];

/// The service's endpoint of chain `chain_id`, if this build knows it.
pub(crate) fn default_endpoint(chain_id: u64) -> Option<&'static str> {
    ENDPOINTS
        .iter()
        .find(|(id, _)| *id == chain_id)
        .map(|(_, endpoint)| *endpoint)
}

/// How an answer's bytes are encoded, as its `Content-Encoding` says. The number is the
/// first byte of a chunk file, which holds answers of one encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Encoding {
    Identity = 0,
    Gzip = 1,
    Zstd = 2,
}

impl Encoding {
    /// Reads the encoding from the first byte of a chunk file.
    pub(crate) const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Identity),
            1 => Some(Self::Gzip),
            2 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// What one answer held.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fetched {
    /// First block the answer does not cover.
    pub(crate) next_block: u64,
}

/// What the requests in flight have done, counted as it happens.
#[derive(Debug, Default)]
pub(crate) struct Meters {
    /// Bytes of answer bodies received, as they travelled.
    pub(crate) wire_bytes: AtomicU64,
    /// Time spent decoding answers to find their cursor, in nanoseconds, summed over all
    /// requests.
    pub(crate) decode_nanos: AtomicU64,
}

/// The end of a stream written to it: where an answer's cursor is.
#[derive(Debug, Default)]
struct Tail(Vec<u8>);

impl Write for Tail {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = bytes.get(bytes.len().saturating_sub(TAIL_BYTES)..);
        self.0.extend_from_slice(end.unwrap_or(bytes));
        if self.0.len() > 2 * TAIL_BYTES {
            self.0.drain(..self.0.len().saturating_sub(TAIL_BYTES));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Decodes an answer as it arrives and keeps only its end.
enum TailDecoder {
    Identity(Tail),
    Gzip(Box<flate2::write::GzDecoder<Tail>>),
    Zstd(zstd::stream::write::Decoder<'static, Tail>),
}

impl TailDecoder {
    fn new(encoding: Encoding) -> io::Result<Self> {
        Ok(match encoding {
            Encoding::Identity => Self::Identity(Tail::default()),
            Encoding::Gzip => Self::Gzip(Box::new(flate2::write::GzDecoder::new(Tail::default()))),
            Encoding::Zstd => Self::Zstd(zstd::stream::write::Decoder::new(Tail::default())?),
        })
    }

    fn write(&mut self, piece: &[u8]) -> io::Result<()> {
        match self {
            Self::Identity(tail) => tail.write_all(piece),
            Self::Gzip(decoder) => decoder.write_all(piece),
            Self::Zstd(decoder) => decoder.write_all(piece),
        }
    }

    /// Ends the stream and returns its last bytes.
    fn finish(self) -> io::Result<Vec<u8>> {
        Ok(match self {
            Self::Identity(tail) => tail.0,
            Self::Gzip(decoder) => decoder.finish()?.0,
            Self::Zstd(mut decoder) => {
                decoder.flush()?;
                decoder.into_inner().0
            }
        })
    }
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
    /// Whoever takes the answer stopped taking it; the reason is theirs to report.
    #[error("the answer was no longer wanted")]
    Unwanted,
    /// A lookup on L1 failed: which request, and why.
    #[error("{request}: {source}")]
    Lookup { request: String, source: Box<Self> },
}

impl SourceError {
    /// Whether the same request may succeed if sent again: the service is busy or limiting
    /// (429, 408, 5xx), the connection failed, or the answer was cut short. A refused token
    /// or a rejected query (other 4xx) fails every time.
    pub(crate) const fn is_retryable(&self) -> bool {
        match self {
            Self::RateLimited | Self::Transport(_) | Self::Malformed(_) => true,
            Self::Status { status, .. } => *status == 408 || *status >= 500,
            Self::Unauthorized(_) | Self::Unwanted | Self::Lookup { .. } => false,
        }
    }
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
    pub(crate) fn new(endpoint: &str, token: &Secret) -> eyre::Result<Self> {
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", token.expose()))
            .map_err(|_invalid| eyre::eyre!("the API token contains invalid characters"))?;
        bearer.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, bearer);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .default_headers(headers)
            .timeout(REQUEST_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .http1_only()
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
    /// Fetches blocks `from..to` (`to` excluded), or a prefix of them, passing the answer to
    /// `sink` as it travelled, piece by piece. The first piece `sink` gets is `first`'s choice:
    /// it is called with the answer's encoding before any of the body, and may refuse it.
    /// Waits while `sink` is full, so the answer is never held whole.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError`]: see [`SourceError::is_retryable`]. After an error `sink` may
    /// have received part of the answer.
    pub(crate) async fn fetch_into(
        &self,
        from: u64,
        to: u64,
        sink: &mpsc::Sender<Bytes>,
        meters: &Meters,
        accept: impl FnOnce(Encoding) -> Result<Option<Bytes>, SourceError>,
    ) -> Result<Fetched, SourceError> {
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
        let mut response = self.send(&query, ACCEPT_ENCODING).await?;
        let encoding = match response
            .headers()
            .get(CONTENT_ENCODING)
            .map(HeaderValue::as_bytes)
        {
            None | Some(b"identity") => Encoding::Identity,
            Some(b"gzip") => Encoding::Gzip,
            Some(b"zstd") => Encoding::Zstd,
            Some(other) => {
                return Err(SourceError::Malformed(format!(
                    "the answer has content encoding `{}`, which was not asked for",
                    String::from_utf8_lossy(other)
                )));
            }
        };
        if let Some(first) = accept(encoding)? {
            sink.send(first)
                .await
                .map_err(|_closed| SourceError::Unwanted)?;
        }
        let cut_short = |err: io::Error| SourceError::Malformed(format!("undecodable: {err}"));
        let mut tail = TailDecoder::new(encoding).map_err(cut_short)?;
        while let Some(piece) = response.chunk().await? {
            // Decoding a piece takes tens of microseconds: short enough for a runtime thread,
            // which also bounds the decoding to as many threads as the runtime has.
            let started = Instant::now();
            tail.write(&piece).map_err(cut_short)?;
            let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            meters.decode_nanos.fetch_add(nanos, Ordering::Relaxed);
            let bytes = u64::try_from(piece.len()).unwrap_or(u64::MAX);
            meters.wire_bytes.fetch_add(bytes, Ordering::Relaxed);
            sink.send(piece)
                .await
                .map_err(|_closed| SourceError::Unwanted)?;
        }
        let tail = tail.finish().map_err(cut_short)?;
        let next_block = cursor(&tail).ok_or_else(|| {
            SourceError::Malformed("the answer does not end with its cursor".to_owned())
        })?;
        Ok(Fetched { next_block })
    }

    /// Sends one query and returns the response once its status says it is an answer.
    async fn send(
        &self,
        query: &serde_json::Value,
        accept_encoding: &'static str,
    ) -> Result<reqwest::Response, SourceError> {
        let request = self
            .client
            .post(&self.query_url)
            .header(ACCEPT_ENCODING_HEADER, accept_encoding)
            .body(query.to_string());
        Self::accepted(request.send().await?).await
    }

    /// Sends one query and returns the whole answer: for the small answers of L1 lookups.
    async fn query(&self, query: &serde_json::Value) -> Result<Bytes, SourceError> {
        Ok(self.send(query, "identity").await?.bytes().await?)
    }

    /// Maps the status of `response` to an error, or returns it.
    async fn accepted(response: reqwest::Response) -> Result<reqwest::Response, SourceError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(SourceError::RateLimited);
        }
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(SourceError::Unauthorized(status.as_u16()));
        }
        let body = response.bytes().await?;
        Err(SourceError::Status {
            status: status.as_u16(),
            body: String::from_utf8_lossy(body.get(..body.len().min(200)).unwrap_or_default())
                .into_owned(),
        })
    }
}

/// Reads the cursor from the end of an answer: the number after the last `"next_block":`.
/// The key cannot occur inside a row: every string in the rows is hex.
fn cursor(tail: &[u8]) -> Option<u64> {
    let at = tail
        .windows(CURSOR_KEY.len())
        .rposition(|window| window == CURSOR_KEY)?;
    let digits = tail.get(at.saturating_add(CURSOR_KEY.len())..)?;
    let digits = digits.trim_ascii_start();
    let end = digits
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(digits.len());
    std::str::from_utf8(digits.get(..end)?).ok()?.parse().ok()
}

/// The L1 chain's endpoint: logs by contract and topics, each with the transaction that
/// emitted it. Answers are a few logs, so they are parsed in place.
impl HyperSync {
    /// Returns the number of the newest block the service has.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::Lookup`], naming the request, if it fails.
    pub(crate) async fn height(&self) -> Result<u64, SourceError> {
        let height = async {
            let request = self
                .client
                .get(&self.height_url)
                .header(ACCEPT_ENCODING_HEADER, "identity");
            let response = Self::accepted(request.send().await?).await?;
            let body = response.bytes().await?;
            serde_json::from_slice::<Height>(&body)
                .map(|answer| answer.height)
                .map_err(|err| SourceError::Malformed(err.to_string()))
        };
        height.await.map_err(|err| SourceError::Lookup {
            request: format!("GET {}", self.height_url),
            source: Box::new(err),
        })
    }

    /// Returns every log matching `filter`, each with its transaction, following the
    /// service's pages to the head.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::Lookup`], naming the request, if one fails or an answer is
    /// malformed.
    pub(crate) async fn logs(&self, filter: &LogFilter) -> Result<Vec<L1Log>, SourceError> {
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
