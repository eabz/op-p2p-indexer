//! A bounded Flight client: validate the balancer's complete range plan, stream and discard
//! decoded batches directly from servers, then report useful work separately from retries.
//! No chain storage, ingestion or server components are needed.

mod plan;
mod read;
mod report;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use op_indexer_api::ticket::Query;
use serde::Serialize;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tonic::metadata::{Ascii, MetadataValue};
use tracing::{Instrument as _, info, info_span};

pub use report::{Failures, JobReport, Latency, Report, ServerReport};

/// IPC compression requested from each server.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// Uncompressed IPC buffers.
    None,
    /// LZ4 frame compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
}

impl Compression {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

/// One full-range benchmark. Concurrency applies across all jobs in this run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Balancer URL (`grpc://`, `grpc+tcp://` or `http://`).
    pub balancer: String,
    /// Explicit inclusive range, table and finality.
    pub query: Query,
    /// Maximum jobs admitted at once, including their retries and slot waits.
    pub concurrency: usize,
    /// Maximum active read RPCs to each distinct server in this run.
    pub per_server: usize,
    /// Requested IPC compression.
    pub compression: Compression,
    /// Planning deadline, including connection establishment.
    pub plan_timeout: Duration,
    /// Deadline for each entire stream, including connection and decoding.
    pub rpc_timeout: Duration,
    /// Job budget from admission, including slot waits and retries.
    pub retry_for: Duration,
    /// Interval between progress snapshots.
    pub progress: Duration,
    /// Maximum size of a received protobuf message, before IPC decompression.
    pub max_message_bytes: usize,
}

/// Setup, planning or task supervision failed; incomplete reads are in [`Report`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BenchError {
    /// A configuration invariant is invalid.
    #[error("invalid benchmark configuration: {0}")]
    Config(&'static str),
    /// A balancer failed to return a plan.
    #[error("flight planning failed")]
    Planning(#[source] tonic::Status),
    /// The balancer exceeded the planning deadline.
    #[error("flight planning deadline exceeded")]
    PlanTimeout,
    /// Tickets do not cover exactly the requested range.
    #[error("invalid flight plan: {0}")]
    Plan(&'static str),
    /// Cancellation before the read phase.
    #[error("benchmark cancelled before reading")]
    Cancelled,
    /// A worker panicked or could not be joined.
    #[error("benchmark worker failed")]
    Worker(#[source] tokio::task::JoinError),
}

/// Credentials are kept outside serializable configuration and redacted in debug output.
struct Auth(MetadataValue<Ascii>);

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Auth(<redacted>)")
    }
}

#[derive(Debug, Default)]
struct Progress {
    received: AtomicU64,
    failed_bytes: AtomicU64,
}

/// Reusable client configuration. Each run requests a fresh plan and fresh local permits.
#[derive(Debug)]
pub struct Benchmark {
    config: Config,
    auth: Arc<Auth>,
}

impl Benchmark {
    /// Validates limits and authorization without making a network request.
    ///
    /// # Errors
    /// Returns [`BenchError::Config`] for empty/invalid credentials, unsupported URLs,
    /// missing or reversed ranges, zero limits, excessive concurrency or invalid deadlines.
    pub fn new(config: Config, key: &str) -> Result<Self, BenchError> {
        let invalid = BenchError::Config;
        if config.query.from.is_none_or(|from| from > config.query.to) {
            return Err(invalid("an explicit, non-reversed range is required"));
        }
        if config.concurrency == 0
            || config.concurrency > 1024
            || config.per_server == 0
            || config.per_server > 1024
        {
            return Err(invalid(
                "concurrency and per-server must be within 1..=1024",
            ));
        }
        if config.max_message_bytes == 0 || config.max_message_bytes > 1024 * 1024 * 1024 {
            return Err(invalid("max-message-bytes must be within 1..=1073741824"));
        }
        for duration in [
            config.plan_timeout,
            config.rpc_timeout,
            config.retry_for,
            config.progress,
        ] {
            if duration.is_zero() || duration > Duration::from_hours(24) {
                return Err(invalid(
                    "deadlines and progress must be within (0, 86400] seconds",
                ));
            }
        }
        plan::url(&config.balancer)?;
        if key.trim().is_empty() {
            return Err(invalid("KEY must be nonempty"));
        }
        let mut auth = format!("Bearer {key}")
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_invalid| invalid("KEY is not valid ASCII metadata"))?;
        auth.set_sensitive(true);
        Ok(Self {
            config,
            auth: Arc::new(Auth(auth)),
        })
    }

    /// Reads one complete range, stopping admission and draining workers on cancellation.
    ///
    /// Decoding runs on bounded blocking workers, never the async runtime's network threads.
    /// A cancelled or failed job contributes received bytes, but no useful rows or bytes.
    ///
    /// # Errors
    /// Returns [`BenchError`] when setup, planning or task supervision fails. RPC/read
    /// failures return an incomplete [`Report`] so callers can persist partial measurements.
    pub async fn run(&self, cancel: CancellationToken) -> Result<Report, BenchError> {
        let overall = Instant::now();
        let jobs = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(BenchError::Cancelled),
            result = plan::plan(&self.config, &self.auth) => result?,
        };
        let planning_seconds = overall.elapsed().as_secs_f64();
        let began = Instant::now();
        let planned = jobs.len();
        let progress = Arc::new(Progress::default());
        let mut pending = jobs.into_iter();
        let mut workers = JoinSet::new();
        let mut results = Vec::with_capacity(planned);
        let mut interval = tokio::time::interval(self.config.progress);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;
        info!(
            planned,
            table = self.config.query.table.name(),
            concurrency = self.config.concurrency,
            per_server = self.config.per_server,
            compression = self.config.compression.name(),
            "reading range"
        );
        loop {
            while workers.len() < self.config.concurrency && !cancel.is_cancelled() {
                let Some(job) = pending.next() else { break };
                let context = read::Context {
                    config: self.config.clone(),
                    auth: Arc::clone(&self.auth),
                    progress: Arc::clone(&progress),
                    cancel: cancel.child_token(),
                    began,
                };
                workers.spawn(read::job(job, context).instrument(info_span!("read_job")));
            }
            if workers.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                () = cancel.cancelled(), if !cancel.is_cancelled() => {},
                joined = workers.join_next() => {
                    match joined {
                        Some(Ok(Ok(result))) => results.push(result),
                        Some(Ok(Err(error))) => {
                            cancel.cancel();
                            while workers.join_next().await.is_some() {}
                            return Err(error);
                        }
                        Some(Err(error)) => {
                            cancel.cancel();
                            while workers.join_next().await.is_some() {}
                            return Err(BenchError::Worker(error));
                        }
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    info!(finished = results.len(), failed = results.iter().filter(|job| job.error.is_some()).count(),
                        planned, seconds = began.elapsed().as_secs_f64(),
                        received_decoded_bytes = progress.received.load(Ordering::Relaxed),
                        failed_attempt_bytes = progress.failed_bytes.load(Ordering::Relaxed), "progress");
                }
            }
        }
        for job in pending {
            let mut result = JobReport::new(&job, began.elapsed());
            result.error = Some("cancelled before admission".to_owned());
            results.push(result);
        }
        Ok(Report::new(
            &self.config,
            planning_seconds,
            began.elapsed(),
            overall.elapsed(),
            results,
        ))
    }
}
