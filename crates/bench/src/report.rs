//! Serializable measurements contain no credentials and distinguish useful output from work
//! discarded by retries. Latencies include admission queueing; MB always means decimal MB.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

use crate::plan::Job;
use crate::{Compression, Config};

/// Counters for failed read attempts, classified by gRPC status.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Failures {
    /// Server admission refused an attempt.
    pub exhausted: u64,
    /// A server or its transport was unavailable.
    pub unavailable: u64,
    /// An attempt reached its deadline.
    pub timeout: u64,
    /// Nonretryable protocol, decoding or validation failures.
    pub other: u64,
}

/// Measurements for a single planned job, including every unsuccessful attempt.
#[derive(Debug, Serialize)]
pub struct JobReport {
    /// Stable index in the balancer's plan.
    pub index: usize,
    /// First requested block.
    pub from: u64,
    /// Last requested block, inclusive.
    pub to: u64,
    /// Server used by the final attempt.
    pub server: Option<String>,
    /// Rows from the successful attempt, or zero on failure.
    pub rows: u64,
    /// Logical decoded bytes from the successful attempt, or zero on failure.
    pub bytes: u64,
    /// All logical decoded bytes, including unsuccessful attempts.
    pub received: u64,
    /// Decoded bytes discarded by failed/incomplete attempts.
    pub failed_bytes: u64,
    /// Attempts after the first.
    pub retries: u64,
    /// Failed attempts by status.
    pub failures: Failures,
    /// Time from enqueueing to worker admission.
    pub queue_seconds: f64,
    /// Cumulative local permit/backoff waiting.
    pub wait_seconds: f64,
    /// Time from enqueueing to the first decoded batch, including retries.
    pub ttfb_seconds: Option<f64>,
    /// Time from enqueueing to completion, including retries.
    pub latency_seconds: f64,
    /// Duration of the successful RPC, including connection setup and decode.
    pub read_seconds: f64,
    /// Failure message when the whole job did not complete.
    pub error: Option<String>,
}

impl JobReport {
    pub(crate) fn new(job: &Job, queued: Duration) -> Self {
        Self {
            index: job.index,
            from: job.query.from.unwrap_or(0),
            to: job.query.to,
            server: None,
            rows: 0,
            bytes: 0,
            received: 0,
            failed_bytes: 0,
            retries: 0,
            failures: Failures::default(),
            queue_seconds: queued.as_secs_f64(),
            wait_seconds: 0.0,
            ttfb_seconds: None,
            latency_seconds: 0.0,
            read_seconds: 0.0,
            error: None,
        }
    }
}

/// Latency quantiles using the Python benchmark's sorted index convention.
#[derive(Debug, Default, Serialize)]
pub struct Latency {
    /// Median, in seconds; absent when there were no samples.
    pub median: Option<f64>,
    /// 95th percentile, in seconds; absent when there were no samples.
    pub p95: Option<f64>,
}

impl Latency {
    fn new(values: impl Iterator<Item = f64>) -> Self {
        let mut values: Vec<_> = values.collect();
        values.sort_by(f64::total_cmp);
        Self {
            median: values.get(values.len() / 2).copied(),
            p95: values.get(values.len() * 95 / 100).copied(),
        }
    }
}

/// Successful output served by one endpoint.
#[derive(Debug, Default, Serialize)]
pub struct ServerReport {
    /// Completed jobs.
    pub jobs: usize,
    /// Useful decoded bytes.
    pub bytes: u64,
    /// Aggregate duration of successful streams.
    pub stream_seconds: f64,
    /// Decoded MB/s across the complete read phase.
    pub mb_per_second: f64,
    /// Decoded MB/s per successful stream.
    pub mb_per_stream_second: f64,
}

/// A full run; `complete` must be true before using it as a full-range benchmark.
#[derive(Debug, Serialize)]
pub struct Report {
    /// Whether every planned job completed.
    pub complete: bool,
    /// Requested table.
    pub table: &'static str,
    /// Requested first block.
    pub from: u64,
    /// Requested last block.
    pub to: u64,
    /// Requested finality.
    pub cap: &'static str,
    /// Requested compression.
    pub compression: Compression,
    /// Admitted jobs at a time.
    pub concurrency: usize,
    /// Local active reads per server.
    pub per_server: usize,
    /// Time spent planning.
    pub planning_seconds: f64,
    /// Read phase including queueing and completion.
    pub reading_seconds: f64,
    /// Planning plus reading.
    pub end_to_end_seconds: f64,
    /// Successful jobs.
    pub done: usize,
    /// Failed or cancelled jobs.
    pub failed: usize,
    /// Useful rows.
    pub rows: u64,
    /// Useful decoded bytes.
    pub bytes: u64,
    /// Decoded bytes across all attempts.
    pub received: u64,
    /// Decoded bytes discarded by unsuccessful attempts.
    pub failed_bytes: u64,
    /// Attempts after the first per job.
    pub retries: u64,
    /// Aggregate local slot/backoff waiting across jobs.
    pub wait_seconds: f64,
    /// Useful decoded MB/s during reading; incomplete runs are a successful subset only.
    pub mb_per_second: f64,
    /// Useful decoded MB/s including planning; incomplete runs are a successful subset only.
    pub end_to_end_mb_per_second: f64,
    /// First-batch latency including queueing and retries.
    pub ttfb: Latency,
    /// Completion latency including queueing and retries.
    pub latency: Latency,
    /// Admission queueing.
    pub queue: Latency,
    /// Per-server output and successful-stream rates.
    pub servers: BTreeMap<String, ServerReport>,
    /// Every job, including failures; sorted by plan index.
    pub jobs: Vec<JobReport>,
}

impl Report {
    pub(crate) fn new(
        config: &Config,
        planning_seconds: f64,
        reading: Duration,
        overall: Duration,
        mut jobs: Vec<JobReport>,
    ) -> Self {
        jobs.sort_by_key(|job| job.index);
        let done = jobs.iter().filter(|job| job.error.is_none()).count();
        let bytes = jobs.iter().map(|job| job.bytes).sum();
        let mut servers = BTreeMap::<String, ServerReport>::new();
        for job in jobs.iter().filter(|job| job.error.is_none()) {
            if let Some(server) = &job.server {
                let entry = servers.entry(server.clone()).or_default();
                entry.jobs += 1;
                entry.bytes += job.bytes;
                entry.stream_seconds += job.read_seconds;
            }
        }
        for server in servers.values_mut() {
            server.mb_per_second = rate(server.bytes, reading.as_secs_f64());
            server.mb_per_stream_second = rate(server.bytes, server.stream_seconds);
        }
        Self {
            complete: done == jobs.len(),
            table: config.query.table.name(),
            from: config.query.from.unwrap_or(0),
            to: config.query.to,
            cap: config.query.cap.name(),
            compression: config.compression,
            concurrency: config.concurrency,
            per_server: config.per_server,
            planning_seconds,
            reading_seconds: reading.as_secs_f64(),
            end_to_end_seconds: overall.as_secs_f64(),
            done,
            failed: jobs.len() - done,
            rows: jobs.iter().map(|job| job.rows).sum(),
            bytes,
            received: jobs.iter().map(|job| job.received).sum(),
            failed_bytes: jobs.iter().map(|job| job.failed_bytes).sum(),
            retries: jobs.iter().map(|job| job.retries).sum(),
            wait_seconds: jobs.iter().map(|job| job.wait_seconds).sum(),
            mb_per_second: rate(bytes, reading.as_secs_f64()),
            end_to_end_mb_per_second: rate(bytes, overall.as_secs_f64()),
            ttfb: Latency::new(jobs.iter().filter_map(|job| job.ttfb_seconds)),
            latency: Latency::new(
                jobs.iter()
                    .filter(|job| job.latency_seconds > 0.0)
                    .map(|job| job.latency_seconds),
            ),
            queue: Latency::new(jobs.iter().map(|job| job.queue_seconds)),
            servers,
            jobs,
        }
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "throughput is approximate; exact bytes remain in the report"
)]
fn rate(bytes: u64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        bytes as f64 / 1_000_000.0 / seconds
    } else {
        0.0
    }
}
