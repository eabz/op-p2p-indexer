//! Human summaries and machine-readable session output. Exact counters and per-job failures
//! remain in JSON; console rates are rounded. Neither output serializes authorization.

use std::io::{self, Write as _};
use std::path::Path;

use eyre::WrapErr as _;
use op_indexer_bench::{Config, Latency, Report};
use serde::Serialize;

use crate::cli::Cli;

#[derive(Debug, Serialize)]
pub(crate) struct Session {
    version: &'static str,
    client: &'static str,
    heavy: bool,
    repetitions: u32,
    planned_runs: usize,
    rpc_timeout_seconds: f64,
    retry_for_seconds: f64,
    max_message_bytes: usize,
    pub(crate) runs: Vec<Report>,
}

impl Session {
    pub(crate) fn new(args: &Cli) -> Self {
        let Config {
            rpc_timeout,
            retry_for,
            max_message_bytes,
            ..
        } = args.config(op_indexer_api::ticket::Table::Blocks);
        Self {
            version: env!("CARGO_PKG_VERSION"),
            client: "rust-arrow-flight",
            heavy: args.heavy,
            repetitions: args.repeat,
            planned_runs: args.tables().len() * usize::try_from(args.repeat).unwrap_or(10000),
            rpc_timeout_seconds: rpc_timeout.as_secs_f64(),
            retry_for_seconds: retry_for.as_secs_f64(),
            max_message_bytes,
            runs: Vec::new(),
        }
    }

    pub(crate) async fn save(&self, path: Option<&Path>) -> eyre::Result<()> {
        if let Some(path) = path {
            let bytes = tokio::task::block_in_place(|| serde_json::to_vec_pretty(self))?;
            let mut temporary = path.as_os_str().to_os_string();
            temporary.push(format!(".tmp-{}", std::process::id()));
            let temporary = std::path::PathBuf::from(temporary);
            tokio::fs::write(&temporary, bytes)
                .await
                .wrap_err_with(|| format!("failed to write {}", path.display()))?;
            tokio::fs::rename(&temporary, path)
                .await
                .wrap_err_with(|| format!("failed to replace {}", path.display()))?;
        }
        Ok(())
    }
}

pub(crate) fn summary(report: &Report) -> io::Result<()> {
    let mut out = io::stdout().lock();
    writeln!(
        out,
        "\n{} {}..{} ({})",
        report.table, report.from, report.to, report.cap
    )?;
    writeln!(
        out,
        "jobs       {} planned: {} done, {} failed, {} retries, {:.1} s slot/backoff waiting",
        report.jobs.len(),
        report.done,
        report.failed,
        report.retries,
        report.wait_seconds
    )?;
    writeln!(
        out,
        "time       {:.3} s reading, {:.3} s planning, {:.3} s end-to-end",
        report.reading_seconds, report.planning_seconds, report.end_to_end_seconds
    )?;
    writeln!(
        out,
        "received   {} decoded bytes, {} failed/incomplete bytes (not wire bytes)",
        report.received, report.failed_bytes
    )?;
    writeln!(
        out,
        "read       {} decoded bytes, {} rows",
        report.bytes, report.rows
    )?;
    writeln!(
        out,
        "rate       {:.1} decoded MB/s; end-to-end {:.1} useful MB/s",
        report.mb_per_second, report.end_to_end_mb_per_second
    )?;
    writeln!(out, "ttfb incl queue/retries    {}", latency(&report.ttfb))?;
    writeln!(
        out,
        "latency incl queue/retries {}",
        latency(&report.latency)
    )?;
    writeln!(out, "queue                     {}", latency(&report.queue))?;
    writeln!(
        out,
        "attempt failures  exhausted {}, unavailable {}, timeout {}, other {}",
        report
            .jobs
            .iter()
            .map(|job| job.failures.exhausted)
            .sum::<u64>(),
        report
            .jobs
            .iter()
            .map(|job| job.failures.unavailable)
            .sum::<u64>(),
        report
            .jobs
            .iter()
            .map(|job| job.failures.timeout)
            .sum::<u64>(),
        report
            .jobs
            .iter()
            .map(|job| job.failures.other)
            .sum::<u64>()
    )?;
    for (server, result) in &report.servers {
        writeln!(
            out,
            "  {server:40} {} jobs  {:.1} MB/s over run  {:.1} MB/s per stream",
            result.jobs, result.mb_per_second, result.mb_per_stream_second
        )?;
    }
    if !report.complete {
        writeln!(
            out,
            "INCOMPLETE RUN: successful-subset throughput is not a full-range benchmark"
        )?;
    }
    for job in report
        .jobs
        .iter()
        .filter(|job| job.error.is_some())
        .take(10)
    {
        writeln!(
            out,
            "failed job {}: {}",
            job.index,
            job.error.as_deref().unwrap_or("unknown")
        )?;
    }
    Ok(())
}

fn latency(value: &Latency) -> String {
    match (value.median, value.p95) {
        (Some(median), Some(p95)) => format!("median {median:.3} s, p95 {p95:.3} s"),
        _ => "no samples".to_owned(),
    }
}
