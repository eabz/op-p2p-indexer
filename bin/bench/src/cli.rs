//! Command-line workload selection and validated finite bounds. Heavy mode expands tables
//! and deadlines, while leaving admission limits unchanged.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use op_indexer_api::ticket::{Cap, Query, Table};
use op_indexer_bench::{Compression, Config};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Bounded Arrow Flight benchmark (set KEY in the environment)"
)]
pub(crate) struct Cli {
    /// Balancer grpc://host:port.
    #[arg(long)]
    balancer: String,
    /// Table to read (defaults to blocks); heavy mode reads all four tables sequentially.
    #[arg(long, value_parser = ["blocks", "transactions", "receipts", "logs"], conflicts_with = "heavy")]
    table: Option<String>,
    /// Read blocks, transactions, receipts and logs, with 600s RPC / 1800s job defaults.
    #[arg(long)]
    pub(crate) heavy: bool,
    /// First block, inclusive. Heavy workloads should use an explicit representative range.
    #[arg(long)]
    from: u64,
    /// Last block, inclusive.
    #[arg(long)]
    to: u64,
    /// Finality of the requested range.
    #[arg(long, default_value = "finalized", value_parser = ["finalized", "safe", "any"])]
    cap: String,
    /// Concurrent jobs, including local slot waits and retries (1..=1024).
    #[arg(long, default_value_t = 24)]
    concurrency: usize,
    /// Active reads per server across this run (1..=1024), not discovered server capacity.
    #[arg(long, default_value_t = 8)]
    per_server: usize,
    /// IPC compression requested from servers.
    #[arg(long, default_value = "zstd", value_parser = ["none", "lz4", "zstd"])]
    compression: String,
    /// Seconds allowed for planning.
    #[arg(long, default_value = "30", value_parser = seconds)]
    plan_timeout: Duration,
    /// Seconds allowed per complete RPC (default 120; heavy: 600).
    #[arg(long, value_parser = seconds)]
    rpc_timeout: Option<Duration>,
    /// Job budget in seconds after admission, including slot waits (default 300; heavy: 1800).
    #[arg(long, value_parser = seconds)]
    retry_for: Option<Duration>,
    /// Progress interval in seconds.
    #[arg(long, default_value = "5", value_parser = seconds)]
    progress: Duration,
    /// Repeat the selected workload this many times, replanning every run.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=10000))]
    pub(crate) repeat: u32,
    /// Maximum received protobuf message bytes before IPC decompression (default 64 MiB).
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_message_bytes: usize,
    /// Save cumulative results after each run (no API key); existing files are replaced.
    #[arg(long)]
    pub(crate) json: Option<PathBuf>,
    /// Environment file, loaded before the runtime starts.
    #[arg(long)]
    env_file: Option<PathBuf>,
}

impl Cli {
    pub(crate) fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(self.from <= self.to, "--to is below --from");
        Ok(())
    }

    pub(crate) fn tables(&self) -> Vec<Table> {
        if self.heavy {
            Table::ALL.to_vec()
        } else {
            vec![
                self.table
                    .as_deref()
                    .and_then(Table::parse)
                    .unwrap_or(Table::Blocks),
            ]
        }
    }

    pub(crate) fn config(&self, table: Table) -> Config {
        let cap = match self.cap.as_str() {
            "safe" => Cap::Safe,
            "any" => Cap::Any,
            _ => Cap::Finalized,
        };
        let compression = match self.compression.as_str() {
            "none" => Compression::None,
            "lz4" => Compression::Lz4,
            _ => Compression::Zstd,
        };
        Config {
            balancer: self.balancer.clone(),
            query: Query {
                table,
                from: Some(self.from),
                to: self.to,
                cap,
            },
            concurrency: self.concurrency,
            per_server: self.per_server,
            compression,
            plan_timeout: self.plan_timeout,
            rpc_timeout: self
                .rpc_timeout
                .unwrap_or(Duration::from_secs(if self.heavy { 600 } else { 120 })),
            retry_for: self.retry_for.unwrap_or(Duration::from_secs(if self.heavy {
                1800
            } else {
                300
            })),
            progress: self.progress,
            max_message_bytes: self.max_message_bytes,
        }
    }
}

fn seconds(value: &str) -> Result<Duration, &'static str> {
    let value: f64 = value.parse().map_err(|_invalid| "expected seconds")?;
    if !value.is_finite() || value <= 0.0 || value > 86_400.0 {
        return Err("seconds must be finite and within (0, 86400]");
    }
    Duration::try_from_secs_f64(value).map_err(|_invalid| "duration cannot be represented")
}
