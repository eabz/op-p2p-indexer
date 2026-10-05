//! Native Flight benchmark CLI. Runs individual tables or a heavy four-table workload,
//! prints summaries, and optionally saves every run and job as credential-free JSON.

mod cli;
mod output;

use eyre::WrapErr as _;
use op_indexer_bench::Benchmark;
use tokio_util::sync::CancellationToken;
use tracing::info;

fn main() -> eyre::Result<()> {
    if op_indexer_runtime::version_requested(env!("CARGO_BIN_NAME"), env!("CARGO_PKG_VERSION")) {
        return Ok(());
    }
    let config_file = op_indexer_runtime::config::initialize("bench")?;
    if op_indexer_runtime::config::check_requested() {
        op_indexer_runtime::init_tracing(config_file.as_deref());
        info!("configuration valid; workload requirements are checked when the benchmark runs");
        return Ok(());
    }
    let args = cli::Cli::configured();
    args.validate()?;
    let key = op_indexer_runtime::setting("BENCH_API_KEY")
        .ok_or_else(|| eyre::eyre!("set bench.api_key in TOML to an API key the servers accept"))?;
    // Validate every configuration before starting the suite.
    let clients = args
        .tables()
        .into_iter()
        .map(|table| Benchmark::new(args.config(table), &key))
        .collect::<Result<Vec<_>, _>>()?;
    drop(key);
    op_indexer_runtime::init_tracing(config_file.as_deref());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("failed to start benchmark runtime")?
        .block_on(run(args, clients))
}

async fn run(args: cli::Cli, clients: Vec<Benchmark>) -> eyre::Result<()> {
    let cancel = CancellationToken::new();
    let mut session = output::Session::new(&args);
    let suite = async {
        for repetition in 1..=args.repeat {
            for client in &clients {
                if cancel.is_cancelled() {
                    eyre::bail!("benchmark cancelled");
                }
                info!(
                    repetition,
                    total = args.repeat,
                    heavy = args.heavy,
                    "starting benchmark run"
                );
                let report = client.run(cancel.child_token()).await?;
                let printed = output::summary(&report);
                let complete = report.complete;
                session.runs.push(report);
                session.save(args.json.as_deref()).await?;
                printed?;
                eyre::ensure!(
                    complete,
                    "incomplete run; stopping workload (successful-subset rates are not full-range results)"
                );
            }
        }
        Ok::<(), eyre::Report>(())
    };
    tokio::pin!(suite);
    let result = tokio::select! {
        biased;
        signal = op_indexer_runtime::shutdown_signal() => {
            cancel.cancel();
            let drained = suite.await;
            signal?;
            drained.and(Err(eyre::eyre!("benchmark interrupted")))
        }
        result = &mut suite => result,
    };
    result
}
