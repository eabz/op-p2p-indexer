//! The balancer: the directory of a deployment's servers for one chain (`docs/serving.md`
//! section 6). Servers register with it; clients ask it where to read (Flight
//! `GetFlightInfo` and `ListFlights`, gRPC `Locate`) and then read from the servers.
//!
//! Loads the `.env` file ([`op_indexer_runtime::env_file`]), sets up tracing, reads the
//! configuration, reads the chain's manifest from R2 (read-only) and runs the
//! [`Balancer`] until Ctrl-C or SIGTERM.

mod config;

use std::path::PathBuf;

use eyre::WrapErr;
use op_indexer_balancer::Balancer;
use op_indexer_chunks::{ChunkSigner, ChunkStore, Manifest};
use op_indexer_runtime::Startup;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::BalancerSettings;

fn main() -> eyre::Result<()> {
    let Startup::Run { env_file } =
        op_indexer_runtime::startup(env!("CARGO_BIN_NAME"), env!("CARGO_PKG_VERSION"))?
    else {
        return Ok(());
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("failed to start the tokio runtime")?
        .block_on(run(env_file))
}

async fn run(env_file: Option<PathBuf>) -> eyre::Result<()> {
    op_indexer_runtime::init_tracing(env_file.as_deref());

    let settings = BalancerSettings::from_env_and_args()?;
    let store = ChunkStore::r2(&settings.r2, settings.chain, settings.read)
        .wrap_err("failed to set up the R2 chunk store")?;
    let manifest = Manifest::load(&store)
        .await
        .wrap_err("failed to read the R2 manifest")?;
    info!(
        chunks = manifest.entries().len(),
        last_block = manifest.last().map(|entry| entry.last),
        "read the manifest"
    );

    let cancel = CancellationToken::new();
    let mut balancer = Balancer::new(settings.balancer, settings.chain, store, manifest);
    if let Some(presign) = &settings.presign {
        let signer = ChunkSigner::r2(presign).wrap_err("failed to set up the chunk URL signer")?;
        balancer = balancer.with_raw_chunks(signer);
        info!("raw chunk plans on: chunk URLs are signed with the presign key");
    }
    let run = balancer.run(cancel.clone());
    tokio::pin!(run);
    // Until a signal arrives, or the balancer stops on its own (it could not listen).
    tokio::select! {
        result = &mut run => return result.wrap_err("the balancer failed"),
        signal = op_indexer_runtime::shutdown_signal() => info!(signal = signal?, "shutting down"),
    }
    cancel.cancel();
    run.await.wrap_err("the balancer failed")
}
