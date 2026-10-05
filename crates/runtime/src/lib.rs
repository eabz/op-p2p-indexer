//! Shared process setup independent of node and storage services.

use std::path::Path;

use eyre::WrapErr;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

pub mod env_file;

/// Initializes logging and reports the environment file without its contents.
pub fn init_tracing(env_file: Option<&Path>) {
    tracing_subscriber::fmt()
        .with_timer(ChronoUtc::new("%H:%M:%S%.3f".to_owned()))
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    if let Some(path) = env_file {
        tracing::info!(path = %path.display(), "loaded env file");
    }
}

/// Resolves with the signal's name on Ctrl-C (SIGINT) or, on Unix, SIGTERM, which a service
/// manager (systemd, a container runtime) sends on stop.
///
/// # Errors
///
/// Returns an error if the signal handlers cannot be installed.
pub async fn shutdown_signal() -> eyre::Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).wrap_err("failed to listen for SIGTERM")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.wrap_err("failed to listen for Ctrl-C")?;
                Ok("SIGINT")
            }
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .wrap_err("failed to listen for Ctrl-C")?;
        Ok("SIGINT")
    }
}
