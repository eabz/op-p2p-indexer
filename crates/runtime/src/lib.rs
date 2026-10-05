//! Shared process setup independent of node and storage services.

use std::io::{self, Write as _};
use std::path::Path;

use eyre::WrapErr;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

pub mod env_file;

/// Prints `<binary> <version>` and returns `true` if the command line asks for the version
/// (`--version` or `-V`, among [`env_file::other_args`]): the binary then exits at once, before
/// it loads the `.env` file or starts anything.
pub fn version_requested(binary: &str, version: &str) -> bool {
    let asked = env_file::other_args(std::env::args_os().skip(1))
        .iter()
        .any(|arg| arg == "--version" || arg == "-V");
    if asked {
        // The answer is the only output; a closed stdout has nothing to show it on.
        drop(writeln!(io::stdout(), "{binary} {version}"));
    }
    asked
}

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

/// How shutdown handling responds when the Unix SIGTERM handler cannot be installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignalPolicy {
    /// Refuses to run without both service shutdown signals.
    RequireTerminate,
    /// Continues waiting for Ctrl-C when SIGTERM registration fails.
    CtrlCFallback,
}

/// Reads a nonempty Unicode environment value; absent, empty and non-Unicode values are absent.
pub fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Resolves with the signal's name on Ctrl-C (SIGINT) or, on Unix, SIGTERM.
///
/// # Errors
///
/// Returns an error if either required signal handler cannot be installed.
pub async fn shutdown_signal() -> eyre::Result<&'static str> {
    shutdown_signal_with_policy(SignalPolicy::RequireTerminate).await
}

/// Waits for a shutdown signal using the caller's SIGTERM registration policy.
/// On non-Unix systems only Ctrl-C is available and the policy has no effect.
///
/// # Errors
///
/// Returns an error if Ctrl-C fails, or if SIGTERM registration fails under
/// [`SignalPolicy::RequireTerminate`].
///
/// # Cancel safety
///
/// Safe to cancel while waiting; no application state is consumed.
pub async fn shutdown_signal_with_policy(policy: SignalPolicy) -> eyre::Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                return tokio::select! {
                    result = ctrl_c() => result,
                    _ = terminate.recv() => Ok("SIGTERM"),
                };
            }
            Err(err) if policy == SignalPolicy::RequireTerminate => {
                return Err(err).wrap_err("failed to listen for SIGTERM");
            }
            Err(_) => {}
        }
    }
    #[cfg(not(unix))]
    let _ = policy;
    ctrl_c().await
}

async fn ctrl_c() -> eyre::Result<&'static str> {
    tokio::signal::ctrl_c()
        .await
        .wrap_err("failed to listen for Ctrl-C")?;
    Ok("SIGINT")
}
