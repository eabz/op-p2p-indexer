//! Shared process setup independent of node and storage services.

use std::fmt;
use std::io::{self, IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};

use eyre::WrapErr;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoUtc;

pub mod config;
pub mod env_file;
pub mod machine;
pub mod service;

/// What a node binary does once [`startup`] returns.
#[derive(Debug)]
#[must_use]
pub enum Startup {
    /// Run in the foreground, with the configuration file loaded from here, if one was.
    Run {
        /// Where the TOML or legacy environment file was.
        env_file: Option<PathBuf>,
    },
    /// The command line asked for something done already (`--version`, a service command):
    /// exit.
    Exit,
}

/// The start of `indexer`, `server` and `balancer`, before any thread or runtime: answers
/// `--version` before loading configuration, then resolves TOML or legacy environment
/// settings ([`config::initialize`]) and runs a service command if the command line names one
/// ([`service`]).
///
/// # Errors
///
/// Returns an error if configuration cannot be loaded or the service command fails.
pub fn startup(binary: &str, version: &str) -> eyre::Result<Startup> {
    if version_requested(binary, version) {
        return Ok(Startup::Exit);
    }
    if config::command(binary)? {
        return Ok(Startup::Exit);
    }
    let env_file = config::initialize(binary)?;
    if !config::check_requested() && service::command(binary, env_file.as_deref())? {
        return Ok(Startup::Exit);
    }
    Ok(Startup::Run { env_file })
}

/// Prints `<binary> <version>` and returns `true` if the command line asks for the version
/// (`--version` or `-V`, among [`env_file::other_args`]): the binary then exits at once, before
/// it loads the `.env` file or starts anything.
pub fn version_requested(binary: &str, version: &str) -> bool {
    let asked = env_file::other_args(std::env::args_os().skip(1))
        .iter()
        .any(|arg| arg == "--version" || arg == "-V");
    if asked {
        say(format_args!("{binary} {version}"));
    }
    asked
}

/// A line for the operator on stdout: a command's answer. A closed stdout has nowhere to show
/// it.
pub(crate) fn say(line: fmt::Arguments<'_>) {
    drop(writeln!(io::stdout(), "{line}"));
}

/// Initializes logging and reports the configuration file without its contents.
pub fn init_tracing(env_file: Option<&Path>) {
    tracing_subscriber::fmt()
        .with_timer(ChronoUtc::new("%H:%M:%S%.3f".to_owned()))
        // Colours for a terminal only: a log file (`start`, systemd) gets plain text.
        .with_ansi(io::stdout().is_terminal())
        .with_env_filter(
            EnvFilter::try_new(env_var("RUST_LOG").unwrap_or_else(|| "info".to_owned()))
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    if let Some(path) = env_file {
        tracing::info!(path = %path.display(), "loaded configuration");
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

/// Reads the effective value: CLI chain override, nonempty Unicode process environment,
/// then the selected TOML role. Empty process variables do not hide TOML values.
pub fn env_var(name: &str) -> Option<String> {
    config::value(name)
}

/// Reads `name`, which is read for this release only, warning if the environment sets it:
/// `instead` says what replaces it (another variable, a flag, or nothing because it is now
/// automatic). Call it once at startup, after [`init_tracing`]. Never logs the value.
pub fn deprecated(name: &str, instead: impl fmt::Display) -> Option<String> {
    let value = env_var(name);
    if std::env::var_os(name).is_some() {
        tracing::warn!("{name} is deprecated and read for this release only: {instead}");
    }
    value
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
