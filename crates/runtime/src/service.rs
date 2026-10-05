//! Service commands, so a node runs in the background without tmux: `start`, `stop`, `restart`,
//! `status`, `logs` and `install-service`, shared by `indexer`, `server` and `balancer`.
//!
//! With no command a binary runs in the foreground, as it always has. `start` runs the same
//! binary again, with the same arguments but the command, in a process group of its own (so a
//! closed terminal does not stop it), stdin from `/dev/null`, its output appended to the log
//! file, and writes its pid next to it. Everything else reads that pid file. Signals, process
//! details and the log's tail go through `kill`, `ps` and `tail`, as on any Unix: no unsafe code.
//!
//! Where: the directory of the TOML file loaded, else the working directory. The log is
//! `<binary>.log` there, or the path in `--log-file` / the role's `log_file` setting (relative to that
//! directory); the pid file is always `<binary>.pid` there. The log is never rotated by the
//! binary: logrotate with `copytruncate` does it (README, "Running in the background").

use std::ffi::OsString;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use eyre::{WrapErr, bail, eyre};

use crate::{args, config, say, setting};

/// The flag naming the log file.
const LOG_FLAG: &str = "--log-file";
/// The variable naming the log file when the flag is absent.
const LOG_VAR: &str = "OP_INDEXER_LOG_FILE";
/// The commands.
const COMMANDS: [&str; 6] = [
    "start",
    "stop",
    "restart",
    "status",
    "logs",
    "install-service",
];
/// The flag `stop` and `restart` take to kill a node that does not stop in time.
const FORCE_FLAG: &str = "--force";
/// How long `start` watches the new process before it reports it started.
const STARTUP_WAIT: Duration = Duration::from_secs(2);
/// How long `stop` waits for a graceful shutdown after SIGTERM.
const STOP_WAIT: Duration = Duration::from_secs(30);
/// How often a wait looks again.
const POLL: Duration = Duration::from_millis(200);
/// Log lines shown by `status`, and by `start` when the process exits at once.
const TAIL_LINES: &str = "5";
/// Log lines `logs` shows before it follows or ends.
const LOGS_LINES: &str = "100";

/// Runs the service command the command line names (`start`, `stop [--force]`,
/// `restart [--force]`, `status`, `logs [-f]`, `install-service`), if any, for `binary`;
/// `false` if it names none. Called once the TOML file is loaded (`config_file` is where it
/// was) and before anything starts.
///
/// # Errors
///
/// Returns an error if the command cannot do what it was asked: the node is already running
/// (`start`), it exited at once, it did not stop in time without `--force`, a file cannot be
/// read or written, or `kill`/`ps`/`tail` cannot be run.
pub(crate) fn command(binary: &str, config_file: Option<&Path>) -> eyre::Result<bool> {
    let all: Vec<OsString> = std::env::args_os().skip(1).collect();
    let others = config::other_args(all.iter().cloned());
    let Some(name) = others
        .first()
        .and_then(|arg| arg.to_str())
        .filter(|name| COMMANDS.contains(name))
    else {
        return Ok(false);
    };
    let has = |flag: &str| others.iter().any(|arg| arg == flag);
    let force = has(FORCE_FLAG);
    let node = Node::new(binary, config_file, &others)?;
    // What the node itself runs with: everything but the command and the service flags.
    let mut child = args::without_flag(LOG_FLAG, all.iter().cloned());
    if let Some(at) = child.iter().position(|arg| arg == name) {
        child.remove(at);
    }
    child.retain(|arg| arg != FORCE_FLAG);
    if let Some(path) = config::path() {
        child = args::without_flag("--config", child);
        child.push("--config".into());
        child.push(path.as_os_str().to_owned());
    }
    match name {
        "start" => node.start(&child)?,
        "stop" => node.stop(force)?,
        "restart" => {
            node.stop(force)?;
            node.start(&child)?;
        }
        "status" => node.status(),
        "logs" => node.logs(has("-f") || has("--follow"))?,
        _ => node.install_service(&child)?,
    }
    Ok(true)
}

/// A binary's service files.
#[derive(Debug)]
struct Node<'a> {
    binary: &'a str,
    dir: PathBuf,
    log: PathBuf,
    pid: PathBuf,
}

impl<'a> Node<'a> {
    fn new(binary: &'a str, config_file: Option<&Path>, args: &[OsString]) -> eyre::Result<Self> {
        let cwd = std::env::current_dir().wrap_err("failed to read the working directory")?;
        let dir = config_file
            .and_then(|file| std::path::absolute(file).ok())
            .and_then(|file| file.parent().map(Path::to_path_buf))
            .unwrap_or(cwd);
        let log = args::flag_value(LOG_FLAG, args.iter().cloned())
            .or_else(|| setting(LOG_VAR).map(OsString::from))
            .map_or_else(|| dir.join(format!("{binary}.log")), |log| dir.join(log));
        let pid = dir.join(format!("{binary}.pid"));
        Ok(Self {
            binary,
            dir,
            log,
            pid,
        })
    }

    /// The pid of the running node, if the pid file names a live process of this binary.
    fn running(&self) -> Option<u32> {
        let pid = fs::read_to_string(&self.pid).ok()?.trim().parse().ok()?;
        self.is_alive(pid).then_some(pid)
    }

    /// Whether `pid` is a live process of this binary (its command name ends with it), so a
    /// pid file left behind never makes another process the target.
    fn is_alive(&self, pid: u32) -> bool {
        ps(pid, "comm=").is_some_and(|name| name.trim_end().ends_with(self.binary))
    }

    fn start(&self, args: &[OsString]) -> eyre::Result<()> {
        let binary = self.binary;
        if let Some(pid) = self.running() {
            bail!(
                "{binary} is already running, pid {pid} ({})",
                self.pid.display()
            );
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .wrap_err_with(|| format!("failed to open the log {}", self.log.display()))?;
        let exe = std::env::current_exe().wrap_err("failed to find this binary")?;
        let mut command = Command::new(exe);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(log.try_clone().wrap_err("failed to open the log")?)
            .stderr(log);
        // Its own process group: a terminal closing, or Ctrl-C in it, does not reach it.
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .wrap_err_with(|| format!("failed to start {binary}"))?;
        let pid = child.id();
        fs::write(&self.pid, format!("{pid}\n"))
            .wrap_err_with(|| format!("failed to write {}", self.pid.display()))?;
        let deadline = Instant::now() + STARTUP_WAIT;
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait().wrap_err("failed to watch the process")? {
                drop(fs::remove_file(&self.pid));
                return Err(eyre!(
                    "{binary} exited at once ({status}); the end of {}:\n{}",
                    self.log.display(),
                    self.last_lines()
                ));
            }
            thread::park_timeout(POLL);
        }
        say(format_args!(
            "{binary} started, pid {pid}, log {}",
            self.log.display()
        ));
        Ok(())
    }

    fn stop(&self, force: bool) -> eyre::Result<()> {
        let binary = self.binary;
        let Some(pid) = self.running() else {
            drop(fs::remove_file(&self.pid));
            say(format_args!("{binary} is not running"));
            return Ok(());
        };
        signal(pid, "TERM")?;
        say(format_args!("{binary} (pid {pid}): stopping..."));
        let deadline = Instant::now() + STOP_WAIT;
        let mut alive = true;
        while alive && Instant::now() < deadline {
            thread::park_timeout(POLL);
            alive = self.is_alive(pid);
        }
        if alive {
            if !force {
                bail!(
                    "{binary} (pid {pid}) is still running after {} s; `stop {FORCE_FLAG}` \
                     kills it",
                    STOP_WAIT.as_secs()
                );
            }
            signal(pid, "KILL")?;
            while self.is_alive(pid) {
                thread::park_timeout(POLL);
            }
            say(format_args!("{binary} (pid {pid}) killed"));
        } else {
            say(format_args!("{binary} (pid {pid}) stopped"));
        }
        drop(fs::remove_file(&self.pid));
        Ok(())
    }

    fn status(&self) {
        let binary = self.binary;
        match self.running() {
            Some(pid) => {
                let (uptime, rss_mib) = ps(pid, "etime=,rss=")
                    .and_then(|fields| {
                        let mut fields = fields.split_whitespace();
                        let uptime = fields.next()?.to_owned();
                        let rss_kib: u64 = fields.next()?.parse().ok()?;
                        Some((uptime, rss_kib / 1024))
                    })
                    .unwrap_or_default();
                say(format_args!(
                    "{binary} is running, pid {pid}, up {uptime}, {rss_mib} MiB resident"
                ));
            }
            None => say(format_args!("{binary} is not running")),
        }
        say(format_args!("log {}", self.log.display()));
        for line in self.last_lines().lines() {
            say(format_args!("  {line}"));
        }
    }

    /// Shows the end of the log, following it with `follow`: this process becomes `tail`, so
    /// it ends as `tail` does (Ctrl-C, or a kill) and leaves nothing behind.
    fn logs(&self, follow: bool) -> eyre::Result<()> {
        let mut tail = Command::new("tail");
        tail.args(["-n", LOGS_LINES]);
        if follow {
            tail.arg("-F");
        }
        tail.arg(&self.log);
        #[cfg(unix)]
        let failed = tail.exec();
        #[cfg(not(unix))]
        let failed = match tail.status() {
            Ok(_) => return Ok(()),
            Err(err) => err,
        };
        Err(failed).wrap_err("failed to run tail")
    }

    /// Writes a systemd unit next to the log and prints how to install it. Runs no `systemctl`.
    fn install_service(&self, args: &[OsString]) -> eyre::Result<()> {
        let binary = self.binary;
        let exe = std::env::current_exe().wrap_err("failed to find this binary")?;
        let exec = std::iter::once(exe.into_os_string())
            .chain(args.iter().cloned())
            .map(|arg| unit_quote(&arg.to_string_lossy()))
            .collect::<Vec<_>>()
            .join(" ");
        let user = std::env::var("SUDO_USER")
            .or_else(|_| std::env::var("USER"))
            .map(|user| format!("User={user}\n"))
            .unwrap_or_default();
        let name = format!("op-indexer-{binary}.service");
        let unit = format!(
            "[Unit]\n\
             Description=op-p2p-indexer {binary}\n\
             After=network-online.target\n\
             Wants=network-online.target\n\n\
             [Service]\n\
             Type=simple\n\
             {user}\
             WorkingDirectory={dir}\n\
             ExecStart={exec}\n\
             Restart=on-failure\n\
             RestartSec=5\n\
             KillSignal=SIGTERM\n\
             TimeoutStopSec={stop}\n\
             StandardOutput=append:{log}\n\
             StandardError=append:{log}\n\
             LimitNOFILE=65536\n\n\
             [Install]\n\
             WantedBy=multi-user.target\n",
            dir = self.dir.display(),
            stop = STOP_WAIT.as_secs().saturating_add(30),
            log = self.log.display(),
        );
        let path = self.dir.join(&name);
        fs::write(&path, unit).wrap_err_with(|| format!("failed to write {}", path.display()))?;
        let path = path.display();
        say(format_args!(
            "wrote {path}; to install it (Linux, systemd):\n  \
             sudo cp {path} /etc/systemd/system/\n  \
             sudo systemctl daemon-reload\n  \
             sudo systemctl enable --now {name}\n\
             then `sudo systemctl status {name}`; use systemctl, not `{binary} start`/`stop`, \
             for a node it runs"
        ));
        Ok(())
    }

    /// The last lines of the log, by `tail`; empty if it cannot be read.
    fn last_lines(&self) -> String {
        Command::new("tail")
            .args(["-n", TAIL_LINES])
            .arg(&self.log)
            .stderr(Stdio::null())
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
            .unwrap_or_default()
    }
}

/// `ps -p <pid> -o <fields>`, its output; `None` if there is no such process.
fn ps(pid: u32, fields: &str) -> Option<String> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", fields])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    (output.status.success() && !text.trim().is_empty()).then_some(text)
}

/// Sends `pid` the signal named `name` (`TERM`, `KILL`).
fn signal(pid: u32, name: &str) -> eyre::Result<()> {
    let status = Command::new("kill")
        .args(["-s", name, &pid.to_string()])
        .status()
        .wrap_err("failed to run kill")?;
    if !status.success() {
        bail!("kill -s {name} {pid} failed ({status})");
    }
    Ok(())
}

/// A systemd `ExecStart` word: quoted when it holds a space, a quote or a backslash.
fn unit_quote(word: &str) -> String {
    if word.contains([' ', '"', '\'', '\\']) {
        format!("\"{}\"", word.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        word.to_owned()
    }
}
