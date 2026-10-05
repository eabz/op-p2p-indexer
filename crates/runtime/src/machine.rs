//! What the machine has: its cores, its memory and its name, read once, for the settings
//! the binaries size from them (`docs/configuration.md`, "Advanced settings").
//!
//! Only reads; the formulas live with the settings they size.

use std::sync::OnceLock;

/// The machine the process runs on, as [`Machine::get`] read it.
#[derive(Debug)]
pub struct Machine {
    /// Cores the process may use (the CPU quota of a container included).
    pub cores: usize,
    /// Memory the process may use, in bytes: the smaller of the machine's and a container's
    /// limit. `None` when it cannot be read; the settings then keep fixed defaults.
    pub memory: Option<u64>,
    /// The machine's host name; `None` when it cannot be read.
    pub hostname: Option<String>,
}

impl Machine {
    /// The machine, read on the first call.
    pub fn get() -> &'static Self {
        static MACHINE: OnceLock<Machine> = OnceLock::new();
        MACHINE.get_or_init(|| Self {
            cores: std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
            memory: memory(),
            hostname: hostname(),
        })
    }
}

/// The machine's memory (`MemTotal` in `/proc/meminfo`), lowered to a cgroup's limit.
#[cfg(target_os = "linux")]
fn memory() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let total = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)?;
    // cgroup v2, then v1; "max", or v1's huge "no limit" value, leaves the machine's.
    let limit = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| {
        std::fs::read_to_string(path)
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
    });
    Some(limit.map_or(total, |limit| limit.min(total)))
}

/// The machine's memory, from `sysctl hw.memsize`.
#[cfg(target_os = "macos")]
fn memory() -> Option<u64> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn memory() -> Option<u64> {
    None
}

/// The node name `uname` reports.
#[cfg(unix)]
fn hostname() -> Option<String> {
    let name = rustix::system::uname();
    let name = name.nodename().to_str().ok()?;
    (!name.is_empty()).then(|| name.to_owned())
}

#[cfg(not(unix))]
fn hostname() -> Option<String> {
    None
}
