//! The settings sized from the machine ([`Machine`]) when the environment leaves them unset:
//! their formulas, in one place (`docs/configuration.md`, "Advanced settings"). An explicit
//! variable always wins. The node logs what they chose at startup.
//!
//! Memory is shared out in eighths so the defaults leave most of it to the page cache, fjall
//! and everything else: the unsafe chain, the Flight builds and the server's read budget take
//! at most three eighths together.

use op_indexer_runtime::machine::Machine;
use op_indexer_stream::{BUILD_BYTES, PARALLEL_BUILDS};

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// A build is CPU work: more than a couple per core only holds memory.
const BUILDS_PER_CORE: usize = 2;

/// Flight streams at once when the machine is small: what the default was before sizing.
const MIN_FLIGHTS: usize = 8;

/// `OP_INDEXER_UNSAFE_MAX_BYTES`: an eighth of the memory, from 256 MiB to 2 GiB (about a day
/// of a busy chain's blocks, docs/storage.md section 3; more is never read). 2 GiB when the
/// memory is unknown.
pub(crate) fn unsafe_max_bytes(machine: &Machine) -> u64 {
    eighth(machine, 256 * MIB, 2 * GIB, 2 * GIB)
}

/// The Flight builds the stream runs at once, server-wide: two per core, but no more than an
/// eighth of the memory holds at [`BUILD_BYTES`] each, and at least the [`PARALLEL_BUILDS`] of
/// one stream.
pub(crate) fn max_builds(machine: &Machine) -> usize {
    let by_cores = machine.cores.saturating_mul(BUILDS_PER_CORE);
    let by_memory = machine.memory.map_or(usize::MAX, |memory| {
        usize::try_from(memory / 8 / BUILD_BYTES).unwrap_or(usize::MAX)
    });
    by_cores.min(by_memory).max(PARALLEL_BUILDS)
}

/// `OP_INDEXER_STREAM_MAX_FLIGHTS`: as many streams as builds, at least [`MIN_FLIGHTS`]. A
/// stream runs up to [`PARALLEL_BUILDS`] builds at once, so fewer streams keep every build
/// busy; the others wait their turn while their consumers read, most of a stream's time.
pub(crate) fn max_flights(builds: usize) -> usize {
    builds.max(MIN_FLIGHTS)
}

/// The server's `OP_INDEXER_EL_MAX_SESSIONS`: two sessions per core in each direction, from 8
/// to 64. A server exists to serve peers that sync from it; an `indexer` keeps the execution
/// network's default whatever the machine, because full nodes ration their slots.
pub fn server_el_sessions(machine: &Machine) -> usize {
    machine.cores.saturating_mul(2).clamp(8, 64)
}

/// The server's `OP_INDEXER_SERVER_READ_BUDGET_MB`, in bytes: an eighth of the memory, from
/// 256 MiB to 16 GiB. 1 GiB when the memory is unknown.
pub fn server_read_budget(machine: &Machine) -> u64 {
    eighth(machine, 256 * MIB, 16 * GIB, GIB)
}

/// An eighth of the memory, clamped to `min..=max`; `fallback` when the memory is unknown.
fn eighth(machine: &Machine, min: u64, max: u64, fallback: u64) -> u64 {
    machine
        .memory
        .map_or(fallback, |memory| (memory / 8).clamp(min, max))
}

/// Bytes in MiB, for a log line.
pub fn mib(bytes: u64) -> u64 {
    bytes / MIB
}
