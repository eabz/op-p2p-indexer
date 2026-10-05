//! The scan of `download --refetch-incomplete`: which downloaded chunks not sealed yet lack a
//! field their forks have, with what their fill holds, or cannot be read.
//!
//! By default it reads the head of each answer only ([`fill::lacking_at_heads`]): an answer
//! comes from one of the service's servers, and one that leaves a field out leaves it out of
//! every row, so a few rows of each answer tell. The text is still decompressed (the rows that
//! tell are at the end of an answer, after its logs) but not parsed, which is most of what a
//! full read costs. `--scan full` reads every row ([`fill::lacking`]). Either way a chunk is
//! only replaced by an answer that lacks fewer fields, every row counted, and `verify` checks
//! every block, so a chunk the head scan misses cannot reach the store.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::cli::ScanMode;
use crate::fill;
use crate::progress;
use crate::state::{Chunk, Lacking, Plan, State, covered};
use crate::verify::Forks;

/// The chunks on disk not sealed yet that lack a field or cannot be read, in block order;
/// read on `threads` threads, with a progress line now and then.
///
/// # Errors
///
/// Returns an error if the state directory cannot be listed, or `cancel` fires.
pub(crate) async fn incomplete(
    state: &State,
    plan: &Plan,
    mode: ScanMode,
    threads: usize,
    cancel: &CancellationToken,
) -> eyre::Result<Vec<Lacking>> {
    let chunks = {
        let (state, plan) = (state.clone(), *plan);
        tokio::task::spawn_blocking(move || -> std::io::Result<Vec<Chunk>> {
            let sealed = state.sealed_through()?;
            let mut chunks = Vec::new();
            for chunk in plan.chunks().filter(|chunk| !covered(sealed, *chunk)) {
                if state.raw_path(chunk).try_exists()? {
                    chunks.push(chunk);
                }
            }
            Ok(chunks)
        })
        .await??
    };
    let total = chunks.len();
    info!(
        chunks = total,
        ?mode,
        "reading the chunks on disk for fields their rows lack"
    );
    let started = std::time::Instant::now();
    let read = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let scan = {
        let (state, forks) = (state.clone(), Forks::new(plan.chain));
        let (read, stop) = (Arc::clone(&read), Arc::clone(&stop));
        tokio::task::spawn_blocking(move || {
            let next = AtomicUsize::new(0);
            let found = Mutex::new(Vec::new());
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    scope.spawn(|| {
                        while !stop.load(Ordering::Relaxed)
                            && let Some(&chunk) = chunks.get(next.fetch_add(1, Ordering::Relaxed))
                        {
                            if let Some(lacking) = check(&state, &forks, chunk, mode)
                                && let Ok(mut found) = found.lock()
                            {
                                found.push(lacking);
                            }
                            read.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            });
            let mut found = found.into_inner().unwrap_or_default();
            found.sort_unstable_by_key(|lacking| lacking.from);
            found
        })
    };
    tokio::pin!(scan);
    let mut tick = interval(progress::INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let found = loop {
        tokio::select! {
            biased;
            () = cancel.cancelled(), if !stop.load(Ordering::Relaxed) => {
                stop.store(true, Ordering::Relaxed);
            }
            found = &mut scan => break found?,
            _ = tick.tick() => {
                info!(chunks = read.load(Ordering::Relaxed), of = total, "reading the chunks on disk");
            }
        }
    };
    eyre::ensure!(
        !cancel.is_cancelled(),
        "stopped while reading the chunks on disk: run `download` again"
    );
    let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    info!(
        chunks = total,
        incomplete = found.len(),
        chunks_per_sec = u64::try_from(total)
            .unwrap_or(u64::MAX)
            .saturating_mul(1000)
            .checked_div(millis.max(1)),
        "chunks on disk read: those lacking a field are asked for again"
    );
    Ok(found)
}

/// What `chunk` lacks, with its fill, if anything; `u64::MAX` fields if it cannot be read.
fn check(state: &State, forks: &Forks, chunk: Chunk, mode: ScanMode) -> Option<Lacking> {
    let (raw, fill) = (state.raw_path(chunk), state.fill_path(chunk));
    let counted = match mode {
        ScanMode::Head => fill::lacking_at_heads(forks, &raw, &fill),
        ScanMode::Full => fill::lacking(forks, &raw, Some(&fill)).map(|(_, filled)| filled),
    };
    let fields = match counted {
        Ok(0) => return None,
        Ok(fields) => fields,
        Err(err) => {
            warn!(from = chunk.from, to = chunk.to, %err, "a chunk on disk cannot be read: downloading it again");
            u64::MAX
        }
    };
    Some(Lacking {
        from: chunk.from,
        to: chunk.to,
        fields,
    })
}
