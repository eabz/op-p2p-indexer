//! The limits the unsafe chain runs with (docs/storage.md section 8).

/// Most parent links fork choice follows to connect a block to the canonical chain.
pub(super) const MAX_REORG_DEPTH: u32 = 256;
/// How far below the newest block (by timestamp) heights are kept when nothing prunes them, as
/// before L1 is known: a backstop, a day.
pub(super) const RETENTION_SECS: u64 = 24 * 60 * 60;
/// Expired heights one insert removes. More than one, so the chain catches up after falling
/// behind.
pub(super) const RETENTION_HEIGHTS_PER_INSERT: usize = 16;
/// Most blocks one [`ancestry`](crate::UnsafeStore::ancestry) call returns, bounding its
/// memory.
pub(super) const MAX_ANCESTRY_BLOCKS: u64 = 1024;
/// Events kept for readers that fell behind; one further behind reads the state again.
pub(super) const EVENTS_KEPT: usize = 10_000;
