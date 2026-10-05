//! Reads of the chain the stream serves: the archive (committed blocks) and, above its tip,
//! the unsafe store's canonical chain. Shared by the follower and every subscription.
//!
//! Every store call is retried while it fails with a transient error (storage's
//! [`retry`]), until the node shuts down.

use std::collections::VecDeque;
use std::future::Future;

use alloy_primitives::{B256, BlockNumber};
use op_indexer_primitives::{BlockRef, L1Heads, ReadLimits, ReadParts};
use op_indexer_storage::{
    ArchiveRange, ArchiveStore, RetryError, StorageError, Store, UnsafeStore, retry,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::convert::{ConvertError, Prepared, StoredBlock};

/// One history read: at most 64 blocks or 16 MiB (headers, bodies and receipts), which bounds
/// the memory a subscription holds while it catches up. Also the most unsafe blocks read in
/// one call.
const HISTORY_BATCH: ReadLimits = ReadLimits {
    items: 64,
    bytes: 16 * 1024 * 1024,
    lowest: 0,
};
/// A read of one block.
const ONE_BLOCK: ReadLimits = ReadLimits {
    items: 1,
    bytes: usize::MAX,
    lowest: 0,
};

/// Why a read of the chain failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadError {
    /// A store failed in a way retrying cannot fix.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The node is shutting down.
    #[error("the node is shutting down")]
    Cancelled,
    /// A stored block does not decode.
    #[error(transparent)]
    Convert(#[from] ConvertError),
}

/// The status a request ends with when a read fails; logged, except at shutdown. The
/// consumer is told only that the stores could not be read.
pub(crate) fn read_status(err: &ReadError) -> tonic::Status {
    if let ReadError::Cancelled = err {
        return tonic::Status::unavailable("the node is shutting down");
    }
    tracing::warn!(%err, "a stream request cannot read the stores");
    tonic::Status::internal("the node cannot read its stores")
}

impl From<RetryError> for ReadError {
    fn from(err: RetryError) -> Self {
        match err {
            RetryError::Cancelled => Self::Cancelled,
            RetryError::Storage(err) => Self::Storage(err),
        }
    }
}

/// The two stores, read only.
#[derive(Debug, Clone)]
pub(crate) struct Source<U, A> {
    pub(crate) unsafe_store: U,
    pub(crate) archive: A,
    /// The node's shutdown: ends a call waiting to be retried.
    pub(crate) cancel: CancellationToken,
    /// Whether range sync fills the archive from its tip up, so the heights between it and the
    /// unsafe store will be held.
    pub(crate) fills_gaps: bool,
}

/// A request's archive position and owned read-ahead. Rewinds replace the stream.
#[derive(Default)]
pub(crate) struct History {
    tip: Option<BlockNumber>,
    next: Option<BlockNumber>,
    stream: Option<ArchiveRange>,
    /// What of each block the request reads.
    parts: ReadParts,
}

impl History {
    /// A history read of the `parts` of each block.
    pub(crate) fn of(parts: ReadParts) -> Self {
        Self {
            parts,
            ..Self::default()
        }
    }
}

impl std::fmt::Debug for History {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("History")
            .field("tip", &self.tip)
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

/// What the stores hold, as [`Source::holdings`] read it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Holdings {
    /// The archive's first and last block. Its first block does not rise: it keeps all it has.
    pub(crate) archive: Option<(BlockRef, BlockRef)>,
    lowest_unsafe: Option<BlockNumber>,
    fills_gaps: bool,
}

impl Holdings {
    /// The heights between the archive's tip (block 0 when it is empty) and the unsafe
    /// store's lowest block, none of them held, as `start..end`. The unsafe store expires
    /// blocks nothing promoted; range sync, when on, fills them into the archive.
    pub(crate) fn gap(&self) -> Option<(BlockNumber, BlockNumber)> {
        let start = self
            .archive
            .map_or(0, |(_, tip)| tip.number.saturating_add(1));
        let end = self.lowest_unsafe?;
        (start < end).then_some((start, end))
    }

    /// The [gap](Self::gap), if `number` is in it.
    pub(crate) fn gap_at(&self, number: BlockNumber) -> Option<(BlockNumber, BlockNumber)> {
        self.gap()
            .filter(|(start, end)| (*start..*end).contains(&number))
    }

    /// `OUT_OF_RANGE` if the stores do not hold `number` and never will (see
    /// [`Self::held_from`]).
    pub(crate) fn ensure_held(&self, number: BlockNumber) -> Result<(), tonic::Status> {
        let held = self.held_from(number);
        if held > number {
            return Err(tonic::Status::out_of_range(format!(
                "block {number} is not held; the node holds blocks from {held} on"
            )));
        }
        Ok(())
    }

    /// Where blocks from `number` up are held, or will be: `number` itself, unless it is below
    /// the archive's first block, or in the [gap](Self::gap) with range sync off. Neither store
    /// ever gets those heights back, so a read from `number` would wait forever; this is the
    /// height held above them.
    pub(crate) fn held_from(&self, number: BlockNumber) -> BlockNumber {
        if let Some((first, _)) = self.archive
            && number < first.number
        {
            return first.number;
        }
        match self.gap_at(number) {
            Some((_, end)) if !self.fills_gaps => end,
            _ => number,
        }
    }
}

impl<U: UnsafeStore, A: ArchiveStore> Source<U, A> {
    /// Runs `call` with retries.
    pub(crate) async fn call<T, F, Fut>(
        &self,
        store: Store,
        operation: &'static str,
        call: F,
    ) -> Result<T, RetryError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, StorageError>>,
    {
        retry(&self.cancel, store, operation, call).await
    }

    /// The canonical blocks from `from` upwards, oldest first: from the archive while `from`
    /// is in it, else from the unsafe store, up to its head. Empty when no canonical block is
    /// held at `from` (above the head, or in a gap). `history` remembers where the archive was
    /// found to end between calls, so a read above it does not ask the archive first. A batch
    /// the archive returns ends at its limits, not at the archive's end, so its tip is set only
    /// when the archive holds nothing at `from`.
    pub(crate) async fn blocks_from(
        &self,
        from: BlockNumber,
        history: &mut History,
    ) -> Result<Vec<Prepared>, ReadError> {
        if history.tip.is_none_or(|tip| from <= tip) {
            let archived = self.history_batch(from, history).await?;
            if !archived.is_empty() {
                return Ok(archived);
            }
            history.tip = Some(from.saturating_sub(1));
        }
        match self.unsafe_from(from).await? {
            // Above the head: nothing is there yet.
            None => return Ok(Vec::new()),
            Some(unsafe_blocks) if !unsafe_blocks.is_empty() => return Ok(unsafe_blocks),
            Some(_) => {}
        }
        // Promoted meanwhile: archived, then pruned from the unsafe store.
        let archived = self.history_batch(from, history).await?;
        if !archived.is_empty() {
            history.tip = None;
        }
        Ok(archived)
    }

    /// Reads the next owned batch; transient failures reopen at the undelivered height.
    async fn history_batch(
        &self,
        from: BlockNumber,
        history: &mut History,
    ) -> Result<Vec<Prepared>, ReadError> {
        let mut current = history.stream.take().filter(|_| history.next == Some(from));
        let (blocks, stream) = self
            .call(Store::Archive, "archive range", || {
                let mut stream = current
                    .take()
                    .unwrap_or_else(|| self.archive.read_range(from, HISTORY_BATCH, history.parts));
                async move {
                    let blocks = stream.next().await.transpose()?.unwrap_or_default();
                    Ok((blocks, stream))
                }
            })
            .await?;
        if blocks.is_empty() {
            history.next = None;
        } else {
            history.next = u64::try_from(blocks.len())
                .ok()
                .and_then(|len| from.checked_add(len));
            history.stream = Some(stream);
        }
        blocks
            .into_iter()
            .map(|block| Ok(Prepared::new(StoredBlock::Archived(block))?))
            .collect()
    }

    /// The unsafe store's canonical blocks from `from`, at most a batch: the block a batch
    /// above it and its ancestry, in one call each. Only the block at `from` alone when a
    /// block in between is missing. `None` when `from` is above the head.
    async fn unsafe_from(&self, from: BlockNumber) -> Result<Option<Vec<Prepared>>, ReadError> {
        let head = self
            .call(Store::Unsafe, "unsafe head", || self.unsafe_store.head())
            .await?;
        let Some(head) = head.filter(|head| head.number >= from) else {
            return Ok(None);
        };
        let span = u64::try_from(HISTORY_BATCH.items).unwrap_or(u64::MAX);
        let top_number = head.number.min(from.saturating_add(span).saturating_sub(1));
        let top = if top_number == head.number {
            Some(head)
        } else {
            self.canonical(top_number).await?.map(|block| block.at)
        };
        if let Some(top) = top {
            let ancestry = self
                .call(Store::Unsafe, "unsafe ancestry", || {
                    self.unsafe_store.ancestry(top, from.saturating_sub(1))
                })
                .await;
            match ancestry {
                Ok(blocks)
                    if blocks
                        .first()
                        .is_some_and(|block| block.block.header.number == from) =>
                {
                    return blocks
                        .into_iter()
                        .map(|block| Ok(Prepared::new(StoredBlock::Decoded(Box::new(block)))?))
                        .collect::<Result<_, _>>()
                        .map(Some);
                }
                // A gap, or a reorg between the reads: one block at a time.
                Ok(_) | Err(RetryError::Storage(_)) => {}
                Err(RetryError::Cancelled) => return Err(ReadError::Cancelled),
            }
        }
        Ok(Some(self.canonical(from).await?.into_iter().collect()))
    }

    /// The archive's blocks from `number`, within `limits`.
    async fn archived(
        &self,
        number: BlockNumber,
        limits: ReadLimits,
    ) -> Result<Vec<Prepared>, ReadError> {
        let archived = self
            .call(Store::Archive, "archive blocks", || {
                self.archive.blocks(number, limits)
            })
            .await?;
        archived
            .into_iter()
            .map(|block| Ok(Prepared::new(StoredBlock::Archived(block))?))
            .collect()
    }

    /// The unsafe store's canonical block at `number`.
    async fn canonical(&self, number: BlockNumber) -> Result<Option<Prepared>, ReadError> {
        let block = self
            .call(Store::Unsafe, "unsafe canonical", || {
                self.unsafe_store.canonical(number)
            })
            .await?;
        Ok(block
            .map(|block| Prepared::new(StoredBlock::Decoded(Box::new(block))))
            .transpose()?)
    }

    /// The canonical block at `number`: the unsafe store's, else the archive's.
    pub(crate) async fn block_at(
        &self,
        number: BlockNumber,
    ) -> Result<Option<Prepared>, ReadError> {
        if let Some(block) = self.canonical(number).await? {
            return Ok(Some(block));
        }
        Ok(self.archived(number, ONE_BLOCK).await?.into_iter().next())
    }

    /// The canonical block with this hash: the unsafe store's or the archive's. A side block
    /// is `None`.
    pub(crate) async fn block_by_hash(&self, hash: B256) -> Result<Option<Prepared>, ReadError> {
        let block = self
            .call(Store::Unsafe, "unsafe block", || {
                self.unsafe_store.block(hash)
            })
            .await?;
        if let Some(block) = block {
            let block = Prepared::new(StoredBlock::Decoded(Box::new(block)))?;
            if self.is_canonical(block.at).await? {
                return Ok(Some(block));
            }
            return Ok(None);
        }
        let number = self
            .call(Store::Archive, "archive number_of", || {
                self.archive.number_of(hash)
            })
            .await?;
        let Some(number) = number else {
            return Ok(None);
        };
        Ok(self.archived(number, ONE_BLOCK).await?.into_iter().next())
    }

    /// `at` (a block sent as canonical) with its receipts, if a store holds them now: the
    /// unsafe store's block, else the archive's (the pipeline attaches late receipts there).
    pub(crate) async fn with_receipts(&self, at: BlockRef) -> Result<Option<Prepared>, ReadError> {
        let block = self
            .call(Store::Unsafe, "unsafe block", || {
                self.unsafe_store.block(at.hash)
            })
            .await?;
        if let Some(block) = block.filter(|block| block.receipts.is_some()) {
            return Ok(Some(Prepared::new(StoredBlock::Decoded(Box::new(block)))?));
        }
        self.archived_with_receipts(at).await
    }

    /// The archive's `at`, if it holds it with its receipts.
    pub(crate) async fn archived_with_receipts(
        &self,
        at: BlockRef,
    ) -> Result<Option<Prepared>, ReadError> {
        Ok(self
            .archived(at.number, ONE_BLOCK)
            .await?
            .into_iter()
            .find(|block| block.at == at && block.has_receipts()))
    }

    /// What the stores hold. The unsafe store is read first: a block pruned from it was
    /// archived before, so a promotion between the two reads cannot open a false gap.
    pub(crate) async fn holdings(&self) -> Result<Holdings, ReadError> {
        let lowest_unsafe = self
            .call(Store::Unsafe, "unsafe lowest", || {
                self.unsafe_store.lowest()
            })
            .await?;
        Ok(Holdings {
            archive: self.archive_range().await?,
            lowest_unsafe,
            fills_gaps: self.fills_gaps,
        })
    }

    /// The unsafe head and the committed safe and finalized heads (the archive's: a block at
    /// or below the safe head is in the archive).
    pub(crate) async fn heads(&self) -> Result<(Option<BlockRef>, L1Heads), ReadError> {
        let unsafe_head = self
            .call(Store::Unsafe, "unsafe head", || self.unsafe_store.head())
            .await?;
        Ok((unsafe_head, self.archive_heads().await?))
    }

    /// The committed safe and finalized heads (the archive's).
    pub(crate) async fn archive_heads(&self) -> Result<L1Heads, ReadError> {
        Ok(self
            .call(Store::Archive, "archive heads", || self.archive.heads())
            .await?)
    }

    /// The archive's first and last block.
    pub(crate) async fn archive_range(&self) -> Result<Option<(BlockRef, BlockRef)>, ReadError> {
        Ok(self
            .call(Store::Archive, "archive range", || self.archive.range())
            .await?)
    }

    /// Whether `block` is canonical: the unsafe store's at its height, or archived.
    async fn is_canonical(&self, block: BlockRef) -> Result<bool, ReadError> {
        let number = self
            .call(Store::Unsafe, "unsafe canonical_number", || {
                self.unsafe_store.canonical_number(block.hash)
            })
            .await?;
        if number == Some(block.number) {
            return Ok(true);
        }
        let number = self
            .call(Store::Archive, "archive number_of", || {
                self.archive.number_of(block.hash)
            })
            .await?;
        Ok(number == Some(block.number))
    }

    /// Removes from `sent` (oldest first) the blocks above the newest one still canonical,
    /// and returns them, oldest first: what a reorg replaced. All of them if none is.
    pub(crate) async fn rewind(
        &self,
        sent: &mut VecDeque<BlockRef>,
    ) -> Result<Vec<BlockRef>, ReadError> {
        let mut keep = 0;
        for (index, block) in sent.iter().enumerate().rev() {
            if self.is_canonical(*block).await? {
                keep = index.saturating_add(1);
                break;
            }
        }
        Ok(sent.drain(keep..).collect())
    }
}

/// Pushes `block` onto `chain`, dropping the oldest past `cap`; returns the dropped one.
pub(crate) fn push_bounded(
    chain: &mut VecDeque<BlockRef>,
    block: BlockRef,
    cap: usize,
) -> Option<BlockRef> {
    chain.push_back(block);
    (chain.len() > cap).then(|| chain.pop_front()).flatten()
}
