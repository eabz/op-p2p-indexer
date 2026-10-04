//! What the server reads from: the blocks this node holds, as the binary provides them.

use std::fmt;

use alloy_primitives::Bytes;
use op_indexer_primitives::{BlockRead, BlockRef, ItemConvert, ReadLimits};

/// The first and the last block held.
pub(crate) type HeldRange = (BlockRef, BlockRef);

/// The blocks this node can serve: one contiguous range, each block in its consensus encoding.
///
/// The binary implements it over its local archive. The bytes it returns are sent to peers as
/// they are (receipts without their blooms), so they must be the block's original encoding.
pub trait BlockProvider: fmt::Debug + Send + Sync + 'static {
    /// Why a read failed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Reads a run of headers, bodies or receipts (each block's receipts as an RLP list, every
    /// receipt in network encoding with its bloom), each item as the RLP held, ending at the
    /// first block not held and at `limits`. One call answers one request, so it should be one
    /// read of the store, off the async runtime.
    ///
    /// With `convert`, each item is passed through it, on the thread of the read, before it
    /// counts against the limits and is returned; an item it returns `None` for ends the run.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> impl Future<Output = Result<Vec<Bytes>, Self::Error>> + Send;

    /// Returns the first and the last block held, or `None` if nothing is held.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the store cannot be read.
    fn range(&self) -> impl Future<Output = Result<Option<HeldRange>, Self::Error>> + Send;
}
