//! The blocks this node serves to execution peers: the local block archive behind the
//! execution network's [`BlockProvider`].
//!
//! The archive holds each block in the encoding it was received in, and that is what is
//! returned: nothing is decoded or encoded here.

use alloy_primitives::Bytes;
use op_indexer_el::BlockProvider;
use op_indexer_primitives::{BlockRead, BlockRef, ItemConvert, ReadLimits};
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::{ArchiveStore, StorageError};

/// The local block archive, as the execution network reads it.
#[derive(Debug, Clone)]
pub(crate) struct ArchiveProvider(pub(crate) FjallArchive);

impl BlockProvider for ArchiveProvider {
    type Error = StorageError;

    async fn read(
        &self,
        read: BlockRead,
        limits: ReadLimits,
        convert: Option<ItemConvert>,
    ) -> Result<Vec<Bytes>, StorageError> {
        self.0.read(read, limits, convert).await
    }

    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError> {
        self.0.range().await
    }
}
