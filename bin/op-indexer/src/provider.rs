//! The blocks this node serves to execution peers: the local block archive behind the
//! execution network's [`BlockProvider`].
//!
//! The archive holds each block in the encoding it was received in, and that is what is
//! returned: nothing is decoded or encoded here.

use alloy_primitives::{BlockHash, BlockNumber, Bytes};
use op_indexer_el::BlockProvider;
use op_indexer_primitives::BlockRef;
use op_indexer_storage::archive_store::FjallArchive;
use op_indexer_storage::{ArchiveStore, BlockPart, StorageError};

/// The local block archive, as the execution network reads it.
#[derive(Debug, Clone)]
pub(crate) struct ArchiveProvider(pub(crate) FjallArchive);

impl BlockProvider for ArchiveProvider {
    type Error = StorageError;

    async fn header(&self, number: BlockNumber) -> Result<Option<Bytes>, StorageError> {
        self.0.part(number, BlockPart::Header).await
    }

    async fn body(&self, number: BlockNumber) -> Result<Option<Bytes>, StorageError> {
        self.0.part(number, BlockPart::Body).await
    }

    async fn receipts(&self, number: BlockNumber) -> Result<Option<Bytes>, StorageError> {
        self.0.part(number, BlockPart::Receipts).await
    }

    async fn number_of(&self, hash: BlockHash) -> Result<Option<BlockNumber>, StorageError> {
        self.0.number_of(hash).await
    }

    async fn range(&self) -> Result<Option<(BlockRef, BlockRef)>, StorageError> {
        self.0.range().await
    }
}
