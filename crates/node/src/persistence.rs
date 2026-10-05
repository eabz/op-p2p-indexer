//! Opens stores and drains state updates after producers stop; does not choose sync policy.

use crate::Archive;
use eyre::WrapErr;
use op_indexer_p2p::{NodeStore, StoreError};
use op_indexer_primitives::{BeaconCheckpoint, BlockRef, ExecutionPeer};
use op_indexer_storage::{StorageConfig, unsafe_store::MemoryStore};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

/// Saves the peers of one execution network that served us, with `save`, so the next run
/// dials them first. Ends when that network drops its sender.
pub(crate) async fn save_peers(
    store: Arc<NodeStore>,
    mut served: mpsc::Receiver<ExecutionPeer>,
    save: fn(&NodeStore, &ExecutionPeer) -> Result<(), StoreError>,
) {
    while let Some(peer) = served.recv().await {
        let store = Arc::clone(&store);
        match tokio::task::spawn_blocking(move || save(&store, &peer)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to save execution peer"),
            Err(err) => warn!(%err, "execution peer save task failed"),
        }
    }
}

/// Saves each newer finalized beacon block the light client verifies, so a restart
/// bootstraps from it. Ends when the light client drops its sender.
pub(crate) async fn save_l1_checkpoints(
    store: Arc<NodeStore>,
    mut finalized: watch::Receiver<Option<BeaconCheckpoint>>,
) {
    while finalized.changed().await.is_ok() {
        let Some(checkpoint) = *finalized.borrow_and_update() else {
            continue;
        };
        let store = Arc::clone(&store);
        match tokio::task::spawn_blocking(move || store.save_l1_checkpoint(&checkpoint)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to save the beacon checkpoint"),
            Err(err) => warn!(%err, "beacon checkpoint save task failed"),
        }
    }
}

/// Saves the checkpoints the range sync verifies, so a restart does not walk the header
/// chain again. Ends when the execution network drops its sender.
pub(crate) async fn save_sync_checkpoints(
    store: Arc<NodeStore>,
    mut verified: mpsc::Receiver<Vec<BlockRef>>,
) {
    while let Some(checkpoints) = verified.recv().await {
        let store = Arc::clone(&store);
        let save = tokio::task::spawn_blocking(move || store.save_sync_checkpoints(&checkpoints));
        match save.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to save range sync checkpoints"),
            Err(err) => warn!(%err, "range sync checkpoint save task failed"),
        }
    }
}

/// The two stores, connected and ready.
pub(crate) struct Stores<A> {
    pub(crate) unsafe_store: MemoryStore,
    /// The block archive, the committed store.
    pub(crate) archive: A,
    /// The archive's first and last block at startup; `None` when it is empty.
    pub(crate) archive_range: Option<(BlockRef, BlockRef)>,
}

/// Reads the range of `archive` (opened by the binary), opens the unsafe chain and replays its
/// journal, and returns the stores once both are ready, so a mismatched store stops startup.
pub(crate) async fn prepare_storage<A: Archive>(
    config: &StorageConfig,
    archive: A,
) -> eyre::Result<Stores<A>> {
    let archive_range = archive
        .range()
        .await
        .wrap_err("failed to read the block archive")?;
    info!(range = ?archive_range, "block archive ready");
    let (unsafe_config, chain) = (config.unsafe_chain.clone(), config.chain);
    let unsafe_store =
        tokio::task::spawn_blocking(move || MemoryStore::open(&unsafe_config, chain))
            .await
            .wrap_err("the unsafe chain's opening panicked")?
            .wrap_err("failed to open the unsafe chain")?;
    info!(?config, "storage ready");
    Ok(Stores {
        unsafe_store,
        archive,
        archive_range,
    })
}
