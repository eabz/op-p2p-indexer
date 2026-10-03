//! Indexing pipeline.
//!
//! Writes unsafe blocks to the unsafe store and promotes them to the committed store once
//! they become safe or finalized on L1. Generic over the storage backends, so it
//! can be tested without Redis or ClickHouse.
