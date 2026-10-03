//! Indexing pipeline.
//!
//! Writes unsafe blocks to hot storage and promotes them to cold storage once
//! they become safe or finalized on L1. Generic over the storage backends, so it
//! can be tested without Redis or ClickHouse.
