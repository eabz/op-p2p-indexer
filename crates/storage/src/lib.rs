//! Block storage.
//!
//! Hot storage (Redis) holds unsafe blocks not yet derived from L1; cold storage
//! (ClickHouse) holds safe and finalized blocks. Knows nothing about networking.
