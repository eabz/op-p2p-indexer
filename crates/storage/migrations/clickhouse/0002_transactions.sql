CREATE TABLE IF NOT EXISTS transactions
(
    chain_id UInt64 CODEC(ZSTD(1)),
    block_number UInt64 CODEC(DoubleDelta, ZSTD(1)),
    tx_index UInt32 CODEC(T64, ZSTD(1)),
    block_hash FixedString(32) CODEC(NONE),
    block_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
    hash FixedString(32) CODEC(NONE),
    tx_type UInt8 CODEC(T64, ZSTD(1)),
    `from` FixedString(20) CODEC(ZSTD(1)),
    `to` Nullable(FixedString(20)) CODEC(ZSTD(1)),
    nonce Nullable(UInt64) CODEC(T64, ZSTD(1)),
    value UInt256 CODEC(ZSTD(1)),
    gas_limit UInt64 CODEC(T64, ZSTD(1)),
    -- ZSTD(1) instead of the spec's T64, ZSTD(1): ClickHouse rejects T64 for UInt128.
    gas_price Nullable(UInt128) CODEC(ZSTD(1)),
    max_fee_per_gas Nullable(UInt128) CODEC(ZSTD(1)),
    max_priority_fee_per_gas Nullable(UInt128) CODEC(ZSTD(1)),
    input String CODEC(ZSTD(3)),
    source_hash Nullable(FixedString(32)) CODEC(NONE),
    mint Nullable(UInt256) CODEC(ZSTD(1)),
    is_system_tx Nullable(Bool) CODEC(ZSTD(1)),
    raw String CODEC(ZSTD(3)),
    version UInt64 CODEC(Delta, ZSTD(1)),
    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (chain_id, block_number, tx_index)
SETTINGS min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1
