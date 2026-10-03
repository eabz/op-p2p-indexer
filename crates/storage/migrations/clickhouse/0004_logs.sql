CREATE TABLE IF NOT EXISTS logs
(
    chain_id UInt64 CODEC(ZSTD(1)),
    block_number UInt64 CODEC(DoubleDelta, ZSTD(1)),
    log_index UInt32 CODEC(T64, ZSTD(1)),
    block_hash FixedString(32) CODEC(NONE),
    block_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
    tx_index UInt32 CODEC(T64, ZSTD(1)),
    tx_hash FixedString(32) CODEC(NONE),
    address FixedString(20) CODEC(ZSTD(1)),
    topic0 Nullable(FixedString(32)) CODEC(ZSTD(1)),
    topic1 Nullable(FixedString(32)) CODEC(NONE),
    topic2 Nullable(FixedString(32)) CODEC(NONE),
    topic3 Nullable(FixedString(32)) CODEC(NONE),
    data String CODEC(ZSTD(3)),
    version UInt64 CODEC(Delta, ZSTD(1)),
    INDEX idx_address address TYPE bloom_filter GRANULARITY 4,
    INDEX idx_topic0 topic0 TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (chain_id, block_number, log_index)
SETTINGS min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1
