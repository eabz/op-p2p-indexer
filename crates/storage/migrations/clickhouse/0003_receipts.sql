CREATE TABLE IF NOT EXISTS receipts
(
    chain_id UInt64 CODEC(ZSTD(1)),
    block_number UInt64 CODEC(DoubleDelta, ZSTD(1)),
    tx_index UInt32 CODEC(T64, ZSTD(1)),
    block_hash FixedString(32) CODEC(ZSTD(1)),
    block_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
    tx_hash FixedString(32) CODEC(NONE),
    status UInt8 CODEC(T64, ZSTD(1)),
    cumulative_gas_used UInt64 CODEC(T64, ZSTD(1)),
    logs_count UInt32 CODEC(T64, ZSTD(1)),
    deposit_nonce Nullable(UInt64) CODEC(T64, ZSTD(1)),
    deposit_receipt_version Nullable(UInt64) CODEC(T64, ZSTD(1)),
    version UInt64 CODEC(Delta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (chain_id, block_number, tx_index)
SETTINGS min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1
