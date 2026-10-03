CREATE TABLE IF NOT EXISTS blocks
(
    chain_id UInt64 CODEC(ZSTD(1)),
    number UInt64 CODEC(DoubleDelta, ZSTD(1)),
    hash FixedString(32) CODEC(NONE),
    parent_hash FixedString(32) CODEC(NONE),
    timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
    -- One or a few fee vaults per chain: a dictionary stores each once.
    fee_recipient LowCardinality(FixedString(20)) CODEC(ZSTD(1)),
    state_root FixedString(32) CODEC(NONE),
    transactions_root FixedString(32) CODEC(NONE),
    receipts_root FixedString(32) CODEC(NONE),
    logs_bloom FixedString(256) CODEC(ZSTD(1)),
    prev_randao FixedString(32) CODEC(NONE),
    gas_limit UInt64 CODEC(T64, ZSTD(1)),
    gas_used UInt64 CODEC(T64, ZSTD(1)),
    base_fee_per_gas Nullable(UInt64) CODEC(T64, ZSTD(1)),
    extra_data String CODEC(ZSTD(1)),
    tx_count UInt32 CODEC(T64, ZSTD(1)),
    withdrawals_root Nullable(FixedString(32)) CODEC(NONE),
    blob_gas_used Nullable(UInt64) CODEC(T64, ZSTD(1)),
    excess_blob_gas Nullable(UInt64) CODEC(T64, ZSTD(1)),
    parent_beacon_block_root Nullable(FixedString(32)) CODEC(NONE),
    requests_hash Nullable(FixedString(32)) CODEC(NONE),
    source Enum8('gossip' = 0, 'l1' = 1) CODEC(ZSTD(1)),
    has_receipts Bool CODEC(ZSTD(1)),
    version UInt64 CODEC(Delta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (chain_id, number)
SETTINGS min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1
