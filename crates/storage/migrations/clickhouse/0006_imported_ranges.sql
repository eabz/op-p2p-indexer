CREATE TABLE IF NOT EXISTS imported_ranges
(
    chain_id UInt64 CODEC(ZSTD(1)),
    first UInt64 CODEC(DoubleDelta, ZSTD(1)),
    last UInt64 CODEC(DoubleDelta, ZSTD(1)),
    loaded_at DateTime('UTC') CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (chain_id, first, last)
