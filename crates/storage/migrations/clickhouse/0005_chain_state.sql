CREATE TABLE IF NOT EXISTS chain_state
(
    chain_id UInt64 CODEC(ZSTD(1)),
    key Enum8('safe_head' = 0, 'finalized_head' = 1) CODEC(ZSTD(1)),
    number UInt64 CODEC(DoubleDelta, ZSTD(1)),
    hash FixedString(32) CODEC(NONE),
    updated_at UInt64 CODEC(Delta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, key)
