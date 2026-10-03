CREATE TABLE IF NOT EXISTS schema_migrations
(
    version UInt32 CODEC(ZSTD(1)),
    name String CODEC(ZSTD(1)),
    checksum FixedString(32) CODEC(NONE),
    applied_at DateTime('UTC') CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY version
