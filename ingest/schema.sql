-- Attie AT Protocol Data Schema (consolidated)
-- Optimized for billions of records
--
-- Uses the Replicated database engine so that DDL is automatically propagated
-- to all replicas via Keeper. Tables use ReplicatedMergeTree without explicit
-- ZooKeeper paths (the database manages replication paths automatically).

CREATE DATABASE IF NOT EXISTS attie ENGINE = Replicated('/clickhouse/databases/attie', '{shard}', '{replica}');

-- Main records table for all AT Protocol records
CREATE TABLE IF NOT EXISTS attie.records (
    uri String,
    did String,
    collection String,
    rkey String,
    record String,  -- JSON blob
    created_at DateTime64(3) DEFAULT now64(3),
    indexed_at DateTime64(3) DEFAULT now64(3),
    seq UInt64 DEFAULT 0,

    -- Bloom filter indexes for fast filtering
    INDEX idx_collection collection TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_record record TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
) ENGINE = ReplicatedReplacingMergeTree(indexed_at)
-- Partition by month, but consolidate all pre-2023 data (Twitter imports, etc.) into one partition
PARTITION BY if(toYear(created_at) < 2023, 202200, toYYYYMM(created_at))
ORDER BY (collection, did, rkey)
SETTINGS
    index_granularity = 8192,
    parts_to_delay_insert = 500,
    parts_to_throw_insert = 5000;

-- Cursor tracking for firehose position
CREATE TABLE IF NOT EXISTS attie.cursors (
    name String,
    cursor Int64,
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY name;

-- Collection statistics materialized view for fast stats
CREATE MATERIALIZED VIEW IF NOT EXISTS attie.collection_stats
ENGINE = ReplicatedSummingMergeTree()
ORDER BY collection
AS SELECT
    collection,
    count() as record_count
FROM attie.records
GROUP BY collection;

-- Backlinks table for indexing references (DIDs and AT-URIs) found in records
CREATE TABLE IF NOT EXISTS attie.backlinks (
    ref String,
    ref_type Enum8('did' = 1, 'uri' = 2),
    collection String,
    path String,
    source_uri String,
    source_did String,
    created_at DateTime64(3),
    indexed_at DateTime64(3) DEFAULT now64(3),

    INDEX idx_source_did source_did TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_collection collection TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = ReplicatedReplacingMergeTree(indexed_at)
ORDER BY (ref, collection, source_uri)
PARTITION BY toYYYYMM(created_at)
SETTINGS
    index_granularity = 8192,
    parts_to_delay_insert = 500,
    parts_to_throw_insert = 5000;

-- Pending deletes table for deferred deletion
CREATE TABLE IF NOT EXISTS attie.pending_deletes (
    uri String,
    table_name LowCardinality(String),
    created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree()
ORDER BY (table_name, created_at);

-- Time-optimized posts view (from migration 002)
CREATE TABLE IF NOT EXISTS attie.posts_by_time_data
(
    uri String,
    did String,
    rkey String,
    created_at DateTime,
    indexed_at DateTime,
    text String,
    reply_parent String,
    INDEX idx_text text TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
)
ENGINE = ReplicatedReplacingMergeTree(indexed_at)
ORDER BY (created_at, uri)
PARTITION BY toYYYYMM(created_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS attie.posts_by_time
TO attie.posts_by_time_data
AS SELECT
    uri,
    did,
    rkey,
    created_at,
    indexed_at,
    JSONExtractString(record, 'text') as text,
    JSONExtractString(record, 'reply', 'parent', 'uri') as reply_parent
FROM attie.records
WHERE collection = 'app.bsky.feed.post';

