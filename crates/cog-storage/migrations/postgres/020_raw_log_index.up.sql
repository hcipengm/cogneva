-- Migration: 020_raw_log_index
-- Raw data log index for locating protobuf files in object storage.
-- The table is partitioned by `log_date` (monthly), so the unique key
-- must include the partition column. The runtime store in `sf-core`
-- treats `(stream_name, log_date)` as the logical primary key.

CREATE TABLE IF NOT EXISTS raw_log_index (
    stream_name VARCHAR(32) NOT NULL CHECK (stream_name IN (
        'session_raw', 'task_raw', 'agent_raw', 'llm_raw',
        'tool_raw', 'system_raw', 'transport_raw'
    )),
    log_date DATE NOT NULL,
    storage_path TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(128) NOT NULL,
    first_at TIMESTAMPTZ NOT NULL,
    last_at TIMESTAMPTZ NOT NULL,
    is_hot BOOLEAN NOT NULL DEFAULT true,
    encoding VARCHAR(16) NOT NULL DEFAULT 'protobuf' CHECK (encoding IN (
        'protobuf', 'protobuf_zstd3', 'protobuf_zstd9', 'parquet'
    )),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_name, log_date)
) PARTITION BY RANGE (log_date);

-- Initial monthly partitions
CREATE TABLE IF NOT EXISTS raw_log_index_y2026m05 PARTITION OF raw_log_index
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS raw_log_index_y2026m06 PARTITION OF raw_log_index
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX IF NOT EXISTS idx_raw_log_index_stream_date
    ON raw_log_index(stream_name, log_date DESC);
CREATE INDEX IF NOT EXISTS idx_raw_log_index_path
    ON raw_log_index(storage_path);
CREATE INDEX IF NOT EXISTS idx_raw_log_index_time_range
    ON raw_log_index(stream_name, first_at, last_at);
