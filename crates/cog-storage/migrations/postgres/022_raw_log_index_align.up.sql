-- Migration: 022_raw_log_index_align
-- Align raw_log_index schema with design doc:
--   - Add hour column for hourly rollup granularity
--   - Rename record_count → event_count and upgrade to BIGINT

ALTER TABLE raw_log_index
    ADD COLUMN IF NOT EXISTS hour SMALLINT NOT NULL DEFAULT 0;

ALTER TABLE raw_log_index
    ALTER COLUMN record_count TYPE BIGINT;

ALTER TABLE raw_log_index
    RENAME COLUMN record_count TO event_count;

-- Index for hour-based queries
CREATE INDEX IF NOT EXISTS idx_raw_log_index_hour
    ON raw_log_index (stream_name, log_date, hour);
