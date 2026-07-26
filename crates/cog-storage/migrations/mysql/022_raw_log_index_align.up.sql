-- Migration: 022_raw_log_index_align
-- Align raw_log_index schema with design doc:
--   - Add hour column for hourly rollup granularity
--   - Rename record_count → event_count and upgrade to BIGINT

ALTER TABLE raw_log_index
    ADD COLUMN hour SMALLINT NOT NULL DEFAULT 0,
    CHANGE COLUMN record_count event_count BIGINT UNSIGNED NOT NULL DEFAULT 0;

CREATE INDEX idx_raw_log_index_hour ON raw_log_index (stream_name, log_date, hour);
