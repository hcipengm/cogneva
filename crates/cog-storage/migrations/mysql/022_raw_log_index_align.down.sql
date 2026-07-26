-- Migration: 022_raw_log_index_align (down)
DROP INDEX idx_raw_log_index_hour ON raw_log_index;
ALTER TABLE raw_log_index
    DROP COLUMN hour,
    CHANGE COLUMN event_count record_count INT UNSIGNED NOT NULL DEFAULT 0;
