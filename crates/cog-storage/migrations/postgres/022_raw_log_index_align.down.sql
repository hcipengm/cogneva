-- Migration: 022_raw_log_index_align (down)
DROP INDEX IF EXISTS idx_raw_log_index_hour;
ALTER TABLE raw_log_index
    RENAME COLUMN event_count TO record_count,
    ALTER COLUMN record_count TYPE INTEGER,
    DROP COLUMN IF EXISTS hour;
