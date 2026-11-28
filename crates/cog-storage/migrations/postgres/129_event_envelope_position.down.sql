DROP INDEX IF EXISTS idx_cogneva_event_recorded_at;
DROP INDEX IF EXISTS idx_cogneva_event_seq;
ALTER TABLE cogneva_event
    DROP COLUMN IF EXISTS recorded_at,
    DROP COLUMN IF EXISTS seq;
