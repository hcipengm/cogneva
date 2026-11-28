DROP INDEX IF EXISTS cogneva_event_session_id_idx;
ALTER TABLE cogneva_event DROP COLUMN IF EXISTS session_id;
