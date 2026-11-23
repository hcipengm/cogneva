DROP INDEX IF EXISTS idx_task_defined_intent_id;
ALTER TABLE task_defined
    DROP COLUMN IF EXISTS intent_id;

DROP INDEX IF EXISTS idx_cogneva_event_intent_at;
ALTER TABLE cogneva_event
    DROP COLUMN IF EXISTS intent_parent,
    DROP COLUMN IF EXISTS intent_text,
    DROP COLUMN IF EXISTS intent_external_id,
    DROP COLUMN IF EXISTS intent_source;
