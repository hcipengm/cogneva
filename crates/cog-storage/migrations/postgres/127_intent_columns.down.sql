DROP INDEX IF EXISTS idx_task_defined_intent_id;
ALTER TABLE task_defined
    DROP COLUMN IF EXISTS intent_id;
