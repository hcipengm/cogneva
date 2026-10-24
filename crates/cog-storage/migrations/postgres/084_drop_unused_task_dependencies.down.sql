-- Migration: 084_drop_unused_task_dependencies (down)
-- Restores the 010_task_dependencies shape verbatim. It stays empty either way:
-- no code path writes it. Recreating it here only keeps down-migrations
-- reversible.

CREATE TABLE IF NOT EXISTS task_dependencies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id UUID NOT NULL,
    task_id UUID NOT NULL,
    depends_on UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_id, task_id, depends_on)
);

CREATE INDEX IF NOT EXISTS idx_task_dependencies_dag_id ON task_dependencies(dag_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_task_id ON task_dependencies(task_id);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_depends_on ON task_dependencies(depends_on);
