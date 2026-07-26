-- Migration: 010_task_dependencies
-- DAG task dependency edges

CREATE TABLE IF NOT EXISTS task_dependencies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id UUID NOT NULL,
    task_id UUID NOT NULL,
    depends_on UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_id, task_id, depends_on)
);

CREATE INDEX idx_task_dependencies_dag_id ON task_dependencies(dag_id);
CREATE INDEX idx_task_dependencies_task_id ON task_dependencies(task_id);
CREATE INDEX idx_task_dependencies_depends_on ON task_dependencies(depends_on);
