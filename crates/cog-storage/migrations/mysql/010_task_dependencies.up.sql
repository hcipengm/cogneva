-- Migration: 010_task_dependencies
-- DAG task dependency edges

CREATE TABLE IF NOT EXISTS task_dependencies (
    id BINARY(16) PRIMARY KEY,
    dag_id BINARY(16) NOT NULL,
    task_id BINARY(16) NOT NULL,
    depends_on BINARY(16) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_dag_task_depends (dag_id, task_id, depends_on),
    INDEX idx_dag_id (dag_id),
    INDEX idx_task_id (task_id),
    INDEX idx_depends_on (depends_on)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
