-- Migration: 004_tasks
-- Task table aligned with sf-core Task model

CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(64) PRIMARY KEY,
    task_type VARCHAR(32) NOT NULL,
    status ENUM('pending', 'scheduled', 'running', 'completed', 'failed', 'cancelled') NOT NULL DEFAULT 'pending',
    input JSON NOT NULL,
    result JSON,
    error TEXT,
    blocked_by JSON NOT NULL DEFAULT (JSON_ARRAY()),
    blocks JSON NOT NULL DEFAULT (JSON_ARRAY()),
    priority INT NOT NULL DEFAULT 1,
    agent_id VARCHAR(64),
    workspace_id VARCHAR(64),
    retry_count INT UNSIGNED NOT NULL DEFAULT 0,
    max_retries INT UNSIGNED NOT NULL DEFAULT 3,
    timeout_seconds INT UNSIGNED NOT NULL DEFAULT 300,
    started_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_agent_id (agent_id),
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_created_at (created_at),
    INDEX idx_task_type (task_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
