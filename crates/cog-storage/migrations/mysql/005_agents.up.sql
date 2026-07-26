-- Migration: 005_agents
-- Agent registry and performance tracking

CREATE TABLE IF NOT EXISTS agents (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL,
    workspace_id VARCHAR(64),
    status ENUM('online', 'offline', 'busy', 'error') NOT NULL DEFAULT 'offline',
    capabilities JSON,
    metadata JSON,
    last_heartbeat_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_status (status),
    INDEX idx_role (role),
    INDEX idx_last_heartbeat (last_heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_performance (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    agent_id VARCHAR(64) NOT NULL,
    workspace_id VARCHAR(64),
    role VARCHAR(32) NOT NULL,
    tasks_completed INT UNSIGNED DEFAULT 0,
    tasks_failed INT UNSIGNED DEFAULT 0,
    avg_task_duration_ms INT UNSIGNED DEFAULT 0,
    token_consumed BIGINT UNSIGNED DEFAULT 0,
    score_avg FLOAT DEFAULT 0,
    recorded_at DATE NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_agent_workspace_date (agent_id, workspace_id, recorded_at),
    INDEX idx_agent_id (agent_id),
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_recorded_at (recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
