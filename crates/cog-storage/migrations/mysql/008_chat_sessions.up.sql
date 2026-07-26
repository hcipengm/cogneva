-- Migration: 008_chat_sessions
-- AI chat sessions aligned with sf-core session model

CREATE TABLE IF NOT EXISTS chat_sessions (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    workspace_id BINARY(16),
    title VARCHAR(256),
    agent_id VARCHAR(64),
    status ENUM('active', 'archived', 'pinned') NOT NULL DEFAULT 'active',
    context JSON NOT NULL DEFAULT (JSON_OBJECT()),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME,
    INDEX idx_user_id (user_id),
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_status (status),
    INDEX idx_user_updated (user_id, updated_at DESC),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
