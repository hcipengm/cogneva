-- Migration: 009_messages
-- Chat messages (time-series table; MySQL range partitioning available in 8.0+)

CREATE TABLE IF NOT EXISTS messages (
    id BINARY(16) NOT NULL,
    session_id BINARY(16) NOT NULL,
    role ENUM('system', 'user', 'assistant', 'tool') NOT NULL,
    content TEXT NOT NULL,
    agent_id VARCHAR(64),
    task_id BINARY(16),
    tool_calls JSON,
    usage_input_tokens INT,
    usage_output_tokens INT,
    usage_total_tokens INT,
    file_attachments JSON NOT NULL DEFAULT (JSON_ARRAY()),
    status ENUM('sending', 'completed', 'failed') NOT NULL DEFAULT 'completed',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at),
    INDEX idx_session_id (session_id, created_at DESC),
    INDEX idx_task_id (task_id),
    INDEX idx_agent_id (agent_id),
    FOREIGN KEY (session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
