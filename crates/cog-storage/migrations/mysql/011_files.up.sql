-- Migration: 011_files
-- File metadata for uploaded files

CREATE TABLE IF NOT EXISTS files (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    workspace_id BINARY(16),
    file_name VARCHAR(255) NOT NULL,
    file_type VARCHAR(128) NOT NULL,
    file_size BIGINT UNSIGNED NOT NULL,
    storage_path TEXT NOT NULL,
    storage_provider VARCHAR(32) NOT NULL DEFAULT 'local-fs',
    purpose ENUM('chat', 'knowledge', 'avatar') NOT NULL DEFAULT 'chat',
    checksum VARCHAR(64),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME,
    INDEX idx_user_id (user_id),
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_created_at (created_at),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
