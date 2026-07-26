-- Migration: 007_workspace_members
-- Workspace member relationships for multi-tenant isolation

CREATE TABLE IF NOT EXISTS workspace_members (
    id BINARY(16) PRIMARY KEY,
    workspace_id BINARY(16) NOT NULL,
    user_id BINARY(16) NOT NULL,
    role ENUM('visitor', 'member', 'admin', 'owner') NOT NULL DEFAULT 'member',
    joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_workspace_user (workspace_id, user_id),
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_user_id (user_id),
    INDEX idx_role (role),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
