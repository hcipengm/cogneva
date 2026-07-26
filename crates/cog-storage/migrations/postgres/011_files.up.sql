-- Migration: 011_files
-- File metadata for uploaded files

CREATE TABLE IF NOT EXISTS files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    workspace_id VARCHAR(64) REFERENCES workspaces(id) ON DELETE SET NULL,
    file_name VARCHAR(255) NOT NULL,
    file_type VARCHAR(128) NOT NULL,
    file_size BIGINT NOT NULL,
    storage_path TEXT NOT NULL,
    storage_provider VARCHAR(32) NOT NULL DEFAULT 'local-fs',
    purpose VARCHAR(32) NOT NULL DEFAULT 'chat' CHECK (purpose IN ('chat', 'knowledge', 'avatar')),
    checksum VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_files_user_id ON files(user_id);
CREATE INDEX idx_files_workspace_id ON files(workspace_id);
CREATE INDEX idx_files_created_at ON files(created_at);
