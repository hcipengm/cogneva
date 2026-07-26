-- Migration: 008_chat_sessions
-- AI chat sessions aligned with sf-core session model

CREATE TABLE IF NOT EXISTS chat_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    workspace_id VARCHAR(64) REFERENCES workspaces(id) ON DELETE SET NULL,
    title VARCHAR(256),
    agent_id VARCHAR(64),
    status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived', 'pinned')),
    context JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_chat_sessions_user_id ON chat_sessions(user_id);
CREATE INDEX idx_chat_sessions_workspace_id ON chat_sessions(workspace_id);
CREATE INDEX idx_chat_sessions_status ON chat_sessions(status);
CREATE INDEX idx_chat_sessions_user_updated ON chat_sessions(user_id, updated_at DESC);
