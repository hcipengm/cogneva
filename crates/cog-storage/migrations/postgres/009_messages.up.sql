-- Migration: 009_messages
-- Chat messages (time-series partitioned table)

CREATE TABLE IF NOT EXISTS messages (
    id UUID NOT NULL,
    session_id UUID NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role VARCHAR(16) NOT NULL CHECK (role IN ('system', 'user', 'assistant', 'tool')),
    content TEXT NOT NULL,
    agent_id VARCHAR(64),
    task_id UUID,
    tool_calls JSONB,
    usage_input_tokens INTEGER,
    usage_output_tokens INTEGER,
    usage_total_tokens INTEGER,
    file_attachments JSONB NOT NULL DEFAULT '[]'::jsonb,
    status VARCHAR(16) NOT NULL DEFAULT 'completed' CHECK (status IN ('sending', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Initial monthly partitions
CREATE TABLE IF NOT EXISTS messages_y2026m05 PARTITION OF messages
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS messages_y2026m06 PARTITION OF messages
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX idx_messages_session_id ON messages(session_id, created_at DESC);
CREATE INDEX idx_messages_task_id ON messages(task_id) WHERE task_id IS NOT NULL;
CREATE INDEX idx_messages_agent_id ON messages(agent_id) WHERE agent_id IS NOT NULL;
