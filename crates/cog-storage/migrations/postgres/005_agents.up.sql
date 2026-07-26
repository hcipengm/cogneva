-- Migration: 005_agents
-- Agent registry and performance tracking

CREATE TABLE IF NOT EXISTS agents (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL,
    workspace_id VARCHAR(64),
    status VARCHAR(16) NOT NULL DEFAULT 'offline' CHECK (status IN ('online', 'offline', 'busy', 'error')),
    capabilities JSONB,
    metadata JSONB,
    last_heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agents_workspace_id ON agents(workspace_id);
CREATE INDEX idx_agents_status ON agents(status);
CREATE INDEX idx_agents_role ON agents(role);
CREATE INDEX idx_agents_last_heartbeat ON agents(last_heartbeat_at);

CREATE TABLE IF NOT EXISTS agent_performance (
    id BIGSERIAL PRIMARY KEY,
    agent_id VARCHAR(64) NOT NULL,
    workspace_id VARCHAR(64),
    role VARCHAR(32) NOT NULL,
    tasks_completed INT DEFAULT 0,
    tasks_failed INT DEFAULT 0,
    avg_task_duration_ms INT DEFAULT 0,
    token_consumed BIGINT DEFAULT 0,
    score_avg REAL DEFAULT 0,
    recorded_at DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_id, workspace_id, recorded_at)
);

CREATE INDEX idx_agent_performance_agent_id ON agent_performance(agent_id);
CREATE INDEX idx_agent_performance_workspace_id ON agent_performance(workspace_id);
CREATE INDEX idx_agent_performance_recorded_at ON agent_performance(recorded_at);
