-- 撤回 111：两张表与一列按原样建回来。`workspaces` 没被动过（只把 `settings`
-- 转成了 text），所以这里也不重建它；`settings` 的 jsonb 形状是回滚这一面时
-- 唯一要换回去的东西。
--
-- agents 这一张在 005 里还带了一张 agent_performance；那张表 085 已经删过，
-- 这里不重建——down 的职责是把 111 自己的决定撤回，不是把更早的决定一起翻回来。
CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(64) PRIMARY KEY,
    task_type VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'scheduled', 'running', 'completed', 'failed', 'cancelled')),
    input JSONB NOT NULL,
    result JSONB,
    error TEXT,
    blocked_by JSONB NOT NULL DEFAULT '[]'::jsonb,
    blocks JSONB NOT NULL DEFAULT '[]'::jsonb,
    priority INT NOT NULL DEFAULT 1,
    agent_id VARCHAR(64),
    workspace_id VARCHAR(64),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    timeout_seconds INT NOT NULL DEFAULT 300,
    started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_agent_id ON tasks(agent_id);
CREATE INDEX IF NOT EXISTS idx_tasks_workspace_id ON tasks(workspace_id);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_task_type ON tasks(task_type);

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
CREATE INDEX IF NOT EXISTS idx_agents_workspace_id ON agents(workspace_id);
CREATE INDEX IF NOT EXISTS idx_agents_status ON agents(status);
CREATE INDEX IF NOT EXISTS idx_agents_role ON agents(role);
CREATE INDEX IF NOT EXISTS idx_agents_last_heartbeat ON agents(last_heartbeat_at);

ALTER TABLE user_auth_methods ADD COLUMN IF NOT EXISTS extra JSONB;

ALTER TABLE IF EXISTS workspaces ALTER COLUMN settings TYPE JSONB USING settings::jsonb;
