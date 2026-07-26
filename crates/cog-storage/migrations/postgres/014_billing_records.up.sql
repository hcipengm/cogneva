-- Migration: 014_billing_records
-- Token consumption billing records (time-series partitioned table)

CREATE TABLE IF NOT EXISTS billing_records (
    id UUID NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id),
    workspace_id VARCHAR(64) REFERENCES workspaces(id) ON DELETE SET NULL,
    task_id UUID,
    model VARCHAR(64) NOT NULL,
    task_kind VARCHAR(32) NOT NULL DEFAULT 'custom',
    tokens_input BIGINT NOT NULL DEFAULT 0,
    tokens_output BIGINT NOT NULL DEFAULT 0,
    cost NUMERIC(18, 8) NOT NULL DEFAULT 0.0,
    cost_before_weight NUMERIC(18, 8) NOT NULL DEFAULT 0.0,
    weight_applied NUMERIC(5, 4) NOT NULL DEFAULT 1.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Initial monthly partitions
CREATE TABLE IF NOT EXISTS billing_records_y2026m05 PARTITION OF billing_records
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS billing_records_y2026m06 PARTITION OF billing_records
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX idx_billing_records_user_id ON billing_records(user_id, created_at DESC);
CREATE INDEX idx_billing_records_workspace_id ON billing_records(workspace_id, created_at DESC);
CREATE INDEX idx_billing_records_model ON billing_records(model, created_at DESC);
