-- Migration: 015_recharge_records
-- Token quota recharge records

CREATE TABLE IF NOT EXISTS recharge_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_type VARCHAR(16) NOT NULL CHECK (target_type IN ('user', 'workspace')),
    target_id UUID NOT NULL,
    amount NUMERIC(18, 8) NOT NULL,
    tokens_added BIGINT NOT NULL,
    valid_until TIMESTAMPTZ,
    operator_id UUID NOT NULL REFERENCES users(id),
    remark TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_recharge_records_target ON recharge_records(target_type, target_id, created_at DESC);
CREATE INDEX idx_recharge_records_operator ON recharge_records(operator_id);
