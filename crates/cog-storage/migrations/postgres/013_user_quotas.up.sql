-- Migration: 013_user_quotas
-- User token quota tracking (daily/monthly/total)

CREATE TABLE IF NOT EXISTS user_quotas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    quota_type VARCHAR(32) NOT NULL DEFAULT 'daily' CHECK (quota_type IN ('daily', 'monthly', 'total')),
    total_tokens BIGINT NOT NULL DEFAULT 0,
    used_tokens BIGINT NOT NULL DEFAULT 0,
    reset_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, quota_type)
);

CREATE INDEX idx_user_quotas_user_id ON user_quotas(user_id);
