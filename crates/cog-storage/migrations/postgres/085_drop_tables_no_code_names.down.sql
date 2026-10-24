-- Migration: 085_drop_tables_no_code_names (down)
-- Restores the nine shapes verbatim from the migrations that created them.

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

CREATE TABLE IF NOT EXISTS model_pricing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_id VARCHAR(64) NOT NULL UNIQUE,
    model_name VARCHAR(128) NOT NULL,
    input_price NUMERIC(18, 8) NOT NULL,
    output_price NUMERIC(18, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'CNY',
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_until TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS user_devices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id VARCHAR(64) NOT NULL,
    platform VARCHAR(16) NOT NULL CHECK (platform IN ('ios', 'android', 'harmonyos', 'web', 'desktop')),
    device_model VARCHAR(128),
    os_version VARCHAR(32),
    app_version VARCHAR(32),
    push_token TEXT,
    push_provider VARCHAR(16),
    biometric_enabled BOOLEAN NOT NULL DEFAULT false,
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, device_id)
);

CREATE TABLE IF NOT EXISTS user_biometric_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id VARCHAR(64) NOT NULL,
    public_key TEXT NOT NULL,
    key_type VARCHAR(16) NOT NULL DEFAULT 'ecdsa' CHECK (key_type IN ('ecdsa', 'rsa')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    UNIQUE (user_id, device_id)
);

CREATE TABLE IF NOT EXISTS memory_raw_sources (
    id VARCHAR(64) PRIMARY KEY,
    content_type VARCHAR(64) NOT NULL,
    payload BYTEA NOT NULL,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS memory_schema_entries (
    id VARCHAR(64) PRIMARY KEY,
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('entity', 'relation', 'event', 'sentiment', 'custom')),
    name VARCHAR(255) NOT NULL,
    key_name VARCHAR(255) NOT NULL,
    properties JSONB NOT NULL,
    source_raw_uri VARCHAR(512) NOT NULL,
    extractor_version VARCHAR(64) NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS memory_summary_entries (
    id VARCHAR(64) PRIMARY KEY,
    text TEXT NOT NULL,
    embedding BYTEA,
    embedding_model VARCHAR(64) NOT NULL,
    source_raw_uri VARCHAR(512) NOT NULL,
    related_schema_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    confidence REAL NOT NULL DEFAULT 1.0,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS principle_debt (
    id           BIGSERIAL PRIMARY KEY,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    by_kind      TEXT NOT NULL,
    submitted    BIGINT NOT NULL DEFAULT 0,
    already      BIGINT NOT NULL DEFAULT 0,
    failed       BIGINT NOT NULL DEFAULT 0,
    over_bound   BIGINT NOT NULL DEFAULT 0,
    bound        BIGINT NOT NULL,
    bound_source VARCHAR(64) NOT NULL,
    rev          VARCHAR(64),
    delta        BIGINT
);
