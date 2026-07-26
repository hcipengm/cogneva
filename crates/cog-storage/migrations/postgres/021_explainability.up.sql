-- Migration: 021_explainability
-- Explainability data for AI decision tracing (type 15).

CREATE TABLE IF NOT EXISTS explainability (
    id BIGSERIAL PRIMARY KEY,
    record_id VARCHAR(64) NOT NULL UNIQUE,
    session_id VARCHAR(64),
    task_id VARCHAR(64),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent_id VARCHAR(64),
    decision_type VARCHAR(32) NOT NULL,
    confidence REAL,
    model VARCHAR(64),
    input_tokens BIGINT DEFAULT 0,
    output_tokens BIGINT DEFAULT 0,
    data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_explain_lookup ON explainability(session_id, timestamp DESC, decision_type);
CREATE INDEX idx_explain_time ON explainability(timestamp);
CREATE INDEX idx_explain_task ON explainability(task_id);
CREATE INDEX idx_explain_agent ON explainability(agent_id);
CREATE INDEX idx_explain_record ON explainability(record_id);
