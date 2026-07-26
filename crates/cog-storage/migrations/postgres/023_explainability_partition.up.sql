-- Migration: 023_explainability_partition
-- Add monthly partitioning and BRIN index for time-series queries.

-- Create partitioned table structure. Since PostgreSQL does not support
-- turning a regular table into a partitioned table in-place, we create
-- the partitioned version and migrate data.
CREATE TABLE IF NOT EXISTS explainability_part (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    record_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64),
    task_id VARCHAR(64),
    timestamp TIMESTAMPTZ NOT NULL,
    agent_id VARCHAR(64),
    decision_type VARCHAR(32) NOT NULL,
    confidence REAL,
    model VARCHAR(64),
    input_tokens BIGINT DEFAULT 0,
    output_tokens BIGINT DEFAULT 0,
    data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Initial monthly partition
CREATE TABLE IF NOT EXISTS explainability_y2026m05 PARTITION OF explainability_part
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS explainability_y2026m06 PARTITION OF explainability_part
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- BRIN index for lightweight time-range scans
CREATE INDEX IF NOT EXISTS idx_explain_brin ON explainability_part USING BRIN (timestamp);

-- Unique index on record_id (must include partition key timestamp)
CREATE UNIQUE INDEX IF NOT EXISTS idx_explain_record_id ON explainability_part (record_id, timestamp);
