-- Migration: 006_memory
-- Three-tier memory storage aligned with sf-core memory types

-- Layer 0 — Raw Sources
CREATE TABLE IF NOT EXISTS memory_raw_sources (
    id VARCHAR(64) PRIMARY KEY,
    content_type VARCHAR(64) NOT NULL,
    payload BYTEA NOT NULL,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_memory_raw_sources_content_type ON memory_raw_sources(content_type);
CREATE INDEX idx_memory_raw_sources_created_at ON memory_raw_sources(created_at);

-- Layer 1 — Schema Entries
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

CREATE INDEX idx_memory_schema_kind ON memory_schema_entries(kind);
CREATE INDEX idx_memory_schema_key_name ON memory_schema_entries(key_name);
CREATE INDEX idx_memory_schema_name ON memory_schema_entries(name);
CREATE INDEX idx_memory_schema_extracted_at ON memory_schema_entries(extracted_at);

-- Layer 2 — Summary Entries
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

CREATE INDEX idx_memory_summary_generated_at ON memory_summary_entries(generated_at);
CREATE INDEX idx_memory_summary_embedding_model ON memory_summary_entries(embedding_model);
