-- Migration: 006_memory
-- Three-tier memory storage aligned with sf-core memory types

-- Layer 0 — Raw Sources
CREATE TABLE IF NOT EXISTS memory_raw_sources (
    id VARCHAR(64) PRIMARY KEY,
    content_type VARCHAR(64) NOT NULL,
    payload LONGBLOB NOT NULL,
    tags JSON NOT NULL DEFAULT (JSON_ARRAY()),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_content_type (content_type),
    INDEX idx_created_at (created_at),
    INDEX idx_tags ( (CAST(tags AS CHAR(64) ARRAY)) )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Layer 1 — Schema Entries
CREATE TABLE IF NOT EXISTS memory_schema_entries (
    id VARCHAR(64) PRIMARY KEY,
    kind ENUM('entity', 'relation', 'event', 'sentiment', 'custom') NOT NULL,
    name VARCHAR(255) NOT NULL,
    key_name VARCHAR(255) NOT NULL,
    properties JSON NOT NULL,
    source_raw_uri VARCHAR(512) NOT NULL,
    extractor_version VARCHAR(64) NOT NULL,
    confidence FLOAT NOT NULL DEFAULT 1.0,
    extracted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_kind (kind),
    INDEX idx_key_name (key_name),
    INDEX idx_name (name),
    INDEX idx_extracted_at (extracted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Layer 2 — Summary Entries
CREATE TABLE IF NOT EXISTS memory_summary_entries (
    id VARCHAR(64) PRIMARY KEY,
    text TEXT NOT NULL,
    embedding BLOB,
    embedding_model VARCHAR(64) NOT NULL,
    source_raw_uri VARCHAR(512) NOT NULL,
    related_schema_ids JSON NOT NULL DEFAULT (JSON_ARRAY()),
    confidence FLOAT NOT NULL DEFAULT 1.0,
    generated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_generated_at (generated_at),
    INDEX idx_embedding_model (embedding_model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
