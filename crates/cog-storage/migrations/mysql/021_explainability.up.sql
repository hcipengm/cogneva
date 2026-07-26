-- Migration: 021_explainability
-- Explainability data for AI decision tracing (type 15).

CREATE TABLE IF NOT EXISTS explainability (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    agent_id VARCHAR(64),
    decision_type VARCHAR(32),
    confidence FLOAT,
    data JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_explain_lookup (session_id, timestamp DESC, decision_type),
    INDEX idx_explain_time (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
