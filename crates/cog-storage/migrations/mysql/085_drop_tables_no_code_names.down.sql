-- Migration: 085_drop_tables_no_code_names (down)
-- Restores the nine shapes verbatim from the migrations that created them.

CREATE TABLE IF NOT EXISTS agent_performance (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    agent_id VARCHAR(64) NOT NULL,
    workspace_id VARCHAR(64),
    role VARCHAR(32) NOT NULL,
    tasks_completed INT UNSIGNED DEFAULT 0,
    tasks_failed INT UNSIGNED DEFAULT 0,
    avg_task_duration_ms INT UNSIGNED DEFAULT 0,
    token_consumed BIGINT UNSIGNED DEFAULT 0,
    score_avg FLOAT DEFAULT 0,
    recorded_at DATE NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_agent_workspace_date (agent_id, workspace_id, recorded_at),
    INDEX idx_agent_id (agent_id),
    INDEX idx_workspace_id (workspace_id),
    INDEX idx_recorded_at (recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS model_pricing (
    id BINARY(16) PRIMARY KEY,
    model_id VARCHAR(64) NOT NULL UNIQUE,
    model_name VARCHAR(128) NOT NULL,
    input_price DECIMAL(18, 8) NOT NULL,
    output_price DECIMAL(18, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'CNY',
    effective_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_until DATETIME,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_model_id (model_id, is_active),
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_quotas (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    quota_type ENUM('daily', 'monthly', 'total') NOT NULL DEFAULT 'daily',
    total_tokens BIGINT UNSIGNED NOT NULL DEFAULT 0,
    used_tokens BIGINT UNSIGNED NOT NULL DEFAULT 0,
    reset_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_quota_type (user_id, quota_type),
    INDEX idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_devices (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    platform ENUM('ios', 'android', 'harmonyos', 'web', 'desktop') NOT NULL,
    device_model VARCHAR(128),
    os_version VARCHAR(32),
    app_version VARCHAR(32),
    push_token TEXT,
    push_provider VARCHAR(16),
    biometric_enabled BOOLEAN NOT NULL DEFAULT false,
    last_active_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_device (user_id, device_id),
    INDEX idx_user_id (user_id),
    INDEX idx_push (push_provider, push_token),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_biometric_keys (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    public_key TEXT NOT NULL,
    key_type ENUM('ecdsa', 'rsa') NOT NULL DEFAULT 'ecdsa',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at DATETIME,
    UNIQUE KEY uk_user_device (user_id, device_id),
    INDEX idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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

CREATE TABLE IF NOT EXISTS principle_debt (
    id           BIGINT NOT NULL AUTO_INCREMENT,
    observed_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    by_kind      TEXT NOT NULL,
    submitted    BIGINT NOT NULL DEFAULT 0,
    already      BIGINT NOT NULL DEFAULT 0,
    failed       BIGINT NOT NULL DEFAULT 0,
    over_bound   BIGINT NOT NULL DEFAULT 0,
    bound        BIGINT NOT NULL,
    bound_source VARCHAR(64) NOT NULL,
    rev          VARCHAR(64) NULL,
    delta        BIGINT NULL,
    PRIMARY KEY (id),
    INDEX idx_principle_debt_observed_at (observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
