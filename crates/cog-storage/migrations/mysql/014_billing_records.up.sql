-- Migration: 014_billing_records
-- Token consumption billing records (time-series table)

CREATE TABLE IF NOT EXISTS billing_records (
    id BINARY(16) NOT NULL,
    user_id BINARY(16) NOT NULL,
    workspace_id BINARY(16),
    task_id BINARY(16),
    model VARCHAR(64) NOT NULL,
    task_kind VARCHAR(32) NOT NULL DEFAULT 'custom',
    tokens_input BIGINT UNSIGNED NOT NULL DEFAULT 0,
    tokens_output BIGINT UNSIGNED NOT NULL DEFAULT 0,
    cost DECIMAL(18, 8) NOT NULL DEFAULT 0.0,
    cost_before_weight DECIMAL(18, 8) NOT NULL DEFAULT 0.0,
    weight_applied DECIMAL(5, 4) NOT NULL DEFAULT 1.0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at),
    INDEX idx_user_id (user_id, created_at DESC),
    INDEX idx_workspace_id (workspace_id, created_at DESC),
    INDEX idx_model (model, created_at DESC),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
