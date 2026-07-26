-- Migration: 017_audit_logs
-- Operation audit logs (time-series table)

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGINT UNSIGNED AUTO_INCREMENT,
    user_id BINARY(16),
    action VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(64),
    details JSON NOT NULL DEFAULT (JSON_OBJECT()),
    ip_address VARBINARY(16),
    user_agent TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at),
    INDEX idx_user_id (user_id, created_at DESC),
    INDEX idx_action (action, created_at DESC),
    INDEX idx_resource (resource_type, resource_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
