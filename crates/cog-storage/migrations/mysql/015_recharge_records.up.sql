-- Migration: 015_recharge_records
-- Token quota recharge records

CREATE TABLE IF NOT EXISTS recharge_records (
    id BINARY(16) PRIMARY KEY,
    target_type ENUM('user', 'workspace') NOT NULL,
    target_id BINARY(16) NOT NULL,
    amount DECIMAL(18, 8) NOT NULL,
    tokens_added BIGINT UNSIGNED NOT NULL,
    valid_until DATETIME,
    operator_id BINARY(16) NOT NULL,
    remark TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_target (target_type, target_id, created_at DESC),
    INDEX idx_operator (operator_id),
    FOREIGN KEY (operator_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
