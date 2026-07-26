-- Migration: 013_user_quotas
-- User token quota tracking (daily/monthly/total)

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
