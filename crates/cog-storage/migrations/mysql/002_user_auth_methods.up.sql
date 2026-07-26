-- Migration: 002_user_auth_methods
-- External authentication methods bound to users

CREATE TABLE IF NOT EXISTS user_auth_methods (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    auth_type ENUM('phone', 'wechat', 'enterprise_wechat', 'ldap', 'email') NOT NULL,
    auth_id VARCHAR(255) NOT NULL,
    extra JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_auth_type_id (auth_type, auth_id),
    INDEX idx_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
