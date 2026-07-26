-- Migration: 001_users
-- Create users table aligned with sf-auth User model

CREATE TABLE IF NOT EXISTS users (
    id BINARY(16) PRIMARY KEY,
    phone VARCHAR(20) UNIQUE,
    email VARCHAR(255) UNIQUE,
    username VARCHAR(64) NOT NULL UNIQUE,
    display_name VARCHAR(128),
    avatar_url VARCHAR(512),
    status ENUM('active', 'inactive', 'disabled', 'locked') NOT NULL DEFAULT 'active',
    user_type ENUM('admin', 'standard', 'guest') NOT NULL DEFAULT 'standard',
    password_hash VARCHAR(255),
    token_quota_daily BIGINT UNSIGNED DEFAULT 0,
    token_used_today BIGINT UNSIGNED DEFAULT 0,
    last_login_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email),
    INDEX idx_phone (phone),
    INDEX idx_status (status),
    INDEX idx_user_type (user_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
