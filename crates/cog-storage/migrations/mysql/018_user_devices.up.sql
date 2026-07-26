-- Migration: 018_user_devices
-- User registered devices and push tokens

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
