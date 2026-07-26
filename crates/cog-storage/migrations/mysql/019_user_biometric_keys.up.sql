-- Migration: 019_user_biometric_keys
-- Biometric login public keys (private keys stored only on device secure hardware)

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
