-- Migration: 018_user_devices
-- User registered devices and push tokens

CREATE TABLE IF NOT EXISTS user_devices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id VARCHAR(64) NOT NULL,
    platform VARCHAR(16) NOT NULL CHECK (platform IN ('ios', 'android', 'harmonyos', 'web', 'desktop')),
    device_model VARCHAR(128),
    os_version VARCHAR(32),
    app_version VARCHAR(32),
    push_token TEXT,
    push_provider VARCHAR(16),
    biometric_enabled BOOLEAN NOT NULL DEFAULT false,
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, device_id)
);

CREATE INDEX idx_user_devices_user_id ON user_devices(user_id);
CREATE INDEX idx_user_devices_push ON user_devices(push_provider, push_token) WHERE push_token IS NOT NULL;
