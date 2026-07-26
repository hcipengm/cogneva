-- Migration: 019_user_biometric_keys
-- Biometric login public keys (private keys stored only on device secure hardware)

CREATE TABLE IF NOT EXISTS user_biometric_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id VARCHAR(64) NOT NULL,
    public_key TEXT NOT NULL,
    key_type VARCHAR(16) NOT NULL DEFAULT 'ecdsa' CHECK (key_type IN ('ecdsa', 'rsa')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    UNIQUE (user_id, device_id)
);

CREATE INDEX idx_user_biometric_keys_user_id ON user_biometric_keys(user_id);
