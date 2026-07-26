-- Migration: 002_user_auth_methods
-- External authentication methods bound to users

CREATE TABLE IF NOT EXISTS user_auth_methods (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    auth_type VARCHAR(32) NOT NULL CHECK (auth_type IN ('phone', 'wechat', 'enterprise_wechat', 'ldap', 'email')),
    auth_id VARCHAR(255) NOT NULL,
    extra JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (auth_type, auth_id)
);

CREATE INDEX idx_user_auth_methods_user_id ON user_auth_methods(user_id);
