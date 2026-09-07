-- Migration: 024_platform_identities
-- Code-platform identities (GitHub/Gitee) linked to local users. Connecting a
-- platform account is how a local account comes into existence. Tokens live
-- only in the secure-gateway Secret; this table keeps references, never
-- plaintext.

CREATE TABLE IF NOT EXISTS platform_identities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider VARCHAR(16) NOT NULL,
    provider_user_id VARCHAR(64) NOT NULL,
    login VARCHAR(128) NOT NULL,
    access_token_ref VARCHAR(255),
    refresh_token_ref VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, provider_user_id)
);

CREATE INDEX IF NOT EXISTS idx_platform_identities_user ON platform_identities(user_id);
