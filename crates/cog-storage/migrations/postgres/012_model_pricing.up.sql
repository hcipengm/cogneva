-- Migration: 012_model_pricing
-- LLM model token pricing configuration

CREATE TABLE IF NOT EXISTS model_pricing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_id VARCHAR(64) NOT NULL UNIQUE,
    model_name VARCHAR(128) NOT NULL,
    input_price NUMERIC(18, 8) NOT NULL,
    output_price NUMERIC(18, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'CNY',
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_until TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_model_pricing_model_id ON model_pricing(model_id, is_active);
CREATE INDEX idx_model_pricing_active ON model_pricing(is_active) WHERE effective_until IS NULL;
