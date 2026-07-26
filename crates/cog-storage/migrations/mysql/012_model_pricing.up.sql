-- Migration: 012_model_pricing
-- LLM model token pricing configuration

CREATE TABLE IF NOT EXISTS model_pricing (
    id BINARY(16) PRIMARY KEY,
    model_id VARCHAR(64) NOT NULL UNIQUE,
    model_name VARCHAR(128) NOT NULL,
    input_price DECIMAL(18, 8) NOT NULL,
    output_price DECIMAL(18, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'CNY',
    effective_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_until DATETIME,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_model_id (model_id, is_active),
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
