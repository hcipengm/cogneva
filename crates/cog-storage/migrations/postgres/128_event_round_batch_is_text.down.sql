-- 回到 jsonb 生成列（124 的形状）。
ALTER TABLE cogneva_event DROP COLUMN IF EXISTS round_batch;
ALTER TABLE cogneva_event
    ADD COLUMN IF NOT EXISTS round_batch JSONB GENERATED ALWAYS AS (attributes::jsonb -> 'batch') STORED;
