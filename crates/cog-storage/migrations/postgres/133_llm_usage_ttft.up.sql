-- 首字时延：第一段内容到达减第一帧到达。可空：非流式或没内容帧就是「没量到」，
-- 不是 0 毫秒。
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS ttft_ms BIGINT;
