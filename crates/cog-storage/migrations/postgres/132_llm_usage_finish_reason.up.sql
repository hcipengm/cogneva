-- 模型侧为什么停了：平台结果词说的是调用成没成，这一格说的是模型停在哪
-- （stop/length/tool_calls/content_filter…）。可空：上游没报就是「没说」，不是空串。
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS finish_reason TEXT;
