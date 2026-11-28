-- 这一份提示在装配时被挤掉了多少条更早的消息；null 是「没量到/没裁」。
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS prompt_dropped BIGINT;
