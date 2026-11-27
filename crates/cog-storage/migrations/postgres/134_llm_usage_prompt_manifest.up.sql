-- 提示清单：这一次发出去的每段 sha256（JSON 数组）。留底过期之后，靠它回到当时的提示。
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS prompt_manifest TEXT;
