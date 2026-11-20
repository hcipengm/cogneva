-- 回退：只去掉指针列。大对象本身**不在这条迁移里删**——删除正文要「搬走并校验才删」，
-- 回退一条迁移不是那条路。
DROP INDEX IF EXISTS idx_gateway_llm_exchange_unsealed;
ALTER TABLE gateway_llm_exchange
    DROP COLUMN IF EXISTS request_body_oid,
    DROP COLUMN IF EXISTS request_body_sha256,
    DROP COLUMN IF EXISTS request_body_sealed_at,
    DROP COLUMN IF EXISTS response_body_oid,
    DROP COLUMN IF EXISTS response_body_sha256,
    DROP COLUMN IF EXISTS response_body_sealed_at;
