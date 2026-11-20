-- Migration: 123_exchange_body_objects
--
-- 正文的指针：一次调用的请求与响应各可能是一段大对象。
--
-- 为什么要指针而不是把正文塞进行里：正文是**流**进来的，逐块到达；行要等流结束才写得
-- 出来。逐块写进大对象、行里只留 oid 与封口读数，是同一个落点（PG）上的两种形状：
-- 非流式的短正文仍可写在行内列，流式的长正文写在大对象里。
--
-- 三列一组，缺一列那句话就不成立：
--   *_body_oid      —— 哪一段大对象；
--   *_body_sha256   —— 封口时算出的哈希（与上游字节相等才是「无损」）；
--   *_body_sealed_at—— 什么时候封的口。**没有它 = 没封口 = 未写完**，
--                      这条读数把「崩溃留下的半段」与「完整的正文」分开。
ALTER TABLE gateway_llm_exchange
    ADD COLUMN IF NOT EXISTS request_body_oid   OID,
    ADD COLUMN IF NOT EXISTS request_body_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS request_body_sealed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS response_body_oid  OID,
    ADD COLUMN IF NOT EXISTS response_body_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS response_body_sealed_at TIMESTAMPTZ;

-- 未封口的行是运维要看的：它们在账上存在，却说不清那次调用收到了什么。
CREATE INDEX IF NOT EXISTS idx_gateway_llm_exchange_unsealed
    ON gateway_llm_exchange (ts)
    WHERE request_body_sealed_at IS NULL OR response_body_sealed_at IS NULL;
