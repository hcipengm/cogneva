-- 载荷里被**筛**的那几个键升成生成列。
--
-- 载荷存 text 是为了逐字节保留；代价是「按载荷里的键筛」写起来要 cast，而那一路
-- 没有索引，且它一出现就说明这个字段已经是查询面了。生成列自带索引、不会与载荷
-- 漂移（库自己按表达式算），查询因此不再解析 JSON。
--
-- 五个键都是 `llm_call` 的用量面：读侧按人/按批/按天算 token 与花费时筛的就是它们。
ALTER TABLE cogneva_event
    ADD COLUMN IF NOT EXISTS input_tokens  BIGINT GENERATED ALWAYS AS ((attributes::jsonb ->> 'input_tokens')::bigint) STORED,
    ADD COLUMN IF NOT EXISTS output_tokens BIGINT GENERATED ALWAYS AS ((attributes::jsonb ->> 'output_tokens')::bigint) STORED,
    ADD COLUMN IF NOT EXISTS cost_micros   BIGINT GENERATED ALWAYS AS ((attributes::jsonb ->> 'cost_micros')::bigint) STORED,
    ADD COLUMN IF NOT EXISTS price_book    TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'price_book') STORED,
    ADD COLUMN IF NOT EXISTS call_result   TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'result') STORED,
    -- 轮次事件读的两格：开工那一刻钉住的台账 rev 与代码 rev。它们也是「按载荷里的键
    -- 取值」的查询面，同样升成列。
    ADD COLUMN IF NOT EXISTS ledger_rev    TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'ledger_rev') STORED,
    ADD COLUMN IF NOT EXISTS code_rev      TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'code_rev') STORED,
    -- 用量读侧按模型面 / 人 / 任务分桶时筛的四格。
    ADD COLUMN IF NOT EXISTS api_style     TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'api_style') STORED,
    ADD COLUMN IF NOT EXISTS upstream_name TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'upstream') STORED,
    ADD COLUMN IF NOT EXISTS owner_name    TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'owner') STORED,
    ADD COLUMN IF NOT EXISTS task_id_text  TEXT   GENERATED ALWAYS AS (attributes::jsonb ->> 'task_id') STORED,
    ADD COLUMN IF NOT EXISTS cached_tokens BIGINT GENERATED ALWAYS AS ((attributes::jsonb ->> 'cached_tokens')::bigint) STORED,
    ADD COLUMN IF NOT EXISTS cache_created_tokens BIGINT GENERATED ALWAYS AS ((attributes::jsonb ->> 'cache_created_tokens')::bigint) STORED,
    -- 工具调用读侧按 `unit_of_work` 列出、再取参数里的路径。
    ADD COLUMN IF NOT EXISTS tool_path TEXT GENERATED ALWAYS AS (attributes::jsonb ->> 'path') STORED,
    -- 这一轮开了哪些条：`round_opened` 把清单放在载荷的 `batch` 键上，读取方按列取。
    -- 原先那条 SELECT 点名的是一个从来不存在的 `batch` 列，每次调用都报「列不存在」。
    ADD COLUMN IF NOT EXISTS round_batch JSONB GENERATED ALWAYS AS (attributes::jsonb -> 'batch') STORED;

CREATE INDEX IF NOT EXISTS idx_cogneva_event_llm_call_at
    ON cogneva_event (at) WHERE kind = 'llm_call';
