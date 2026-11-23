-- 收件台读的那几格升成生成列。
--
-- `intent_received` 这条事件把「谁说的、说的是哪一句、回哪一条」写在载荷里，
-- 而收件台的读法是「按 `kind` 筛、把这几格取出来」——筛选与取值都落在载荷上，
-- 就是一条没有索引、每次都要解析 JSON 的查询（真库上 46,745 行定义逐行转换
-- 要 32 秒，2026-11-24 实测）。按 124 的同一条规矩：查询面出现的字段升成列，
-- 由库自己按表达式算，不与载荷漂移。
ALTER TABLE cogneva_event
    ADD COLUMN IF NOT EXISTS intent_source      TEXT GENERATED ALWAYS AS (attributes::jsonb ->> 'source') STORED,
    ADD COLUMN IF NOT EXISTS intent_external_id TEXT GENERATED ALWAYS AS (attributes::jsonb ->> 'external_id') STORED,
    ADD COLUMN IF NOT EXISTS intent_text        TEXT GENERATED ALWAYS AS (attributes::jsonb ->> 'text') STORED,
    ADD COLUMN IF NOT EXISTS intent_parent      TEXT GENERATED ALWAYS AS (attributes::jsonb ->> 'parent') STORED;

CREATE INDEX IF NOT EXISTS idx_cogneva_event_intent_at
    ON cogneva_event (at) WHERE kind = 'intent_received';

-- 派生了它的那条任务：收件台反过来问「这条意图有没有一条任务带着它的身份」。
-- `input` 是 JSON 文本；先认形状再转换，一行历史上不是 JSON 的输入不该让整张表
-- 在加列那一刻失败。
ALTER TABLE task_defined
    ADD COLUMN IF NOT EXISTS intent_id TEXT GENERATED ALWAYS AS (
        (CASE WHEN input ~ '^[[:space:]]*[\[{]' THEN input::jsonb END) ->> 'intent_id'
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_task_defined_intent_id
    ON task_defined(intent_id) WHERE intent_id IS NOT NULL;
