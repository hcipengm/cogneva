-- 收件台问「这条意图有没有一条任务带着它的身份」。`input` 是 JSON 文本，按载荷里的
-- 键筛就得逐行 `::jsonb`——真库上 46,745 行要 32 秒（2026-11-24 实测），而且那条路
-- 没有索引。按 124 的规矩：查询面出现的字段升成生成列，由库自己按表达式算。
--
-- 载荷本身（谁说的、说给谁、原话）由读面在 Rust 里解析，不在这里升列：那是每一行
-- 都要原样交出去的内容，不是筛选条件。
ALTER TABLE task_defined
    ADD COLUMN IF NOT EXISTS intent_id TEXT GENERATED ALWAYS AS (
        (CASE WHEN input ~ '^[[:space:]]*[\[{]' THEN input::jsonb END) ->> 'intent_id'
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_task_defined_intent_id
    ON task_defined(intent_id) WHERE intent_id IS NOT NULL;
