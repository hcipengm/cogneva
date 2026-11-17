-- 事件的载荷：写入期校验，字节不动。
--
-- 载荷存 text 是为了逐字节保留（内容寻址、擦除后证明原字节），但 text 本身不校验：
-- 一段写坏的 JSON 能落库，直到读取那一层才被发现，而那时的读法是「标 undecodable」
-- ——错误被发现得太晚，且丢掉了「写的时候就该拦下」这个更强的判据。
--
-- 两条约束把它补回来，且不触碰字节：`IS JSON` 与「顶层是对象」。PostgreSQL 16 起
-- 有 `IS JSON` 谓词。
--
-- 新库里 117 建表时就带着这两条；这一份给已经在 117 建过表、但还没有约束的库补上。
DO $payload$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'cogneva_event_attributes_is_json'
           AND conrelid = 'cogneva_event'::regclass
    ) THEN
        ALTER TABLE cogneva_event
            ADD CONSTRAINT cogneva_event_attributes_is_json
            CHECK (attributes IS JSON);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'cogneva_event_attributes_is_object'
           AND conrelid = 'cogneva_event'::regclass
    ) THEN
        ALTER TABLE cogneva_event
            ADD CONSTRAINT cogneva_event_attributes_is_object
            CHECK (jsonb_typeof(attributes::jsonb) = 'object');
    END IF;
END
$payload$;
