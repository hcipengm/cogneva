-- 载荷是 text；按其中的键筛时升成**真列**，类型跟着那个键的真实类型走。
--
-- `round_batch` 是这张表上最后一个 jsonb 生成列。同一份迁移（124）里其余生成列
-- 都是 text / bigint，只有它把载荷里的数组整份留成 jsonb，于是「全库没有 jsonb
-- 列」这条判据在这里多了一个例外，而例外是不会有第二个人来收的。
--
-- 生成列不能直接 ALTER TYPE，只能按同一表达式删了重加：表达式从 `->` 改成 `->>`，
-- 列里存的是那段 JSON 的文本，读侧按 `Vec<String>` 解析（旧的双重编码形状仍读得懂）。
ALTER TABLE cogneva_event DROP COLUMN IF EXISTS round_batch;
ALTER TABLE cogneva_event
    ADD COLUMN IF NOT EXISTS round_batch TEXT GENERATED ALWAYS AS (attributes::jsonb ->> 'batch') STORED;
