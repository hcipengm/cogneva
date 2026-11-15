-- 库只存列：开集载荷存 text，要按某个键查就给它加列。
-- 这里两件事一起做：`source` 从 labels 里升成真列（它有一条按值过滤的查询），
-- 其余部分不再需要可查询，转 text。
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS source TEXT;
UPDATE alerts SET source = labels->>'source' WHERE source IS NULL;
ALTER TABLE alerts ALTER COLUMN labels TYPE TEXT USING labels::text;
CREATE INDEX IF NOT EXISTS idx_alerts_source_firing ON alerts(source) WHERE state = 'firing';
