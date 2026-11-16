-- 库只存列：开集载荷存 text，要按某个键查就给它加列。
-- 这里两件事一起做：`source` 从 labels 里升成真列（它有一条按值过滤的查询），
-- 其余部分不再需要可查询，转 text。
--
-- 整段包在 `to_regclass` 的守卫里：`alerts` 是**告警存储自己**在建表时创建的，
-- 迁移链里没有它的 `CREATE TABLE`。恢复演练先跑迁移、再建 store 表——没有这个
-- 守卫，那一步会在一张还没建的图上失败，而失败会被读成「包不可恢复」。
-- 表不在时这一段什么也不做：新库那边由 store 的 DDL 直接建成 text。
DO $mig$
BEGIN
    IF to_regclass('alerts') IS NULL THEN
        RETURN;
    END IF;
    ALTER TABLE alerts ADD COLUMN IF NOT EXISTS source TEXT;
    UPDATE alerts SET source = labels ->> 'source' WHERE source IS NULL;
    ALTER TABLE alerts ALTER COLUMN labels TYPE TEXT USING labels::text;
    CREATE INDEX IF NOT EXISTS idx_alerts_source_firing ON alerts(source) WHERE state = 'firing';
END
$mig$;
