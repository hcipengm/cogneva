-- 告警的 `source`：从标签集里的一格升成真列之后，老行只有标签集。
--
-- 这一列是镜子收口的判据（「哪些在响的行是我写的」）。它空着，镜子读到的就是空集：
-- 自己的行永远关不掉，别人的行会被它关掉——两种都不报错。所以老行必须回填一次。
--
-- 回填是**行改写**，不是 catalog 能回答的形状，因此它不能住在 store 的启动批次里
-- （那一批的每条语句都得能读成「库已经是这个形状」，否则每次启动都会向一张有写入
-- 的表要 ACCESS EXCLUSIVE）。迁移链里跑一次，正是它该在的地方。
--
-- `alerts` 表由 alert store 首启时建，迁移链里没有它：空库重放时这一步什么也不做。
DO $backfill$
BEGIN
    IF to_regclass('alerts') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name = 'alerts' AND column_name = 'source'
    ) THEN
        RETURN;
    END IF;
    UPDATE alerts
       SET source = labels::jsonb ->> 'source'
     WHERE source IS NULL
       AND labels IS NOT NULL
       AND (labels::jsonb ? 'source');
END
$backfill$;
