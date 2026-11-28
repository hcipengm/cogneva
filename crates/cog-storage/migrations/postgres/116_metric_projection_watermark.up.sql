-- 指标三张累加表是**投影**，它们的真源是 Prometheus（实测：500 条累加里 469 条对应
-- 的样本已被保留窗口清掉，样本日志重建不出它们）。投影要自证，自证的第一条是水位：
-- 「我读到哪一刻」。
--
-- 写入端在每次 upsert 里写 `as_of = NOW()`；重建命令（`cogneva
-- rebuild-metric-projections`）把整个表按 Prometheus 的当前值重写，并把 `as_of` 写成
-- 那一次查询的时刻。所以这一列回答的是「这一行是哪个时刻的读数」，不是「哪个时刻写的」。
--
-- 三张表都由指标存储自己建，迁移链里没有它们的 CREATE TABLE：空库重放时这一步什么
-- 也不做，新库那边由 store 的 DDL 建出同名列。
ALTER TABLE IF EXISTS cog_metric_counter_totals ADD COLUMN IF NOT EXISTS as_of TIMESTAMPTZ;
ALTER TABLE IF EXISTS cog_metric_histogram_buckets ADD COLUMN IF NOT EXISTS as_of TIMESTAMPTZ;
ALTER TABLE IF EXISTS cog_metric_histogram_sums ADD COLUMN IF NOT EXISTS as_of TIMESTAMPTZ;

DO $mig$
BEGIN
    IF to_regclass('cog_metric_counter_totals') IS NOT NULL THEN
        UPDATE cog_metric_counter_totals SET as_of = NOW() WHERE as_of IS NULL;
    END IF;
    IF to_regclass('cog_metric_histogram_buckets') IS NOT NULL THEN
        UPDATE cog_metric_histogram_buckets SET as_of = NOW() WHERE as_of IS NULL;
    END IF;
    IF to_regclass('cog_metric_histogram_sums') IS NOT NULL THEN
        UPDATE cog_metric_histogram_sums SET as_of = NOW() WHERE as_of IS NULL;
    END IF;
END
$mig$;
