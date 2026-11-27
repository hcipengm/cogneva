-- Migration: 139_metric_labels
-- 指标标签集里被按键读的键，落成一张能按 (name, labels, key) 查的侧表。
--
-- 标签是开集：`endpoint`、`status`、`upstream`、`job`……一列一个键不成立。而读者
-- 按某个键筛行时（第 10 环要 `endpoint` 与 `status`）此前每拍把 `labels` 文本解析
-- 一遍——键名打错只会读成 NULL，不会报错，读数就把「没有那一档」印成 0。
--
-- 写者（`metrics_backend::write_batch`）从这一刻起在写累加行的同一事务里把每个
-- 序列的标签拆成行；`labels` 与累加行是同一份 `::jsonb::text` 字节，所以读者按
-- `(name, labels, key)` 联结是逐字相等，不重新规整。这一条迁移把存量拆一次。
CREATE TABLE IF NOT EXISTS metric_labels (
    name TEXT NOT NULL,
    labels TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (name, labels, key)
);

INSERT INTO metric_labels (name, labels, key, value)
SELECT t.name, t.labels, e.key, e.value
  FROM (SELECT DISTINCT name, labels FROM cog_metric_counter_totals
        UNION
        SELECT DISTINCT name, labels FROM cog_metric_histogram_buckets
        UNION
        SELECT DISTINCT name, labels FROM cog_metric_histogram_sums) t,
       LATERAL jsonb_each_text(t.labels::jsonb) e
 WHERE t.labels ~ '^[[:space:]]*\{'
ON CONFLICT DO NOTHING;
