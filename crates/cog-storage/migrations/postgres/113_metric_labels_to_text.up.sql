-- Migration: 113_metric_labels_to_text
--
-- 指标标签集是指纹，不是文档：它只被比较是否相等（`ON CONFLICT (name, labels)`、
-- `labels =`、`labels >` 逐序列走一遍）和排序，从来没有哪一条 SQL 按标签名索引
-- 进去过——除了名册那一步，而它读的永远是同一个键（`upstream`）。
--
-- 所以：`labels` 转 text，按那一个键读的东西升成一列。
--
-- 转 text 之后存的是什么拼写，是这张表能不能活的问题：标签集是行的身份，两份
-- 对同一个序列给出不同拼写的写法会让 upsert 把它拆成两行、让计数各分一半。写入端
-- 因此不再自己拼 JSON，而是走 `t.labels::jsonb::text`——服务端把客户端送来的
-- 紧凑 JSON 规整成 PostgreSQL 自己的规范拼写，与这里 `labels::text` 对历史行写出
-- 的拼写逐字节相同。拼写在库里只由一处决定。
ALTER TABLE cog_metrics_samples ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metrics_samples ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE cog_metrics_samples ALTER COLUMN labels SET DEFAULT '{}';

ALTER TABLE cog_metric_counter_totals ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metric_counter_totals ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE cog_metric_counter_totals ALTER COLUMN labels SET DEFAULT '{}';

ALTER TABLE cog_metric_histogram_buckets ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metric_histogram_buckets ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE cog_metric_histogram_buckets ALTER COLUMN labels SET DEFAULT '{}';

ALTER TABLE cog_metric_histogram_sums ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metric_histogram_sums ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE cog_metric_histogram_sums ALTER COLUMN labels SET DEFAULT '{}';

-- 名册读的那个键升成真列。名册那一步问的是「这一行的上游在不在新鲜的名单上」，
-- 要的是这一个键的值，不是整份标签集——而它对四张表问同一个问题，所以四张表
-- 都带这一列。写入端与标签集在同一条语句里写出来，文档里那一格照旧保留。
ALTER TABLE cog_metrics_samples ADD COLUMN IF NOT EXISTS upstream TEXT;
ALTER TABLE cog_metric_counter_totals ADD COLUMN IF NOT EXISTS upstream TEXT;
ALTER TABLE cog_metric_histogram_buckets ADD COLUMN IF NOT EXISTS upstream TEXT;
ALTER TABLE cog_metric_histogram_sums ADD COLUMN IF NOT EXISTS upstream TEXT;
UPDATE cog_metrics_samples
   SET upstream = labels::jsonb ->> 'upstream'
 WHERE upstream IS NULL;
UPDATE cog_metric_counter_totals
   SET upstream = labels::jsonb ->> 'upstream'
 WHERE upstream IS NULL;
UPDATE cog_metric_histogram_buckets
   SET upstream = labels::jsonb ->> 'upstream'
 WHERE upstream IS NULL;
UPDATE cog_metric_histogram_sums
   SET upstream = labels::jsonb ->> 'upstream'
 WHERE upstream IS NULL;
CREATE INDEX IF NOT EXISTS idx_cog_metrics_upstream
    ON cog_metrics_samples(name, upstream, timestamp)
    WHERE upstream IS NOT NULL;
