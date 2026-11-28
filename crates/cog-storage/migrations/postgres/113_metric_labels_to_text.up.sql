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
--
-- 四张表都是**指标存储自己**在建表时创建的，迁移链里没有它们的 `CREATE TABLE`；
-- 恢复演练先跑迁移、再建 store 表，所以每一条都要容忍「表还没建」：`IF EXISTS`
-- 对 ALTER 就够，回填与索引包在 `to_regclass` 的守卫里。表不在时这一步什么也不做
-- ——新库那边由 store 的 DDL 直接建成 text，没有旧行要搬。
ALTER TABLE IF EXISTS cog_metrics_samples ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE IF EXISTS cog_metrics_samples ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE IF EXISTS cog_metrics_samples ALTER COLUMN labels SET DEFAULT '{}';

ALTER TABLE IF EXISTS cog_metric_counter_totals ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE IF EXISTS cog_metric_counter_totals ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE IF EXISTS cog_metric_counter_totals ALTER COLUMN labels SET DEFAULT '{}';

ALTER TABLE IF EXISTS cog_metric_histogram_buckets ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE IF EXISTS cog_metric_histogram_buckets ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE IF EXISTS cog_metric_histogram_buckets ALTER COLUMN labels SET DEFAULT '{}';

ALTER TABLE IF EXISTS cog_metric_histogram_sums ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE IF EXISTS cog_metric_histogram_sums ALTER COLUMN labels TYPE TEXT USING labels::text;
ALTER TABLE IF EXISTS cog_metric_histogram_sums ALTER COLUMN labels SET DEFAULT '{}';

-- 名册读的那个键升成真列。名册那一步问的是「这一行的上游在不在新鲜的名单上」，
-- 要的是这一个键的值，不是整份标签集——而它对四张表问同一个问题，所以四张表
-- 都带这一列。写入端与标签集在同一条语句里写出来，文档里那一格照旧保留。
ALTER TABLE IF EXISTS cog_metrics_samples ADD COLUMN IF NOT EXISTS upstream TEXT;
ALTER TABLE IF EXISTS cog_metric_counter_totals ADD COLUMN IF NOT EXISTS upstream TEXT;
ALTER TABLE IF EXISTS cog_metric_histogram_buckets ADD COLUMN IF NOT EXISTS upstream TEXT;
ALTER TABLE IF EXISTS cog_metric_histogram_sums ADD COLUMN IF NOT EXISTS upstream TEXT;

DO $mig$
BEGIN
    IF to_regclass('cog_metrics_samples') IS NOT NULL THEN
        UPDATE cog_metrics_samples
           SET upstream = labels::jsonb ->> 'upstream'
         WHERE upstream IS NULL;
        CREATE INDEX IF NOT EXISTS idx_cog_metrics_upstream
            ON cog_metrics_samples(name, upstream, timestamp)
            WHERE upstream IS NOT NULL;
    END IF;
    IF to_regclass('cog_metric_counter_totals') IS NOT NULL THEN
        UPDATE cog_metric_counter_totals
           SET upstream = labels::jsonb ->> 'upstream'
         WHERE upstream IS NULL;
    END IF;
    IF to_regclass('cog_metric_histogram_buckets') IS NOT NULL THEN
        UPDATE cog_metric_histogram_buckets
           SET upstream = labels::jsonb ->> 'upstream'
         WHERE upstream IS NULL;
    END IF;
    IF to_regclass('cog_metric_histogram_sums') IS NOT NULL THEN
        UPDATE cog_metric_histogram_sums
           SET upstream = labels::jsonb ->> 'upstream'
         WHERE upstream IS NULL;
    END IF;
END
$mig$;
