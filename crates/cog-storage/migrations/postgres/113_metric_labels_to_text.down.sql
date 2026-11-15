-- 撤回 113：标签集回到 jsonb。
--
-- `upstream` 不拆：它是从标签集里升出来的一列，留着不影响谁读 labels；删掉会
-- 连这条索引一起丢，而名册那一步还要用它。
ALTER TABLE cog_metrics_samples ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metrics_samples ALTER COLUMN labels TYPE JSONB USING labels::jsonb;
ALTER TABLE cog_metrics_samples ALTER COLUMN labels SET DEFAULT '{}'::jsonb;

ALTER TABLE cog_metric_counter_totals ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metric_counter_totals ALTER COLUMN labels TYPE JSONB USING labels::jsonb;
ALTER TABLE cog_metric_counter_totals ALTER COLUMN labels SET DEFAULT '{}'::jsonb;

ALTER TABLE cog_metric_histogram_buckets ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metric_histogram_buckets ALTER COLUMN labels TYPE JSONB USING labels::jsonb;
ALTER TABLE cog_metric_histogram_buckets ALTER COLUMN labels SET DEFAULT '{}'::jsonb;

ALTER TABLE cog_metric_histogram_sums ALTER COLUMN labels DROP DEFAULT;
ALTER TABLE cog_metric_histogram_sums ALTER COLUMN labels TYPE JSONB USING labels::jsonb;
ALTER TABLE cog_metric_histogram_sums ALTER COLUMN labels SET DEFAULT '{}'::jsonb;
