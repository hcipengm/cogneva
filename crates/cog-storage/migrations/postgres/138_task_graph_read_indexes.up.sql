-- 任务图的两条读数要一条覆盖索引，让它们不随历史（迁移条数）变慢。
--
-- 实测（2026-11-26，现网库：task_transition 36,014 行、task_defined 46,454 行、
-- 1,452 个 default 工作区任务）：
--   * 全库状态计数 `DISTINCT ON (task_id) … ORDER BY task_id, seq DESC`：
--     78.7ms，计划是 Seq Scan + Sort（全量排序）。
--   * 工作区状态计数 `DISTINCT ON (task_id) … WHERE workspace_id=$1 ORDER BY task_id, rev DESC`
--     + 每任务取最后一条迁移：136.2ms，计划里 46k 行的排序落到外部归并（2.4MB）。
-- 同一份行放进临时表、只加下面两条索引之后：16.8ms 与 32.4ms，两条都变成
-- Index Only Scan、没有 Sort。按历史涨十倍推算约 0.17–0.32s，仍在一秒之内；
-- 而排序方案在十倍时外部归并到 ~25MB、越过 1s。
--
-- 索引的列序不是随手写的：它必须与查询的 ORDER BY 逐列一致（前导列对得上），
-- 否则规划器仍然要排序。门在
-- crates/cogneva/tests/the_task_graph_reads_use_indexes_that_do_not_grow_with_history_test.rs。
CREATE INDEX IF NOT EXISTS idx_task_defined_workspace_task_rev
    ON task_defined (workspace_id, task_id, rev DESC);

CREATE INDEX IF NOT EXISTS idx_task_transition_task_seq_state
    ON task_transition (task_id, seq DESC) INCLUDE (to_state);
