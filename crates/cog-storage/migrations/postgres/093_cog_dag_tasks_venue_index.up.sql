-- Migration: 093_cog_dag_tasks_venue_index
-- Which rows the venue board reads, made findable without scanning the table.
--
-- A venue is a label on a task row (`task->>'venue'`), and the board answers
-- three numbers per venue by folding the rows that carry one. Every row that
-- does not carry one is read only to be dropped, and a workspace that has been
-- running for a while is almost entirely such rows: measured on 2026-11-06,
-- 9,987 of 9,991 rows carried no label, the column's payload was 42 MB, and
-- the scan that found the other four took 0.9 s -- on a board the workbench
-- polls every 20 seconds.
--
-- Not partial, and that is a measurement rather than a preference. The obvious
-- shape -- index only the labeled rows, so the index grows with the venues'
-- work instead of with the task graph -- was tried first and the planner never
-- chose it: `ANALYZE` gathers an expression's statistics from the rows the
-- index covers, so a partial index's null fraction is zero, the planner reads
-- that as "effectively every row has a label" (its estimate was 9,891 of
-- 9,991), and a scan of the whole table beats an index scan that would visit
-- that many rows. With the predicate dropped the sample spans the table, the
-- null fraction is real (49 rows), and the same statement plans as an index
-- scan returning four rows in 0.13 ms instead of 106 ms.
--
-- The expression is spelled exactly as `dag_get_venue_tasks` spells it, because
-- a functional index is used only when the query's expression matches it; a
-- reader that trims differently gets a correct answer and a sequential scan --
-- slower, never wrong.
--
-- Guarded on the table's existence because this store creates `cog_dag_tasks`
-- in its own schema, which runs *after* the migrations on a fresh install. An
-- unguarded CREATE INDEX would fail there, and a failed migration is not
-- skipped and retried later: the runner stops at it, so every migration after
-- this one would be blocked too. The same index is declared beside that CREATE
-- so a fresh install still gets it; on an existing database this statement is
-- what adds it.

DO $$
BEGIN
    IF to_regclass('cog_dag_tasks') IS NULL THEN
        RAISE NOTICE 'cog_dag_tasks does not exist yet; its schema declares the venue index';
        RETURN;
    END IF;
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_cog_dag_tasks_venue
                 ON cog_dag_tasks (workspace_id, (task ->> ''venue''))';
END
$$;
