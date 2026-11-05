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
-- Partial, because the predicate is part of the rule: the board only ever asks
-- for labeled rows, so rows without a label earn no entry. The index
-- therefore grows with the venues' work, not with the task graph -- the graph
-- accumulates history forever, the four rows it hid behind did not.
--
-- The expression and the predicate are spelled exactly as `dag_get_venue_tasks`
-- spells them in its WHERE clause. A functional index is used only when the
-- query's expression matches it, so a reader that trims differently (or that
-- drops the empty-label clause) gets a correct answer and a sequential scan --
-- slower, never wrong. `btrim` of the empty string is the empty string, which
-- is why the same `<> ''` excludes both an empty label and a whitespace one.

CREATE INDEX IF NOT EXISTS idx_cog_dag_tasks_venue
    ON cog_dag_tasks (workspace_id, (task ->> 'venue'))
    WHERE task ->> 'venue' IS NOT NULL AND btrim(task ->> 'venue') <> '';
