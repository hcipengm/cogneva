-- Migration: 094_analyze_after_venue_index
-- The index 093 adds is chosen only when the planner has statistics for the
-- expression it covers, and creating it gathers none.
--
-- Measured on the live database on 2026-11-06, minutes after 093 applied: the
-- venue board's statement still planned as
--
--   Seq Scan on cog_dag_tasks  (cost=0.00..4350.98 rows=9764 width=1090)
--     Filter: ((task ->> 'venue') IS NOT NULL AND workspace_id = 'default'
--              AND btrim(task ->> 'venue') <> '')
--
-- against a table of 9,999 rows of which 5 carry a venue. One `ANALYZE` later,
-- the same statement on the same table:
--
--   Index Scan using idx_cog_dag_tasks_venue  (cost=0.29..16.56 rows=5)
--
-- The board polls that read every 20 seconds, so the difference is not a
-- one-off: it is a lookup against a scan of 42 MB of JSON, twenty times a
-- minute, for as long as the database lives -- and the index is right there
-- being passed over.
--
-- This is the second step of 093 for the same reason it was written at all.
-- 093's own header reasons carefully about what a partial index's statistics
-- would say, and then assumes statistics exist. They do on a database where
-- someone typed `ANALYZE`; on the upgrade path nobody does. A test that folds
-- the same rows can only check the first half -- it runs `ANALYZE` itself,
-- because that is the state it wants to judge -- so the state the upgrade
-- actually leaves behind is what is added here.
--
-- Guarded on the table's existence because this store creates `cog_dag_tasks`
-- in its own schema, which runs *after* the migrations on a fresh install: a
-- failed migration stops the runner and blocks everything numbered after it.
-- On a fresh install there is nothing to analyze yet; the table arrives empty,
-- and the first `ANALYZE` -- the autovacuum's, once rows are there -- gathers
-- exactly these statistics.

DO $$
BEGIN
    IF to_regclass('cog_dag_tasks') IS NULL THEN
        RAISE NOTICE 'cog_dag_tasks does not exist yet; the first ANALYZE gathers these statistics';
        RETURN;
    END IF;
    EXECUTE 'ANALYZE cog_dag_tasks';
END
$$;
