-- Migration: 093_cog_dag_tasks_venue_index (down)
-- Dropping the index costs the venue board its lookup and nothing else: the
-- board's answer is the query's, not the index's, so this leaves every row and
-- every reading intact, slower. Guarded the same way the up is, because the
-- down of an upgrade run against a database this store never built should not
-- be the thing that stops a rollback.

DO $$
BEGIN
    IF to_regclass('cog_dag_tasks') IS NULL THEN
        RETURN;
    END IF;
    EXECUTE 'DROP INDEX IF EXISTS idx_cog_dag_tasks_venue';
END
$$;
