-- Migration: 093_cog_dag_tasks_venue_index (down)
-- Dropping the index costs the venue board its lookup and nothing else: the
-- board's answer is the query's, not the index's, so this leaves every row and
-- every reading intact, slower.

DROP INDEX IF EXISTS idx_cog_dag_tasks_venue;
