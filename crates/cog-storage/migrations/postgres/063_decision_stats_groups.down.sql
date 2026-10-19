-- Reverse 063: the evidence goes back to being a file the engine owns.
-- The rows are dropped rather than moved: a down migration cannot write the
-- file, and leaving a table behind would make the next up migration's
-- `CREATE TABLE IF NOT EXISTS` silently adopt a half-understood schema.
DROP INDEX IF EXISTS idx_decision_stats_group_totals;
DROP TABLE IF EXISTS decision_stats_groups;
