-- Migration: 046_backup_runs (down)
-- See the Postgres migration of the same name: the record of every run whose
-- package has been pruned, and of every run that never produced one, is gone,
-- and an empty table reads as "this deployment has never backed up".
DROP TABLE IF EXISTS backup_runs;
