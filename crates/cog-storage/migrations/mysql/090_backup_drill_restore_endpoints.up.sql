-- Migration: 090_backup_drill_restore_endpoints
-- See the Postgres migration of the same name for what the columns answer and
-- why a row written before them is left NULL rather than filled in.
--
-- Dialect difference, and only this: MySQL has no `ADD COLUMN IF NOT EXISTS`,
-- so the statement is the plain form the other MySQL migrations use.
ALTER TABLE backup_drills ADD COLUMN restore_source TEXT NULL;
ALTER TABLE backup_drills ADD COLUMN restore_target TEXT NULL;
