-- Migration: 080_backup_run_package_coverage
-- See the Postgres migration of the same name for what the column answers and
-- why an absent value is left absent rather than filled in.
--
-- Dialect difference, and only this: MySQL has no `ADD COLUMN IF NOT EXISTS`,
-- so the statement is the plain form the other MySQL migrations use. TEXT holds
-- the same JSON the Postgres column does.
ALTER TABLE backup_runs ADD COLUMN package_coverage TEXT NULL;
