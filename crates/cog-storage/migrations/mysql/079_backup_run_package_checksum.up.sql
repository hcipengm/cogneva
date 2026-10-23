-- Migration: 079_backup_run_package_checksum
-- See the Postgres migration of the same name for what the column answers and
-- why an absent digest is left absent rather than filled in.
--
-- Dialect difference, and only this: MySQL has no `ADD COLUMN IF NOT EXISTS`,
-- so the statement is the plain form the other MySQL migrations use.
ALTER TABLE backup_runs ADD COLUMN package_checksum TEXT NULL;
