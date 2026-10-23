-- Migration: 079_backup_run_package_checksum (down)
ALTER TABLE backup_runs DROP COLUMN package_checksum;
