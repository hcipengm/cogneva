-- Migration: 079_backup_run_package_checksum (down)
-- The digests cannot be reconstructed from anything else, and nothing else reads
-- them, so dropping the column loses only the answers it was added to carry.
ALTER TABLE backup_runs DROP COLUMN IF EXISTS package_checksum;
