-- Migration: 064_backup_drill_offsite_repository (down)
ALTER TABLE backup_drills
    DROP COLUMN IF EXISTS offsite_key_created_at,
    DROP COLUMN IF EXISTS offsite_remote_newest_at,
    DROP COLUMN IF EXISTS offsite_observed_at;
