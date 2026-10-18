ALTER TABLE backup_drills
    DROP COLUMN IF EXISTS offsite_matches_newest,
    DROP COLUMN IF EXISTS offsite_newest_at;
