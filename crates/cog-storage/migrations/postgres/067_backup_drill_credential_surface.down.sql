ALTER TABLE backup_drills
    DROP COLUMN IF EXISTS credential_violations_backup_pvc,
    DROP COLUMN IF EXISTS credential_violations_offsite,
    DROP COLUMN IF EXISTS credential_violations_state_dir;
