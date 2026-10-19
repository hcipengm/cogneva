-- Migration: 065_backup_drill_segment_skew (down)
ALTER TABLE backup_drills
    DROP COLUMN IF EXISTS segment_skew_bound_seconds,
    DROP COLUMN IF EXISTS segment_skew_seconds;
