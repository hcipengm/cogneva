-- Migration: 059_backup_drill_generated_columns (down)
-- Drops the list of columns a restore let the target recompute. Nothing else
-- reads it to decide anything, so dropping it loses that reading and the record
-- of which packages came back partly computed.
ALTER TABLE backup_drills DROP COLUMN IF EXISTS generated_columns_recomputed;
