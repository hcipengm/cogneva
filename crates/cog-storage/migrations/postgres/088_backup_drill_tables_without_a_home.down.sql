-- Migration: 088_backup_drill_tables_without_a_home (down)
-- Drops the list of package tables the schema had already dropped. Nothing
-- reads it to decide anything, so dropping it loses the record of which
-- packages came back without some of the tables they carried.
ALTER TABLE backup_drills DROP COLUMN IF EXISTS tables_without_a_home;
