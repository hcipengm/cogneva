-- Migration: 088_backup_drill_tables_without_a_home
-- Which tables a restored package carried that the schema no longer has.
--
-- The drill builds its scratch database from the running image's migrations and
-- then loads the package into it. A package written before a migration dropped
-- a table therefore names a table that has no home in the target; its rows can
-- be neither truncated nor copied. Refusing the whole package would let one
-- dropped table invalidate the entire retention window, so the loader skips
-- those tables and reports their names here.
--
-- Not part of the success constraint, for the same reason the recomputed
-- columns are not: what came back is what the verdict is about. But silence is
-- the other failure mode -- with the sweep done and the names unrecorded, the
-- table count and the row count agree with the manifest and the row reads as a
-- complete restore. Empty means every table the package declared still had a
-- home.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS tables_without_a_home TEXT[] NOT NULL DEFAULT '{}';
