-- Migration: 062_backup_drill_artifacts
-- Artifact-level evolution is part of what a package must carry back.
--
-- Until now a drill opened only the PG segment: a package could lose every
-- policy store and every decision_stats file and still be recorded as
-- restorable. These columns make the artifact layer a measurement of its own.
--
-- A unit is one policy store (versions, head, active) or one
-- decision_stats.json (groups, trials). Packages written before the artifact
-- section existed declare zero units, so their rows keep today's verdict:
-- "that format had no such section" is not "the section was empty".
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS artifacts_expected INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS artifacts_restored INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS artifact_readings TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS artifact_errors TEXT[] NOT NULL DEFAULT '{}';

-- The success word has to carry this evidence too. A row cannot say "ok" while
-- a promised store is unread or an error says why it could not be read.
ALTER TABLE backup_drills
    DROP CONSTRAINT IF EXISTS backup_drills_ok_carries_its_evidence;

ALTER TABLE backup_drills
    ADD CONSTRAINT backup_drills_ok_carries_its_evidence
        CHECK (
            outcome <> 'ok' OR (
                package IS NOT NULL
                AND package_written_at IS NOT NULL
                AND tables_expected IS NOT NULL
                AND tables_restored IS NOT NULL
                AND tables_restored = tables_expected
                AND rows_expected IS NOT NULL
                AND rows_restored IS NOT NULL
                AND rows_restored = rows_expected
                AND source_runs_matched IS TRUE
                AND artifacts_restored = artifacts_expected
                AND cardinality(artifact_errors) = 0
            )
        );
