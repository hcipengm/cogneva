-- Reverse 062: put back the constraint that only looked at the PG segment,
-- then drop the artifact columns.
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
            )
        );

ALTER TABLE backup_drills
    DROP COLUMN IF EXISTS artifacts_expected,
    DROP COLUMN IF EXISTS artifacts_restored,
    DROP COLUMN IF EXISTS artifact_readings,
    DROP COLUMN IF EXISTS artifact_errors;
