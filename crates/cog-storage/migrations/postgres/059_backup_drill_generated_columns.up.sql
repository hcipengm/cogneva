-- Migration: 059_backup_drill_generated_columns
-- What a restore let the target compute instead of copying.
--
-- A package written before the export stopped selecting every column carries
-- values for columns the target derives from other columns of the same row
-- (`GENERATED ALWAYS AS ... STORED`). Those values cannot be loaded: the target
-- computes them. The loader therefore stages the package in a table shaped like
-- the target and moves only the writable columns across.
--
-- That is a correct restore, and it is deliberately absent from the success
-- constraint: the row count, the table count and the account comparison all
-- agree whether or not it happened. What those three cannot see is a changed
-- generated expression -- an older package restored under a new expression comes
-- back with values the package never held, and every one of them stays green.
-- The names are the only place that divergence is visible, so the column holds
-- `table.column` for each one, and a drill that recomputed nothing stores an
-- empty array rather than NULL: "none" is a measurement, absence is not.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS generated_columns_recomputed TEXT[] NOT NULL DEFAULT '{}';
