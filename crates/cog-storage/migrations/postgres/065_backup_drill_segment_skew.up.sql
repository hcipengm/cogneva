-- Migration: 065_backup_drill_segment_skew
-- How far apart the package's own segments were, and the bound it declared.
--
-- The drill already refuses a package whose segments are further apart than the
-- package's own `max_segment_skew_seconds`: a restore must not stitch data
-- taken at different moments into one "success". That refusal is a cliff. Until
-- this migration the only visible trace of the distance was a log line on the
-- night the drill failed, so a package drifting toward the bound looked exactly
-- like a healthy one.
--
-- Two columns rather than a ratio, because the bound is declared per package
-- (and can be widened or tightened through the environment at write time).
-- Publishing the skew alone would leave the reader without the number it has to
-- be compared against, and a ratio alone would hide which side moved.
--
-- Both NULL means the package carried no segment table (older format) or no
-- package was opened -- there is no distance to report, and zero would claim
-- there was one and it was perfect.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS segment_skew_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS segment_skew_bound_seconds BIGINT;
