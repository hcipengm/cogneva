-- Migration: 054_backup_drill_offsite
-- The drill's second measurement: what lives outside the backup volume.
--
-- The account and the drill both describe the backup volume. A package on that
-- volume protects against a bad deploy, a bad restore, a shortened table; it
-- does not protect against the volume itself going away. `cogneva-backup-pvc`
-- is a local-path claim, so deleting it, losing it or reinstalling under
-- another name takes every package on it at once. The writer copies the newest
-- package to a path outside that volume for exactly that case, and the drill --
-- which already walks the volume rather than trusting the writer's count --
-- walks the second destination too.
--
-- Two columns, both about the copy and neither part of the ok verdict:
--
--   * `offsite_newest_at` is the instant in the newest copied package's name.
--     NULL means nothing has ever been written there, which is a fact about the
--     installation, not about the package being restored.
--   * `offsite_matches_newest` says whether that copy is the very file the
--     drill restored: same name, same byte count. A copy one night behind has
--     an age and answers false here; a copy written and interrupted answers
--     false with the same name.
--
-- They are kept out of the success constraint on purpose. A package that
-- restores is restorable whether or not a second copy exists, and folding the
-- two into one verdict would make a broken copy read as a broken backup -- the
-- two failures have different repairs and different rules.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS offsite_newest_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS offsite_matches_newest BOOLEAN;
