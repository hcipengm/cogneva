-- Migration: 064_backup_drill_offsite_repository
-- What the offsite repository itself says, as observed by the host timer.
--
-- The drill already measures the second destination by walking a directory:
-- `offsite_newest_at` is the instant in the newest copied file's name. That
-- reading says a file exists on this machine. It does not say a copy left the
-- machine, and it cannot say anything about the encryption key the repository
-- was opened with.
--
-- The host systemd timer is the only party that talks to the repository, and it
-- now records three instants it reads back from there:
--
--   * `offsite_observed_at` -- when the host last looked. Its presence is what
--     separates "nobody observes this installation" (NULL) from "somebody
--     looked, and the repository held nothing to report" (a value here, with
--     one or both of the repository times NULL). Empty repository and absent
--     observer are different repairs.
--
--   * `offsite_remote_newest_at` -- `restic snapshots`: the newest snapshot the
--     repository reports. A successful upload is not this reading; a repository
--     that accepted bytes but holds no snapshot is exactly the failure a writer
--     cannot see about itself.
--   * `offsite_key_created_at` -- `restic key list`: when the newest key was
--     created. The age of that instant is how long the key has not been
--     rotated; the mtime of an encrypted credential file is not, because
--     re-sealing a file is not rotating a key.
--
-- All three NULL means no observation has ever been taken -- the timer is not
-- installed, or its observation file is not mounted into this job. That must
-- stay distinguishable from "observed and old": one means nobody is measuring,
-- the other means the measurement says the copy stopped. The rules read them as
-- two different shapes.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS offsite_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS offsite_remote_newest_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS offsite_key_created_at TIMESTAMPTZ;
