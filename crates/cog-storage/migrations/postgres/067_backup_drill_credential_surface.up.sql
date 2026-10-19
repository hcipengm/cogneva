-- Migration: 067_backup_drill_credential_surface
-- How many permission violations the drill found on each directory that holds
-- credential-bearing material.
--
-- The backup volume and the offsite copy hold packages of the whole database;
-- the host state directory holds the observation file and whatever the host
-- timer keeps beside it. All three are written by a root process, so a mode that
-- lets any other account read them is not a cosmetic difference -- it is the
-- package contents readable without the key.
--
-- The drill takes these counts because it is the one process that runs as root
-- with all three directories mounted. A host-side timer would see only the one
-- directory it happens to run next to, and the host ledger DaemonSet cannot
-- stat a 0700 directory at all with the uid it runs under.
--
-- NULL per column means the drill was not given that directory. That is an
-- installation shape, not a clean surface: an installation without an offsite
-- copy has nothing to say about the offsite copy's modes, and a zero there
-- would read as "inspected and clean". The rules distinguish the two.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS credential_violations_backup_pvc INTEGER,
    ADD COLUMN IF NOT EXISTS credential_violations_offsite INTEGER,
    ADD COLUMN IF NOT EXISTS credential_violations_state_dir INTEGER;
