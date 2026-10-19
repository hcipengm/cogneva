-- Migration: 066_backup_drill_offsite_missing
-- How much of the retention window the second copy does not cover.
--
-- The offsite column answers one question: is the newest package on the volume
-- also the newest one off it. That is the right question for "the copy stopped
-- following", and it is silent about the packages that were never copied at
-- all. An installation that enabled the copy yesterday has a fresh copy and a
-- green column while the rest of its window exists in exactly one place.
--
-- The drill walks both directories, so it is the one reader that can count the
-- volume packages whose name is absent from the offsite directory. NULL means
-- the drill was given no offsite directory at all: the question does not apply,
-- and the rule stays silent rather than reading a fabricated zero. Zero means
-- the window is fully covered.
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS offsite_missing_packages INTEGER;
