-- Migration: 055_change_dispatch_window_start
-- Records when the pool's quota window began, on the same row that records how
-- much of it was left. See the Postgres file for why the column is nullable:
-- zero is a real instant, and a row written before the column existed is not a
-- measurement of the window.
--
-- No `IF NOT EXISTS` here, as on the other MySQL files: a second run fails on the
-- duplicate column and names it rather than passing for a migration that did
-- nothing.
ALTER TABLE change_dispatch_state
    ADD COLUMN window_start_unix BIGINT NULL;
