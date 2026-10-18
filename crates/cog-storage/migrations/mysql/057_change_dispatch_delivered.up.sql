-- Migration: 057_change_dispatch_delivered
-- How many owed changes a commit on the line claims to have landed. See the
-- Postgres file for why this is an annotation over the outcomes rather than one
-- of them, and why NULL (unread) is not zero (nothing delivered).
--
-- No `IF NOT EXISTS`, as on the other MySQL files.
ALTER TABLE change_dispatch_state
    ADD COLUMN delivered BIGINT NULL;
