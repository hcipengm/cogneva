-- Migration: 049_change_dispatch_unread_sources (down)
-- See the Postgres migration of the same name: the passes stay recorded, and
-- what is lost is the difference between a pass whose intake could not be read
-- and a pass with nothing to record.
ALTER TABLE change_dispatch_state DROP COLUMN sources_unread;
