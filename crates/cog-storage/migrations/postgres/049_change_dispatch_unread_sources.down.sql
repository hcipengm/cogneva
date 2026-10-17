-- Migration: 049_change_dispatch_unread_sources (down)
-- Drops the column. What is lost is the account's ability to say that a pass
-- ran and could not read one of its intakes: the passes themselves stay
-- recorded, but a pass whose only source was unread becomes indistinguishable
-- from a pass with nothing to record, which is the state this column was added
-- to end.
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS sources_unread;
