-- Migration: 052_change_ledger_events (down)
-- Drops the event half and the three window columns on the dispatch series. The
-- account itself is untouched: nothing in `change_ledger` was added, altered or
-- backfilled by the up file, so rolling this back loses the transitions recorded
-- since it was applied and leaves the account as it would have been without it.
DROP TABLE IF EXISTS change_ledger_event;

-- The three window columns go with it: they were read for the batch bound and
-- recorded on the same rows this migration added the events for, and a downgrade
-- that kept them would leave a series whose reader no longer knows what they are.
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS window_allowance_tokens;
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS window_spent_tokens;
ALTER TABLE change_dispatch_state DROP COLUMN IF EXISTS window_reserve_tokens;
