-- Migration: 052_change_ledger_events (down)
-- Drops the event half and the three window columns on the dispatch series; the
-- account is untouched.
DROP TABLE IF EXISTS change_ledger_event;

ALTER TABLE change_dispatch_state DROP COLUMN window_allowance_tokens;
ALTER TABLE change_dispatch_state DROP COLUMN window_spent_tokens;
ALTER TABLE change_dispatch_state DROP COLUMN window_reserve_tokens;
