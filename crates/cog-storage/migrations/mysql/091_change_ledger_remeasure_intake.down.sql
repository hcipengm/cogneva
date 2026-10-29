-- Migration: 091_change_ledger_remeasure_intake (down)
-- Dropping the intake loses only the requests not yet folded. A row already
-- measured again keeps the measurement on `change_ledger`, and is not touched by
-- this. See the up migration of the same name.

DROP TABLE IF EXISTS change_ledger_remeasure;
