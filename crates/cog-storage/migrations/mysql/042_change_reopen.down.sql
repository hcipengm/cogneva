-- Migration: 042_change_reopen (down)
-- Drops the reopen series. See the Postgres migration of the same name: the rows
-- are observations the account no longer keeps anywhere else, so dropping them
-- loses a history, not a premise.

DROP TABLE IF EXISTS change_reopen;
