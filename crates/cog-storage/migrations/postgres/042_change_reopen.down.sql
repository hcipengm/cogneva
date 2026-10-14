-- Migration: 042_change_reopen (down)
-- Drops the reopen series.
--
-- Lossy for the reason it exists: the rows it holds are observations of moments
-- the account itself no longer keeps -- each reopen is the record of a claim
-- that was dropped -- and nothing else wrote them down. `change_ledger` does not
-- derive from this table, so no premise is lost with it; what a reader loses is
-- the history of the account being wrong, which cannot be rebuilt by re-running
-- anything.

DROP TABLE IF EXISTS change_reopen;
