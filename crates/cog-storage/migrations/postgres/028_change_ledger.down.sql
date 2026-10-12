-- Migration: 028_change_ledger (down)
-- Dropping this table discards the account itself, not a cache of it: the
-- repository copy is a projection rendered from these rows, so after this the
-- checked-in views keep describing a state nothing maintains. There is no way
-- back to the rows -- first_seen_at in particular is not recoverable from any
-- other surface.

DROP TABLE IF EXISTS change_ledger;
