-- Migration: 028_change_ledger (down)
-- Dropping this table discards the account itself, not a cache of it. See the
-- Postgres migration of the same name.

DROP TABLE IF EXISTS change_ledger;
