-- Migration: 029_change_ledger_declared (down)
-- Dropping this throws away the intake, not the account. See the Postgres
-- migration of the same name.

DROP TABLE IF EXISTS change_ledger_declared;
