-- Migration: 073_principle_priority
-- How urgent the memory that states a principle said landing it is.
-- See the Postgres migration of the same name for why the row's column is a
-- member of the closed set while the intake's is nullable, and why neither is a
-- default band.
--
-- Dialect differences are `change_ledger`'s and `check_kind`'s: no CHECK
-- constraint (MySQL 5.7 has none) and no `IF NOT EXISTS` on `ADD COLUMN` --
-- migrations run once, so the guard would buy nothing and MySQL does not parse
-- it. The store writes only names the contract's enum produced, so the rule the
-- CHECK states on Postgres is held here by the one writer that exists.

ALTER TABLE principle_ledger
    ADD COLUMN priority VARCHAR(16) NOT NULL DEFAULT 'unstated';

ALTER TABLE principle_ledger_declared
    ADD COLUMN priority VARCHAR(16) NULL;
