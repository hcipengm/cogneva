-- Migration: 048_principle_where_red
-- Where a principle says it would go red, carried beside the two gate columns.
-- See the Postgres migration of the same name for why the ledger needs it and why
-- NULL is a value rather than a gap.
--
-- Dialect differences are `change_ledger`'s and `check_kind`'s: no CHECK
-- constraint (MySQL 5.7 has none), and no `IF NOT EXISTS` on `ADD COLUMN` --
-- migrations run once, so the guard would buy nothing and MySQL does not parse it.
-- `TEXT` inherits the table's `utf8mb4` collation, which is what the sentence
-- needs: it is almost always Chinese, and a `latin1` column would store bytes that
-- a reader cannot get back.

ALTER TABLE principle_ledger
    ADD COLUMN where_red TEXT NULL;

ALTER TABLE principle_ledger_declared
    ADD COLUMN where_red TEXT NULL;
