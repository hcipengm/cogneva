-- Migration: 038_change_ledger_retired
-- A second terminal state, beside `landed`, for a row whose identity turned out
-- not to be one the account should hold. See the Postgres migration of the same
-- name for why the exit is a state rather than a delete, and why it is
-- re-examinable.
--
-- Dialect differences are the same as `change_ledger`'s:
--   * `state` is widened by rewriting its ENUM rather than by a CHECK, because
--     MySQL 5.7 has no CHECK, and an ENUM that does not contain a value refuses
--     it -- the closed-set property the column is for;
--   * `retire_reason` is a NULLable ENUM for the same reason: the three names are
--     a closed set, and NULL is what every non-retired row holds;
--   * the biconditional the Postgres CHECK holds -- a row is retired exactly when
--     it carries a reason -- has no home here, so the store enforces it on both
--     dialects instead. That is why the store's check exists at all rather than
--     being left to the schema.
ALTER TABLE change_ledger
    MODIFY COLUMN state ENUM('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired') NOT NULL;

ALTER TABLE change_ledger
    ADD COLUMN retire_reason ENUM('superseded', 'fixture', 'out_of_scope') NULL;

ALTER TABLE change_ledger
    ADD COLUMN retired_at DATETIME NULL;
