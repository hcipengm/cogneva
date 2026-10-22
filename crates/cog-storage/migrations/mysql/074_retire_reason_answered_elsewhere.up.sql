-- Migration: 074_retire_reason_answered_elsewhere
--
-- See the Postgres migration of the same name for what the reason means and why
-- it is neither `out_of_scope` nor `surface_moved`.
--
-- Dialect differences, the same as `038_change_ledger_retired`'s: MySQL 5.7 has no
-- CHECK, so both closed sets are ENUMs and are widened by rewriting them. An ENUM
-- that already holds every stored value rewrites in place; a value outside the new
-- list would be refused, which is the same guarantee the Postgres CHECK gives.
--
-- Both tables are widened together: the intake refuses the value before the fold
-- parses it, and the account refuses it again on its own column.
ALTER TABLE change_ledger
    MODIFY COLUMN retire_reason
        ENUM('superseded', 'fixture', 'out_of_scope', 'surface_moved', 'answered_elsewhere') NULL;

ALTER TABLE change_ledger_retired
    MODIFY COLUMN reason
        ENUM('superseded', 'fixture', 'out_of_scope', 'surface_moved', 'answered_elsewhere') NOT NULL;
