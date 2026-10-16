-- Migration: 047_change_ledger_declared_priority
-- How urgent the document making a declaration said the change was. See the
-- Postgres migration of the same name for why the ledger needs it, why a band is
-- transcribed rather than measured, and why `unstated` is a member of the set
-- rather than NULL.
--
-- The closed set is an ENUM here, as `state` and `retire_reason` are: MySQL 5.7
-- has no CHECK, and an ENUM that does not contain a value refuses it rather than
-- storing it -- the same refusal the Postgres CHECK gives. It is NOT NULL with a
-- default of `unstated`, so a declaration queued before this column existed
-- backfills to the value that says, truthfully, that nothing asked the question.

ALTER TABLE change_ledger_declared
    ADD COLUMN priority ENUM('unstated', 'p0', 'p1', 'p2', 'p3')
        NOT NULL DEFAULT 'unstated';
