-- Migration: 039_change_ledger_unsettled_reason
-- Why a row is `undecided`, kept on the row itself. See the Postgres migration of
-- the same name for why the ledger needs it, why it is nullable, and why a row
-- measured before the column existed reads NULL until its next measurement.
--
-- The closed set is an ENUM here, as `state` and `retire_reason` are: MySQL 5.7
-- has no CHECK, and an ENUM that does not contain a value refuses it rather than
-- storing it -- the same refusal the Postgres CHECK gives. NULL is what every row
-- that is not undecided holds.

ALTER TABLE change_ledger
    ADD COLUMN unsettled_reason ENUM(
        'revision_unreadable', 'kind_without_predicate', 'predicate_is_a_rendering',
        'scope_outside_tree', 'scope_absent_in_revision', 'git_failed'
    ) NULL;
