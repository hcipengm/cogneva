-- Migration: 038_change_ledger_retired (down)
-- Rows retired move to `undecided` before the ENUM narrows, for the reason the
-- Postgres down migration gives: the old schema cannot say "retired", and
-- `undecided` both keeps the identity and is the state these rows held before
-- retirement existed. The UPDATE must precede the MODIFY, because narrowing an
-- ENUM over values it does not contain silently rewrites them to the empty
-- string rather than failing.
UPDATE change_ledger SET state = 'undecided' WHERE state = 'retired';

ALTER TABLE change_ledger DROP COLUMN retired_at;
ALTER TABLE change_ledger DROP COLUMN retire_reason;

ALTER TABLE change_ledger
    MODIFY COLUMN state ENUM('conflict', 'pending', 'partial', 'undecided', 'landed') NOT NULL;
