-- Migration: 038_change_ledger_retired (down)
-- The old schema has no way to say "retired", so rows in that state have to go
-- somewhere before the CHECK narrows.
--
-- They move to `undecided`, not out of the table. That is the least-committal
-- non-terminal state and the one these rows held before retirement existed: an
-- identity whose check could not be settled was left as a document gave it, and
-- a later measurement decides it again. Deleting them instead would lose the
-- identity and its sources, which is more than the downgrade is meant to cost --
-- the same reason the state was added rather than the rows deleted in the first
-- place. The reason column and the retirement time go with the state, since
-- nothing in the old schema keeps them.
--
-- Order matters: the biconditional has to be dropped before the UPDATE, because
-- moving a row off 'retired' while it still carries a reason would violate it.
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retired_reason_check;

UPDATE change_ledger SET state = 'undecided' WHERE state = 'retired';

ALTER TABLE change_ledger DROP COLUMN IF EXISTS retired_at;
ALTER TABLE change_ledger DROP COLUMN IF EXISTS retire_reason;

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_state_check;

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_state_check
    CHECK (state IN ('conflict', 'pending', 'partial', 'undecided', 'landed'));
