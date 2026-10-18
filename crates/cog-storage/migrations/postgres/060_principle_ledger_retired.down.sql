-- Migration: 060_principle_ledger_retired (down)
-- The old schema has no way to say "retired", so rows in that state have to go
-- somewhere before the CHECK narrows.
--
-- They move to NULL -- the state these rows held before retirement existed, and
-- the one the old code reads as "nothing has settled this row". That is exactly
-- the reading the migration was written to replace, but a downgrade restoring the
-- old behaviour is the point; deleting the rows would cost the account its
-- history, which is more than the downgrade is meant to take.
UPDATE principle_ledger SET state = NULL WHERE state = 'retired';

ALTER TABLE principle_ledger
    DROP CONSTRAINT IF EXISTS principle_ledger_state_check;

ALTER TABLE principle_ledger
    ADD CONSTRAINT principle_ledger_state_check
    CHECK (state IN ('enforced', 'unenforced'));
