-- Migration: 074_retire_reason_answered_elsewhere (down)
--
-- The old schema has no word for "another account answered this", so the rows
-- carrying it have to go somewhere before the CHECKs narrow.
--
-- They return to `pending`, which is where they were before they were retired
-- and where the old vocabulary puts a demand nobody here has answered. That
-- state is the truthful one for them under the old set: the row is a question
-- this account cannot answer, and leaving it out would read as work that had
-- been done. They are re-examinable from there -- a later measurement of the
-- tree leaves them unsettled, and one that finds the declaration moves them.
-- Deleting them would lose the identity and its sources, more than a downgrade
-- should cost.
--
-- In the intake they are deleted outright: a request is not the account's
-- history, and the rows it named are kept in the account above. Leaving them
-- would make the narrowing fail on exactly the values this migration
-- introduced.
--
-- Order matters: the CHECKs are dropped before the rows move.

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retire_reason_check;

UPDATE change_ledger
   SET state = 'pending',
       retire_reason = NULL,
       retired_at = NULL
 WHERE retire_reason = 'answered_elsewhere';

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retire_reason_check
    CHECK (retire_reason IS NULL
           OR retire_reason IN ('superseded', 'fixture', 'out_of_scope', 'surface_moved'));

ALTER TABLE change_ledger_retired
    DROP CONSTRAINT IF EXISTS change_ledger_retired_intake_reason_check;

DELETE FROM change_ledger_retired WHERE reason = 'answered_elsewhere';

ALTER TABLE change_ledger_retired
    ADD CONSTRAINT change_ledger_retired_intake_reason_check
    CHECK (reason IN ('superseded', 'fixture', 'out_of_scope', 'surface_moved'));
