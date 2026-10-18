-- Migration: 060_principle_ledger_retired
-- A state for a row whose principle left the host's roster.
--
-- The account is append-only: a row that was folded in cannot be withdrawn, so a
-- principle that was deleted or renamed on the host side -- its carrier is no
-- longer in the roster the host pushes -- would otherwise stay in the account
-- forever with no state, and a row with no state sorts first in the pass that
-- measures outstanding rows. It is looked at every pass, settles nothing, and
-- comes back to the head of the next one. On every surface that reading is the
-- same as a principle nobody has got to yet, which is the opposite of what it is.
--
-- The exit is a state rather than a delete, for the same reason the change
-- ledger's is: deleting would lose "this was asked for", and the row would be
-- minted again the next time its identity appeared.
--
-- Retired is re-examinable. A carrier that returns to the roster arrives as a
-- reading with a different host value, and the next look writes whatever it
-- finds; nothing here is a tombstone.
--
-- The closed set is a CHECK the original CREATE TABLE named inline, so it is
-- widened by dropping and re-adding it under its generated name. The DROP always
-- precedes the ADD, so a second run of this file drops what the first added and
-- adds it again: Postgres has no `ADD CONSTRAINT IF NOT EXISTS`.
ALTER TABLE principle_ledger
    DROP CONSTRAINT IF EXISTS principle_ledger_state_check;

ALTER TABLE principle_ledger
    ADD CONSTRAINT principle_ledger_state_check
    CHECK (state IN ('enforced', 'unenforced', 'retired'));
