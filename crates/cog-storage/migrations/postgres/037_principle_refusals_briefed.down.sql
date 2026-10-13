-- Migration: 037_principle_refusals_briefed (down)
-- Drops the delivery reading. The refusals keep counting exactly as before, so
-- nothing about the account's arithmetic changes; what is lost is the ability to
-- tell a refusal of a rule the executor had been told from one it had not, and
-- with it the gap reading. Every row falls back to being a refusal with no
-- delivery history -- which is the state this column was added to end. The rows
-- are gone for good: no other account derives the briefed value, and re-adding
-- the column cannot reconstruct a look that was made when the event happened.

ALTER TABLE principle_refusals
    DROP COLUMN IF EXISTS briefed;
