-- Migration: 037_principle_refusals_briefed (down)
-- Drops the delivery reading. See the Postgres migration of the same name: the
-- refusals keep counting exactly as before, and what is lost is the ability to
-- tell a refusal of a rule the executor had been told from one it had not.

ALTER TABLE principle_refusals
    DROP COLUMN briefed;
