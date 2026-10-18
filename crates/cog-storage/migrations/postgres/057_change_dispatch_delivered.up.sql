-- Migration: 057_change_dispatch_delivered
-- How many owed changes a commit on the line claims to have landed.
--
-- The ledger's `landed` is a claim about the tree: a pass re-evaluates the row's
-- check and writes back what it found. That is the right state, and it is not
-- the same fact as delivery. A change whose diff went in but whose check the
-- tree still fails is delivered and not landed; a row the tree already satisfied
-- before anybody touched it measures landed and was never delivered. A reader
-- that has only the state cannot tell those apart, and a pass that handed over
-- two hundred rows has no column that says how many of them reached the line.
--
-- The landing machine writes the hand-over into the commit it makes
-- (`Landing change <task id>`), so the delivery is readable off the same tree
-- the state is measured on: the pass reads the trailers and counts the owed rows
-- they name. This column is where that count survives the process.
--
-- Nullable: a row written before the column existed, and a pass whose commit log
-- could not be read, both have nothing to say. Zero is a measurement -- "none
-- this pass" -- and must not be what a failed read writes.

ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS delivered BIGINT;
