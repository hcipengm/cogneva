-- Migration: 058_change_ledger_delivered_rev
-- Which commit delivered a change, kept beside the state that says whether the
-- change's claim holds.
--
-- The account has one door a state goes through, and that door reads the tree:
-- `landed` says the row's check measured true. Delivery is a different fact. A
-- diff can reach the line and leave the check failing -- the change was made and
-- did not do what it said -- and a row can measure landed without any commit
-- delivering it, because the tree already satisfied it before anyone touched it.
-- With only `state` and `measured_rev`, those two cases read the same, and a pass
-- that handed two hundred changes over has no column saying how many reached the
-- line.
--
-- The value is the revision of the commit whose landing trailer names the task
-- the change was handed over under. Written by the ledger's own reader of the
-- line, never by the landing process: a record made after the commit is missing
-- for every change whose writer died in between, while the commit itself stays
-- readable.
--
-- Nullable: no row written before this column existed was delivered, and a
-- reader that finds no trailer says nothing rather than writing a revision. The
-- column is never cleared -- a change that reached the line did reach it, and a
-- later revision taking the diff out is what `state` and the reopen record are
-- for.

ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS delivered_rev VARCHAR(64);
