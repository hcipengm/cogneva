-- Migration: 039_change_ledger_unsettled_reason (down)
-- Dropping the column keeps every row and every state, and loses only the reason
-- an undecided row was stuck for. See the Postgres migration of the same name.

ALTER TABLE change_ledger
    DROP COLUMN unsettled_reason;
