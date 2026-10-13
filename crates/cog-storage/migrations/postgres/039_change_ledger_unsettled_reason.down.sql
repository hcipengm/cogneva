-- Migration: 039_change_ledger_unsettled_reason (down)
-- Dropping the column keeps every row and every state, and loses only the reason
-- an undecided row was stuck for. The reason is recoverable from the tree -- the
-- next measurement against a revision the judge can read names it again -- so the
-- downgrade costs a pass's worth of re-derivation and no identity.

ALTER TABLE change_ledger
    DROP COLUMN IF EXISTS unsettled_reason;
