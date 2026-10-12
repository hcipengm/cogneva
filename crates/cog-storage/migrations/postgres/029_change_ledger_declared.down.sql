-- Migration: 029_change_ledger_declared (down)
-- Dropping this throws away the intake, not the account. What is lost is the
-- declarations that have not been folded yet: those changes were asked for and
-- the ledger has no row for them, and nothing else recorded the asking. Rows
-- already folded survive in `change_ledger`, but the citation of which document
-- declared them is in `change_ledger.sources` only if the fold already ran.

DROP TABLE IF EXISTS change_ledger_declared;
