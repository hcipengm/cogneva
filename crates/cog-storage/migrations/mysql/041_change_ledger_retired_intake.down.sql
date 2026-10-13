-- Migration: 041_change_ledger_retired_intake (down)
-- Dropping the intake loses only the declarations not yet folded. A retirement
-- already folded lives on the row it retired, in `change_ledger`. See the up
-- migration of the same name.

DROP TABLE IF EXISTS change_ledger_retired;
