-- Migration: 035_drop_principle_debt
-- Drops the debt dispatcher's own series, which nothing writes any more.
--
-- Why the table goes rather than staying empty. The rows it held were one
-- dispatcher's account of its own passes, and that dispatcher is gone: the
-- registry files it read are now a reading source of the change ledger, so the
-- work it used to hand over is declared and measured there, by a loop whose
-- verdict is drawn from the tree rather than reported by the process that did
-- the work. Keeping the table would leave two accounts of the same debt -- the
-- change ledger's rows and this series -- and the one a reader happened to open
-- would decide what they believed. A table with no writer is not a spare copy
-- of the account; it is the second copy, and the second copy is the one that
-- goes stale silently.
--
-- The series is not migrated anywhere. Its counts were never a source of truth
-- for what the tree owed -- that was always the tree -- so there is nothing to
-- carry over: whatever debt those passes were about is either already a row in
-- the change ledger or is not owed any more.

DROP TABLE IF EXISTS principle_debt;
