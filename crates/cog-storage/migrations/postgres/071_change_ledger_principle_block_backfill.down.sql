-- Migration: 071_change_ledger_principle_block_backfill (down)
--
-- Nothing to undo. The bytes this migration wrote are the same bytes the
-- producer writes for the same row, so a reader cannot tell a repaired citation
-- from one that arrived repaired; stripping the face back off would take it from
-- rows the producer has since stated it for. Going back to the state where a
-- principle row reads as one nobody filed is not a state anyone wants to restore
-- -- if the fold itself is wrong, the migration to revert is 070's, which drops
-- the column and the citations stay exactly as they are.
SELECT 1;
