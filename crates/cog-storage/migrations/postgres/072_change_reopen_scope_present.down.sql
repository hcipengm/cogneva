-- Migration: 072_change_reopen_scope_present (down)
-- Dropping the column loses no fact the tree cannot be asked again: the reopen
-- rows themselves stay, and a reader that wants the split can still compare a
-- row's scope against the revision it names. What goes away is the answer the
-- pass had at the moment it wrote the row, which for a surface that has since
-- been restored is not a thing a later look can recover.
ALTER TABLE change_reopen
    DROP COLUMN IF EXISTS scope_present;
