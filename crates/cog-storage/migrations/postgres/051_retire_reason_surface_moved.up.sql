-- Migration: 051_retire_reason_surface_moved
--
-- A fourth retirement reason: the artifact this row names left the tree the judge
-- reads, by a decision taken outside that tree.
--
-- The three existing reasons all describe rows that never named real work: a
-- fixture, an identity a later declaration renamed, a path no criterion here can
-- read. This one describes the opposite -- work that was done and measured, whose
-- evidence surface was then moved out of the measured tree on purpose. The row
-- cannot settle again: the judge returns `scope_absent_in_revision` and writes no
-- state, so it stays `landed` with a `measured_at` that never advances, and the
-- age of that timestamp reads as neglect while nothing can be done about it.
--
-- It is not `out_of_scope`. That reason's criterion is a declaration of the trees
-- the account measures, and the declaration in force says this tree is the only
-- one read. These rows are inside that tree's paths and their surfaces are gone
-- from it; borrowing the word would put "never measurable here" and "was
-- measurable, then moved" under one name, and a reader asking whether the row can
-- come back -- the one question the two answers differ on -- would get the wrong
-- one.
--
-- Widening both tables in one migration: the intake refuses a value outside its
-- CHECK before the fold ever parses it, and the account refuses the same value on
-- its own column, so widening one alone fails halfway with the intake holding a
-- request the account will not take.
--
-- The constraint on `retire_reason` was created inline on `ADD COLUMN` in 038, so
-- it carries PostgreSQL's generated name. Both files of this migration are
-- re-runnable by hand: the DROP always precedes the ADD.

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retire_reason_check;

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retire_reason_check
    CHECK (retire_reason IS NULL
           OR retire_reason IN ('superseded', 'fixture', 'out_of_scope', 'surface_moved'));

ALTER TABLE change_ledger_retired
    DROP CONSTRAINT IF EXISTS change_ledger_retired_intake_reason_check;

ALTER TABLE change_ledger_retired
    ADD CONSTRAINT change_ledger_retired_intake_reason_check
    CHECK (reason IN ('superseded', 'fixture', 'out_of_scope', 'surface_moved'));
