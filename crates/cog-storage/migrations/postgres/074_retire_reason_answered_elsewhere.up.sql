-- Migration: 074_retire_reason_answered_elsewhere
--
-- A fifth retirement reason: this row's question was answered in an account this
-- one can read the answer from, on a surface this one's checks cannot search.
--
-- The four existing reasons all describe a row whose question is wrong in the
-- row: a fixture, an identity a later declaration renamed, a scope no criterion
-- here can read, a surface that left by decision. This one describes a row whose
-- question was right when it was asked and has since been answered elsewhere.
-- Nothing enforces a rule, so the platform asks for a check; a check is then
-- written, and it is a test in the private tree the host account keeps. The
-- platform's own identity for that row is a search of one scope of one tree, so
-- the answer is not visible to it, and the row stays outstanding forever.
--
-- It is not `out_of_scope`. That reason's row names a scope this account cannot
-- read; this row's scope is inside the measured tree and is read on every pass.
-- Nor is it `surface_moved`: that row was measured here and its surface left,
-- while this one was never measured here and its evidence never was in this
-- tree. The reader's one question -- can the row come back -- has a different
-- answer here than either: it comes back when the other account stops carrying
-- the gate, which is a fact this platform can read from that account.
--
-- Widening both tables in one migration: the intake refuses a value outside its
-- CHECK before the fold ever parses it, and the account refuses the same value on
-- its own column, so widening one alone fails halfway with the intake holding a
-- request the account will not take.

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retire_reason_check;

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retire_reason_check
    CHECK (retire_reason IS NULL
           OR retire_reason IN ('superseded', 'fixture', 'out_of_scope', 'surface_moved',
                                'answered_elsewhere'));

ALTER TABLE change_ledger_retired
    DROP CONSTRAINT IF EXISTS change_ledger_retired_intake_reason_check;

ALTER TABLE change_ledger_retired
    ADD CONSTRAINT change_ledger_retired_intake_reason_check
    CHECK (reason IN ('superseded', 'fixture', 'out_of_scope', 'surface_moved',
                      'answered_elsewhere'));
