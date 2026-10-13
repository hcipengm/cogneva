-- Migration: 039_change_ledger_unsettled_reason
-- Why a row is `undecided`, kept on the row itself.
--
-- A row is `undecided` only when a measurement could not settle its check for a
-- reason that is a property of the row rather than of the look -- no evaluator
-- for its kind, a predicate that is a whole finding written out, a scope this
-- tree structurally cannot hold. No later pass moves those rows, so `undecided`
-- is where they stay. Which of those reasons a given row carries is the whole
-- answer to "why will this never settle", and until this column it lived only in
-- the pass's in-memory map and in a warning line per row: it was counted as one
-- number per pass and, once the log rolled, the row read as an undecided row with
-- nothing to say why. The reason is not a property of any one pass's look, so it
-- belongs on the row, beside the state it explains.
--
-- Nullable, and NULL is a value rather than a gap: every state but `undecided`
-- carries no reason, and a reason left beside another state would read as one. A
-- row measured before this column existed also reads NULL until its next
-- measurement writes it -- `undecided` is outstanding, so every such row is
-- re-measured within a pass or two of the process that carries this column
-- starting, and the reason column fills itself. That retrofit is why the row is
-- not refused for lacking a reason the way a retired row without one is.
--
-- The closed set is held here as well as in the store and in the judge's own
-- enum, the same three-way the state and the retire reason are held: the judge
-- names the reason, this column refuses a name outside the set, and the store
-- couples the reason to the state. A CHECK only Postgres can hold would be a rule
-- MySQL could not enforce, so the store's coupling is the portable one; this
-- CHECK is the same rule stated where the server can also refuse the write.
--
-- Kept as text rather than folded into `state` or into `retire_reason`: those
-- answer "which state" and "why retired", and this answers "why not settled",
-- which is a different question asked about a different state.

ALTER TABLE change_ledger
    ADD COLUMN IF NOT EXISTS unsettled_reason VARCHAR(32)
        CHECK (unsettled_reason IS NULL OR unsettled_reason IN (
            'revision_unreadable', 'kind_without_predicate', 'predicate_is_a_rendering',
            'scope_outside_tree', 'scope_absent_in_revision', 'git_failed'));
