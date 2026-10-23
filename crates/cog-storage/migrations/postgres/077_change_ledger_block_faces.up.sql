-- Migration: 077_change_ledger_block_faces
-- Widen the folded face to the twelve the programme is actually organised into.
--
-- The first version of this column knew six names, all of them faces the
-- account had already been writing rows under. The rest of the organisation was
-- not missing from the work -- it was missing from the column: tasks, security
-- and credentials, install/upgrade/rollback, observability and alerting,
-- CI/build/deploy and commercial delivery had no value to be filed under, so a
-- document that named one had its answer folded away to `none`. Those rows then
-- read as "nobody named a face", which is the same string the account uses for
-- "nobody was read", and the face owner had no queue to be handed.
--
-- The five cross-cutting lines are deliberately *not* here. A change is filed
-- under one face; a line that runs across all of them is a second question
-- about the same row, and putting it in this column would mean a row has to be
-- counted twice to stay visible under its own face.
--
-- Widened, not opened: the set stays closed. A name outside it still folds to
-- `none` rather than becoming a heading of its own, so a misspelled face reads
-- as silence and is fixed where it was written instead of growing a thirteenth
-- bucket that looks like a face somebody defined.
CREATE OR REPLACE FUNCTION change_ledger_block(sources JSONB)
    RETURNS TEXT
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
AS $$
    WITH named AS (
        SELECT DISTINCT entry->>'block' AS block
          FROM jsonb_array_elements(COALESCE(sources, '[]'::jsonb)) AS entry
         WHERE entry->>'block' IN ('tasks', 'token', 'resources', 'principles',
                                   'change', 'persistence', 'communication',
                                   'security', 'install', 'observability',
                                   'ci', 'delivery')
    )
    SELECT CASE WHEN count(*) = 1 THEN min(block) ELSE 'none' END FROM named
$$;

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_block_closed;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_block_closed
        CHECK (block IN ('tasks', 'token', 'resources', 'principles',
                         'change', 'persistence', 'communication',
                         'security', 'install', 'observability',
                         'ci', 'delivery', 'none'));

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_closed
        CHECK (block IN ('tasks', 'token', 'resources', 'principles',
                         'change', 'persistence', 'communication',
                         'security', 'install', 'observability',
                         'ci', 'delivery', 'none'));
