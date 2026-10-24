-- Migration: 087_open_change_ledger_block
-- The column keeps a face name, not a membership test.
--
-- 070 introduced the folded face with six names, 077 widened it to twelve and
-- 078 added a thirteenth. Each widening had to be written in three places at
-- once -- this function, two CHECK constraints, and a variant list in the
-- service -- and each of them failed quietly if it lagged: a document naming a
-- face the function did not know had its answer folded to `none`, which is the
-- same value the account uses for "nobody named a face", so the new face's
-- work was filed as work nobody asked for.
--
-- The list was never the thing being protected. What the column needs is that
-- its value can travel through SQL, JSONB and metric labels without quoting,
-- which is a shape, not a membership. So the fold now takes any well-formed
-- name and the constraints check the shape: a face declared later is filed by
-- naming it, and nothing here has to be edited for it to be counted. The
-- census reads its rows off the account rather than off a list for the same
-- reason.
--
-- The shape is the one the service validates (`ChangeBlock`): lowercase ASCII,
-- digits and underscores, starting with a letter, at most 64 bytes. A value
-- outside it still folds to `none` -- the intake refuses it first, and this is
-- the second line rather than the loud one.
CREATE OR REPLACE FUNCTION change_ledger_block(sources JSONB)
    RETURNS TEXT
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
AS $$
    WITH named AS (
        SELECT DISTINCT entry->>'block' AS block
          FROM jsonb_array_elements(COALESCE(sources, '[]'::jsonb)) AS entry
         WHERE entry->>'block' ~ '^[a-z][a-z0-9_]{0,63}$'
    )
    SELECT CASE WHEN count(*) = 1 THEN min(block) ELSE 'none' END FROM named
$$;

-- The generated column keeps the values already folded into it; what changes is
-- which values a row may hold from here on.
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_block_closed;
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_block_shape;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_block_shape
        CHECK (block ~ '^[a-z][a-z0-9_]{0,63}$');

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_shape;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_shape
        CHECK (block ~ '^[a-z][a-z0-9_]{0,63}$');
