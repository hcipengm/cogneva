-- Migration: 078_change_ledger_eval_block
-- Add the face the programme grew after the set was last widened.
--
-- 077 widened the folded face to the twelve the programme was organised into at
-- the time. A thirteenth face was then declared, and the column did not follow.
-- The consequence is not cosmetic: an intake row naming that face fails the
-- closed set and the declaration is refused, so work on that face cannot be
-- filed at all. Folded through the function instead, the citation matches
-- nothing and the row lands on `none`, which is the same value the account uses
-- for `nobody named a face` -- the face owner has no queue, and the row looks
-- like work nobody asked for.
--
-- Widened, not opened. The value is a literal from the closed set the contract
-- carries; anything else still folds to `none`.
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
                                   'ci', 'delivery', 'eval')
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
                         'ci', 'delivery', 'eval', 'none'));

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_closed
        CHECK (block IN ('tasks', 'token', 'resources', 'principles',
                         'change', 'persistence', 'communication',
                         'security', 'install', 'observability',
                         'ci', 'delivery', 'eval', 'none'));
