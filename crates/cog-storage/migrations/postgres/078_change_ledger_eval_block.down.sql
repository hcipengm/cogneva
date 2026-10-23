-- Migration: 078_change_ledger_eval_block (down)
--
-- The narrow set is restored, and a row filed under the face it does not know
-- would violate the restored CHECK. Nothing is deleted: the intake keeps what
-- each document said, so widening the set again and folding once more restores
-- those rows. Clearing the intake's block first is what makes the narrow
-- constraint expressible -- the account's column is generated, so the citations
-- are the half that can move.
UPDATE change_ledger_declared
   SET block = 'none'
 WHERE block = 'eval';

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_closed
        CHECK (block IN ('tasks', 'token', 'resources', 'principles',
                         'change', 'persistence', 'communication',
                         'security', 'install', 'observability',
                         'ci', 'delivery', 'none'));

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
