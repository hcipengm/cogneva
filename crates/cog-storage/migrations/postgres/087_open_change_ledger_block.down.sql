-- Migration: 087_open_change_ledger_block (down)
--
-- Back to the thirteen names 078 knew. The account's column is generated, so
-- the half that can be moved is the intake's: a declaration naming a face the
-- narrow set does not know is cleared to `none` first, or the restored CHECK
-- can only run against a table that still violates it. The citations stay as
-- each document wrote them, so widening again files those rows back under
-- their face without anyone re-declaring.
UPDATE change_ledger_declared
   SET block = 'none'
 WHERE block NOT IN ('tasks', 'token', 'resources', 'principles', 'change',
                     'persistence', 'communication', 'security', 'install',
                     'observability', 'ci', 'delivery', 'eval', 'none');

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
    DROP CONSTRAINT IF EXISTS change_ledger_block_shape;
ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_block_closed;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_block_closed
        CHECK (block IN ('tasks', 'token', 'resources', 'principles',
                         'change', 'persistence', 'communication',
                         'security', 'install', 'observability',
                         'ci', 'delivery', 'eval', 'none'));

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_shape;
ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_closed
        CHECK (block IN ('tasks', 'token', 'resources', 'principles',
                         'change', 'persistence', 'communication',
                         'security', 'install', 'observability',
                         'ci', 'delivery', 'eval', 'none'));
