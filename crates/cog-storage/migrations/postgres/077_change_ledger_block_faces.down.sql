-- Migration: 077_change_ledger_block_faces (down)
--
-- The narrow set is restored, and the rows filed under a face it does not know
-- would violate the restored CHECK. They are not deleted: the declaration
-- intake keeps what each document said, so putting the wide set back and
-- folding again restores them. Clearing the account's face for those rows first
-- is what makes the narrow constraint expressible at all -- a generated column
-- cannot be updated directly, so the citations are what moves.
UPDATE change_ledger_declared
   SET block = 'none'
 WHERE block NOT IN ('token', 'resources', 'principles', 'change',
                     'persistence', 'communication');

ALTER TABLE change_ledger_declared
    DROP CONSTRAINT IF EXISTS change_ledger_declared_block_closed;
ALTER TABLE change_ledger_declared
    ADD CONSTRAINT change_ledger_declared_block_closed
        CHECK (block IN ('token', 'resources', 'principles', 'change',
                         'persistence', 'communication', 'none'));

CREATE OR REPLACE FUNCTION change_ledger_block(sources JSONB)
    RETURNS TEXT
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
AS $$
    WITH named AS (
        SELECT DISTINCT entry->>'block' AS block
          FROM jsonb_array_elements(COALESCE(sources, '[]'::jsonb)) AS entry
         WHERE entry->>'block' IN ('token', 'resources', 'principles',
                                   'change', 'persistence', 'communication')
    )
    SELECT CASE WHEN count(*) = 1 THEN min(block) ELSE 'none' END FROM named
$$;

-- Rewriting the citations is what makes the account's folded column follow: the
-- function now folds the wide names to `none`, and the generated column keeps
-- the last value it derived until the citing row is written again.
UPDATE change_ledger
   SET sources = (
        SELECT jsonb_agg(
                 CASE
                   WHEN entry->>'block' IN ('tasks', 'security', 'install',
                                            'observability', 'ci', 'delivery')
                   THEN jsonb_set(entry, '{block}', '"none"')
                   ELSE entry
                 END)
          FROM jsonb_array_elements(sources) AS entry)
 WHERE sources @> '[{"block": "tasks"}]'
    OR sources @> '[{"block": "security"}]'
    OR sources @> '[{"block": "install"}]'
    OR sources @> '[{"block": "observability"}]'
    OR sources @> '[{"block": "ci"}]'
    OR sources @> '[{"block": "delivery"}]';

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_block_closed;
ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_block_closed
        CHECK (block IN ('token', 'resources', 'principles', 'change',
                         'persistence', 'communication', 'none'));
