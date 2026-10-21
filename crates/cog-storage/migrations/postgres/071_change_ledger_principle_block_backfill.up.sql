-- Migration: 071_change_ledger_principle_block_backfill
-- Give the rows the principle ledger filed before its citations carried a face
-- the face they were filed under.
--
-- The face is folded from the citations, and a citation written before the
-- field existed has no `block` key at all. Folding skips it, so those rows read
-- `none` -- the same value a row gets when no document filed it, which is a
-- different fact. The account cannot ask its writers again: this intake emits
-- the principles still outstanding, and these rows stopped being emitted when
-- they entered the account, so nothing will ever rewrite their citations.
--
-- Only this one producer is backfilled, and only because the face is not its
-- document's opinion but its own construction: a principle's reading is filed
-- under principles and nowhere else, which is why the code that writes it fills
-- the field unconditionally. Every other producer's face is something a
-- document said, and a document that said nothing must keep reading as silence.
--
-- Idempotent by construction: after it runs, no citing entry with this doc is
-- left without `block`, so a second run matches nothing.
UPDATE change_ledger
   SET sources = (
           SELECT jsonb_agg(
                      CASE
                          WHEN entry ? 'block' THEN entry
                          ELSE entry || jsonb_build_object('block', 'principles')
                      END
                      ORDER BY position)
             FROM jsonb_array_elements(sources) WITH ORDINALITY AS cited(entry, position))
 WHERE EXISTS (
           SELECT 1
             FROM jsonb_array_elements(sources) AS entry
            WHERE entry->>'doc' = 'principle_ledger'
              AND NOT (entry ? 'block'));
