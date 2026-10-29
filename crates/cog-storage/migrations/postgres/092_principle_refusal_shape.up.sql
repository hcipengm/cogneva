-- Migration: 092_principle_refusal_shape
-- Which shape the refused call matched, and the text that matched it.
--
-- A refusal has always been recorded under the principle it counts against, and
-- a principle can be carried by more than one shape. `NARROW_ADD_RE` and
-- `BARE_COMMIT_RE` both answer to the shared-tree rule; the deferral table holds
-- several alternatives under one slug. Until now all of them added to one
-- number, so a reading could say "this principle was refused 312 times" and
-- could not say which of its shapes was doing it -- the one question a reader
-- needs answered before deciding whether the rule is being broken or the gate is
-- too tight. The count was right and the attribution was missing.
--
-- `criterion` names the shape that matched, spelled the way the gate's own table
-- spells it (`NARROW_ADD_RE`, `CIRCLED_CHARS`). It is not the slug: the slug is
-- what the refusal is counted against, the criterion is what was found.
--
-- `span` is the text at the match, kept verbatim and cut to a fixed width. It is
-- what makes a row replayable: from the row alone a reader can see the phrase the
-- gate refused without the call, the tree or the session still existing. A
-- summary ("a wide add") would answer the same question only while whoever wrote
-- the summary agreed with whoever reads it; the text does not have that problem.
--
-- The primary key grows to include `criterion`. Two shapes refused in the same
-- second by the same session under one principle are two facts, and the old key
-- folded them into one row that named whichever arrived first -- which is the
-- failure this column exists to end, reintroduced at the key. Both columns have
-- defaults, so every existing row already satisfies the new key and no row is
-- rewritten.
--
-- Rows written before this have `''` in both columns. That is not a shape and
-- must not be grouped as one: a reader that counted them with a real criterion
-- would move refusals into a shape that never produced them. They are the
-- "shape unknown" population, and they are what the reading has to be able to
-- separate from a shape nobody has matched yet.
ALTER TABLE principle_refusals
    ADD COLUMN IF NOT EXISTS criterion VARCHAR(64) NOT NULL DEFAULT '';

ALTER TABLE principle_refusals
    ADD COLUMN IF NOT EXISTS span VARCHAR(512) NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'principle_refusals'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) LIKE '%criterion%'
    ) THEN
        ALTER TABLE principle_refusals DROP CONSTRAINT IF EXISTS principle_refusals_pkey;
        ALTER TABLE principle_refusals
            ADD PRIMARY KEY (slug, refused_at, source, session_id, criterion);
    END IF;
END $$;
