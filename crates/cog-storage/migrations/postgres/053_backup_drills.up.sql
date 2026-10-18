-- Migration: 053_backup_drills
-- One row per restore drill: what a package was opened up and found to contain.
--
-- The backup account says a run produced a package and how big it was. None of
-- that says the package can be put back. A truncated archive, a table whose CSV
-- got cut short, a schema that moved under the data -- each of those leaves a
-- run that concluded `ok` and a package whose size looks ordinary. The only
-- reading that separates them is a restore that is actually performed.
--
-- The drill runs the platform's own binary, in its own Job, against a scratch
-- database it creates and drops. It never touches the live schema: the same
-- account that would record the answer is the one at risk. What it writes here
-- is the answer, not the operation.
--
-- A row is written once, at the end, in one of two outcomes. There is no
-- `running` state on purpose. The account's runs have one because a run can be
-- killed mid-flight and the open row is the only witness that it existed; a
-- drill that is killed writes nothing, and the rule that asks "how long ago did
-- a drill last prove anything" is answered by the newest row's age -- an absent
-- row ages exactly like a stale one, which is the fact worth acting on.
--
-- The columns are the evidence and not a summary of it:
--
--   * `packages_on_volume` and `oldest_package_at` come from walking the
--     directory, not from the writer's own retention pass. They exist so the
--     count can be read by someone who did not do the counting -- the same
--     reason the account's `kept_after` is not trusted on its own.
--   * `tables_restored`/`rows_restored` against `tables_expected`/`rows_expected`
--     are the manifest's claim checked against the scratch database after the
--     copy. A restore that silently skipped a table leaves a smaller count.
--   * `source_runs_matched` is the account table compared row for row against
--     the same rows in the live database. It is the one column that says the
--     copy produced the same records rather than merely a same-sized set.
--
-- `outcome = 'ok'` cannot be written without all of that agreeing: the check
-- below turns "this package is restorable" into a claim the row itself
-- falsifies if any of the evidence is missing. A failed drill must say why, for
-- the same reason a failed run does.
--
-- Rows are never deleted. One row per drill per day is under a thousand rows a
-- decade, and a drill's past outcomes are what tells a fresh repair from a
-- long-standing one.
CREATE TABLE IF NOT EXISTS backup_drills (
    drill_id            TEXT PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ NOT NULL,
    outcome             TEXT NOT NULL,
    package             TEXT,
    package_written_at  TIMESTAMPTZ,
    package_bytes       BIGINT,
    packages_on_volume  INTEGER NOT NULL,
    oldest_package_at   TIMESTAMPTZ,
    tables_restored     INTEGER,
    tables_expected     INTEGER,
    rows_restored       BIGINT,
    rows_expected       BIGINT,
    source_runs_matched BOOLEAN,
    error               TEXT,
    CONSTRAINT backup_drills_outcome_is_known
        CHECK (outcome IN ('ok', 'failed')),
    -- Success is a set of measurements, not an opinion. Writing `ok` while any
    -- of them is missing or disagreeing is refused here rather than trusted to
    -- every future caller of the writer.
    CONSTRAINT backup_drills_ok_carries_its_evidence
        CHECK (
            outcome <> 'ok' OR (
                package IS NOT NULL
                AND package_written_at IS NOT NULL
                AND tables_expected IS NOT NULL
                AND tables_restored IS NOT NULL
                AND tables_restored = tables_expected
                AND rows_expected IS NOT NULL
                AND rows_restored IS NOT NULL
                AND rows_restored = rows_expected
                AND source_runs_matched IS TRUE
            )
        ),
    CONSTRAINT backup_drills_failed_says_why
        CHECK (outcome <> 'failed' OR (error IS NOT NULL AND error <> ''))
);

-- The reader takes the newest row; the question asked of this table most often
-- is how long ago anything was proven.
CREATE INDEX IF NOT EXISTS backup_drills_finished_at_idx
    ON backup_drills (finished_at DESC);

-- The count of packages on the volume is only meaningful next to the pass that
-- counted them, and the newest row is the only one a rule reads. Failing drills
-- still carry it: the walk happens before the restore and is recorded whether
-- or not the restore works.
CREATE INDEX IF NOT EXISTS backup_drills_outcome_idx
    ON backup_drills (outcome, finished_at DESC);
