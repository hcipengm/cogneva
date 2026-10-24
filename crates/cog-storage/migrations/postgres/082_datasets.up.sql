-- Migration: 082_datasets
-- The catalog row that turns bytes in an object store into a dataset.
--
-- Until now the platform could write an object and forget everything about it:
-- the upload route put the bytes and answered with a size and a timestamp that
-- were never stored, so after the response nobody could say what the bucket
-- held, how large it was, or whether it was still intact. A bucket without a
-- catalog is the same defect as a table without a writer, read the other way
-- round -- a store whose contents only exist while someone remembers them.
--
-- One row per (name, version), enforced by the unique constraint rather than by
-- the writer: a version that denotes two different byte strings is not a
-- version, and a second row would make "which one is the dataset" unanswerable.
-- The digest is what makes the row checkable against the object, which is what
-- a restore or a migration is judged by.
--
-- row_count and the coverage interval are nullable because a non-tabular dataset
-- genuinely has no row count and no time span: writing 0 or a placeholder date
-- there would record an answer where there is no question. The two coverage
-- bounds are either both present or both absent, for the same reason.
CREATE TABLE IF NOT EXISTS datasets (
    id BIGSERIAL PRIMARY KEY,
    -- What the dataset is called, stable across versions.
    name TEXT NOT NULL CHECK (length(btrim(name)) > 0),
    -- Which version of it this row describes. Unique per name.
    version TEXT NOT NULL CHECK (length(btrim(version)) > 0),
    -- Where the bytes live in the object store.
    object_key TEXT NOT NULL CHECK (length(btrim(object_key)) > 0),
    -- sha256 of the object, with the algorithm in the value so a row written by
    -- a later scheme is still readable as "not the one you know".
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    bytes BIGINT NOT NULL CHECK (bytes >= 0),
    row_count BIGINT CHECK (row_count IS NULL OR row_count >= 0),
    coverage_from TIMESTAMPTZ,
    coverage_to TIMESTAMPTZ,
    -- Where the bytes came from: a producer name, a URL, a task id. Free text
    -- because the useful part is what a person would need to fetch it again.
    source TEXT NOT NULL,
    media_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT datasets_one_row_per_version UNIQUE (name, version),
    CONSTRAINT datasets_coverage_ordered CHECK (
        coverage_from IS NULL OR coverage_to IS NULL OR coverage_from <= coverage_to
    ),
    CONSTRAINT datasets_coverage_complete CHECK (
        (coverage_from IS NULL) = (coverage_to IS NULL)
    )
);

-- The two reads a catalog serves: the newest version of a dataset, and the
-- versions of one dataset newest first.
CREATE INDEX IF NOT EXISTS idx_datasets_name_created ON datasets (name, created_at DESC);
