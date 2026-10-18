-- Migration: 056_token_face_beat
-- The token faces' beat log, as a table. The host writes the same beats to two
-- append-only TSV files today; those files stay the first layer, because the
-- beats have to keep being written when the pool or the cluster is the thing
-- that is down, and that is exactly when this table is unreachable. What the
-- files cannot do is answer a query: "which beats were unreadable and for how
-- long", "when this number moved, was it the value or the writer", "which ruler
-- produced the column I am about to compare". Those are index scans here.
--
-- One row per (face, beat_at), not one row per measure. A beat is what the
-- timer writes and what the verdicts are about: `state` and `reason` describe
-- the whole pass, and splitting the measures into rows would leave every one of
-- them carrying a copy of a state that is not about the measure -- and would let
-- a partially ingested beat read as a beat whose other measures were unread.
-- The measures ride along in one JSON object; a measure that was not read is
-- stored as JSON null, never as 0, so "zero" and "not measured" stay apart in
-- the same cell the reader will look at.
--
-- `actor` is who wrote the row (a systemd unit, or a person), not which LLM
-- actor spent the tokens. The spending actor is one of the measures.
--
-- `ruler` is how the numbers were measured -- the statistic and the bound the
-- verdict compared against. It is a column because a value is not a reading
-- without it: the same token count means different things read from upstream
-- truth and from an estimate made before the request left.
--
-- `window_secs` is the width of the window the row covers; 0 means the row is
-- an instant (a pool status read), which is a different claim from a window that
-- happens to be empty. NULL is reserved for rows written before the column
-- existed; the writer always supplies one of the other two.
--
-- `state` is the closed set the reader selects on. Four of them are written
-- here; `stale` is what a reader computes from `beat_at` and belongs to nobody
-- writing a row. A state not in this set is refused rather than stored, and an
-- unknown verdict maps to `unjudged` in the writer instead of defaulting to `ok`
-- -- an unrecognised word must not read as a passing one.
--
-- `writer` carries the hashes of the code that produced the row. Without it a
-- series cannot separate "the value changed" from "the writer changed", and on
-- 2026-10-18 two adjacent beats carried different field names for exactly that
-- reason.
--
-- Append-only: there is no update path, and the ingest inserts with
-- ON CONFLICT DO NOTHING on the identity. A replayed beat is the beat that was
-- already recorded, not a correction of it.

CREATE TABLE IF NOT EXISTS token_face_beat (
    face        VARCHAR(16)  NOT NULL,
    beat_at     TIMESTAMPTZ  NOT NULL,
    actor       VARCHAR(128) NOT NULL,
    state       VARCHAR(16)  NOT NULL
        CHECK (state IN ('ok', 'attention', 'unjudged', 'unreadable', 'zero_sample')),
    reason      TEXT         NOT NULL DEFAULT '-',
    ruler       VARCHAR(128) NOT NULL,
    window_secs BIGINT       NOT NULL,
    readable    BOOLEAN      NOT NULL,
    verdict     TEXT         NOT NULL,
    writer      VARCHAR(64)  NOT NULL,
    measures    JSONB        NOT NULL,
    ingested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (face, beat_at)
);

-- "Which beats need someone to look" is the query the alerting side runs, and
-- it selects by state over time, in that order.
CREATE INDEX IF NOT EXISTS idx_token_face_beat_state
    ON token_face_beat (state, beat_at);

-- "Where did this measure go" reads one face's measures over a window. The
-- measures are in JSON, so this cannot index into them; it narrows by beat
-- interval first, which is what the readers do anyway.
CREATE INDEX IF NOT EXISTS idx_token_face_beat_face_beat_at
    ON token_face_beat (face, beat_at);
