-- Migration: 056_token_face_beat
-- The token faces' beat log, one row per (face, beat_at). See the Postgres
-- migration of the same name for why this is a table beside the append-only
-- files rather than instead of them, why a beat and not a measure is the row,
-- and why an unmeasured cell is stored as JSON null rather than 0.
--
-- Dialect differences, all deliberate:
--   * `state` is an ENUM. The closed set is the point -- an unrecognised word
--     must be refused, not stored and later read as a passing state -- and an
--     ENUM refuses it at the column, which MySQL 5.7 cannot do with CHECK.
--   * `DATETIME(6)` keeps microseconds so two beats in one second stay distinct.
--   * the collation is utf8mb4_bin because (face, beat_at) is an identity.
--   * `measures` has no default: on Postgres a default would let a row be
--     written without measures there and fail here, so both sides require the
--     writer to say what it measured, even when the answer is an empty object.
--
-- Append-only: no update path, and the ingest inserts with ON DUPLICATE KEY
-- UPDATE that does nothing, or INSERT IGNORE, on the identity.

CREATE TABLE IF NOT EXISTS token_face_beat (
    face        VARCHAR(16)  NOT NULL,
    beat_at     DATETIME(6)  NOT NULL,
    actor       VARCHAR(128) NOT NULL,
    state       ENUM('ok', 'attention', 'unjudged', 'unreadable', 'zero_sample') NOT NULL,
    reason      TEXT         NOT NULL,
    ruler       VARCHAR(128) NOT NULL,
    window_secs BIGINT       NOT NULL,
    readable    TINYINT(1)   NOT NULL,
    verdict     TEXT         NOT NULL,
    writer      VARCHAR(64)  NOT NULL,
    measures    JSON         NOT NULL,
    ingested_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (face, beat_at),
    INDEX idx_token_face_beat_state (state, beat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
