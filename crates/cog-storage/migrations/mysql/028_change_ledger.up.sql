-- Migration: 028_change_ledger
-- The change ledger's source of truth: one row per outstanding change, keyed by
-- the identity of the artifact the change names. See the Postgres migration of
-- the same name for why it is a table and why the three timestamps are not
-- interchangeable.
--
-- Two dialect differences are deliberate:
--   * the closed set of states is an ENUM here (MySQL has no CHECK on 5.7), and
--     an ENUM that does not contain a value refuses it rather than storing it;
--   * the collation is utf8mb4_bin, not the utf8mb4_unicode_ci the other tables
--     use. The identity is byte-exact -- two checks that differ only in case are
--     two different artifacts -- and under a case-insensitive collation they
--     would compare equal and collapse onto one row, silently retiring the
--     other. Nothing joins these columns against a case-insensitive column, so
--     the mixed collation is contained.

CREATE TABLE IF NOT EXISTS change_ledger (
    kind          VARCHAR(32)  NOT NULL,
    scope         VARCHAR(191) NOT NULL,
    literal       VARCHAR(512) NOT NULL,
    state         ENUM('conflict', 'pending', 'partial', 'undecided', 'landed') NOT NULL,
    title         TEXT,
    assertion     TEXT,
    sources       JSON NOT NULL DEFAULT (JSON_ARRAY()),
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    measured_at   DATETIME NULL,
    landed_at     DATETIME NULL,
    PRIMARY KEY (kind, scope, literal),
    INDEX idx_change_ledger_state (state),
    INDEX idx_change_ledger_measured_at (measured_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
