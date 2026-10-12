-- Migration: 029_change_ledger_declared
-- The change ledger's intake: one row per declaration that a document asks for a
-- change. See the Postgres migration of the same name for why it is a table in
-- front of `change_ledger`, why the key carries the declaring document, and why
-- `folded_at` is stamped after the reading rather than before.
--
-- Two dialect differences are deliberate, both as in `change_ledger`:
--   * utf8mb4_bin, not the case-insensitive collation. The identity is
--     byte-exact -- two checks differing only in case name two different
--     artifacts -- and under a case-insensitive collation they would compare
--     equal and collapse onto one row, silently retiring the other.
--   * DATETIME rather than the TIMESTAMPTZ Postgres uses. The platform reads
--     both through the same columns, and neither dialect's default clock is
--     used for anything the caller could have chosen.

CREATE TABLE IF NOT EXISTS change_ledger_declared (
    kind        VARCHAR(32)  NOT NULL,
    scope       VARCHAR(191) NOT NULL,
    literal     VARCHAR(512) NOT NULL,
    doc         VARCHAR(512) NOT NULL,
    title       TEXT,
    assertion   TEXT,
    verdict     TEXT,
    declared_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    folded_at   DATETIME NULL,
    PRIMARY KEY (kind, scope, literal, doc),
    INDEX idx_change_ledger_declared_folded_at (folded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
