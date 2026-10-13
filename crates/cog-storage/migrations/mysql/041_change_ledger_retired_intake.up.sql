-- Migration: 041_change_ledger_retired_intake
-- The retirement declarations' intake. See the Postgres migration of the same
-- name for why it is a separate table from `change_ledger_declared`, why the key
-- is the identity alone rather than the identity plus a declarer, and why
-- `folded_at` is stamped after the retirement is written.
--
-- Dialect differences are the same as `change_ledger_declared`'s:
--   * utf8mb4_bin, not the case-insensitive collation: the identity is byte-exact,
--     and two identities differing only in case name two different artifacts;
--   * DATETIME rather than the TIMESTAMPTZ Postgres uses;
--   * `reason` is an ENUM rather than a VARCHAR with a CHECK, MySQL 5.7 having no
--     CHECK. The ENUM refuses a value outside the closed set the same way the
--     Postgres CHECK does.
CREATE TABLE IF NOT EXISTS change_ledger_retired (
    kind        VARCHAR(32)  NOT NULL,
    scope       VARCHAR(191) NOT NULL,
    literal     VARCHAR(512) NOT NULL,
    reason      ENUM('superseded', 'fixture', 'out_of_scope') NOT NULL,
    source      VARCHAR(512) NOT NULL,
    declared_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    folded_at   DATETIME NULL,
    PRIMARY KEY (kind, scope, literal),
    INDEX idx_change_ledger_retired_folded_at (folded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
