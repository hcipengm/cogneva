-- Migration: 091_change_ledger_remeasure_intake
-- The re-measurement intake. See the Postgres migration of the same name for why
-- it is a separate table from the declaration and retirement intakes, and why
-- `folded_at` is stamped after the measurement is written.
--
-- Dialect differences are the same as `change_ledger_retired`'s:
--   * utf8mb4_bin, not the case-insensitive collation: the identity is byte-exact,
--     and two identities differing only in case name two different artifacts;
--   * DATETIME rather than the TIMESTAMPTZ Postgres uses.
CREATE TABLE IF NOT EXISTS change_ledger_remeasure (
    kind        VARCHAR(32)  NOT NULL,
    scope       VARCHAR(191) NOT NULL,
    literal     VARCHAR(512) NOT NULL,
    source      VARCHAR(512) NOT NULL,
    declared_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    folded_at   DATETIME NULL,
    PRIMARY KEY (kind, scope, literal),
    INDEX idx_change_ledger_remeasure_folded_at (folded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
