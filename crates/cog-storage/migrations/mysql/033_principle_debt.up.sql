-- Migration: 033_principle_debt
-- One row per pass of the debt dispatcher. See the Postgres migration of the
-- same name for why this is a table rather than a log line, why the counts are
-- kept apart instead of summed, and why `bound` is stored beside its source.
--
-- Dialect differences are the same as the rest of this schema's:
--   * `observed_at` is DATETIME, as everywhere else here, and it carries the
--     same DEFAULT CURRENT_TIMESTAMP the Postgres column does -- the stamp is
--     the store's rather than the caller's on both dialects. `rev` and `delta`
--     are NULLable here for the same reason they are there: a pass with no
--     earlier revision to compare against has no delta, and that is stored as
--     NULL rather than 0 so a week of no progress and a week nobody compared do
--     not read the same;
--   * the key is an AUTO_INCREMENT surrogate. The row has no natural key: two
--     passes with identical numbers are two observations, and a key drawn from
--     the values would collapse them and hide that the dispatcher had run;
--   * the collation is utf8mb4_bin, as every identity column here. `by_kind` and
--     `bound_source` here are renderings rather than identities compared
--     elsewhere, but keeping the collation uniform costs nothing and stops the
--     column reading differently from the tables it sits beside.

CREATE TABLE IF NOT EXISTS principle_debt (
    id           BIGINT NOT NULL AUTO_INCREMENT,
    observed_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    by_kind      TEXT NOT NULL,
    submitted    BIGINT NOT NULL DEFAULT 0,
    already      BIGINT NOT NULL DEFAULT 0,
    failed       BIGINT NOT NULL DEFAULT 0,
    over_bound   BIGINT NOT NULL DEFAULT 0,
    bound        BIGINT NOT NULL,
    bound_source VARCHAR(64) NOT NULL,
    rev          VARCHAR(64) NULL,
    delta        BIGINT NULL,
    PRIMARY KEY (id),
    INDEX idx_principle_debt_observed_at (observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
