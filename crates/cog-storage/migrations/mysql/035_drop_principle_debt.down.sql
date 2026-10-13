-- Migration: 035_drop_principle_debt (down)
-- Puts the table back, empty. See the Postgres migration of the same name for
-- why the shape comes back without the series: the rows had no writer but the
-- loop that is gone, so there is no second copy to rebuild them from.

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
