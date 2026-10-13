-- Migration: 036_change_dispatch_state
-- What each dispatch pass decided, and why it decided that much. See the Postgres
-- migration of the same name for the whole of the reasoning: the pause is a row
-- rather than an absence because a pass that hands nothing over and a pass with
-- nothing to hand over read identically from `change_ledger` alone, and the series
-- is what answers "stopped since when, until when, and did anything move".
--
-- utf8mb4_bin with the table, as every identity column here is: the decision and
-- the pool reading are compared byte-exact against their closed sets, and a
-- case-insensitive collation would let 'Proceed' past a CHECK that names
-- 'proceed'.

CREATE TABLE IF NOT EXISTS change_dispatch_state (
    id                      BIGINT NOT NULL AUTO_INCREMENT,
    observed_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decision                VARCHAR(32) NOT NULL,
    pool_reading            VARCHAR(16) NOT NULL,
    pending                 INT NOT NULL,
    submitted               INT NOT NULL,
    already                 INT NOT NULL,
    failed                  INT NOT NULL,
    unmeasurable            INT NOT NULL,
    resume_at_unix          BIGINT NOT NULL DEFAULT 0,
    evidenced_recovery_unix BIGINT NOT NULL DEFAULT 0,
    quota_window_secs       BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    INDEX idx_change_dispatch_state_observed_at (observed_at),
    CONSTRAINT change_dispatch_state_decision_check
        CHECK (decision IN ('proceed', 'paused_llm_pool_unavailable')),
    CONSTRAINT change_dispatch_state_pool_reading_check
        CHECK (pool_reading IN ('available', 'unavailable', 'absent'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
