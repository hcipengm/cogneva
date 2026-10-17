-- Migration: 050_resource_events
--
-- The resource account's event half: one row per event a bound moved or a build
-- was turned away, appended and never updated except for `folded_at`.
--
-- Why a table at all: the values a resource bound takes already have a home --
-- git for what we declared and the host's own drop-ins for what the kernel runs
-- with -- so a table of "current values" would be a second definition of the
-- same fact. Events are the other half. A bound that moved at runtime left no
-- commit behind, and a refusal exists only as a fact with an identity and an
-- instant. The in-process counter that reports refusals dies with its pod; the
-- three-day sample on 2026-10-15 found 243 of 244 series starting from zero, so
-- a reading kept in the process cannot answer "how many were turned away while
-- the pod that asked was alive", let alone across restarts.
--
-- This table never holds a current value. It holds what happened, who decided
-- it, which reading the decision was made on, and how it turned out -- so that
-- any row answers those five questions on its own.
--
-- Identity is content, not a cursor: the primary key is the tuple below, and a
-- repeated report of the same event collapses onto one row with a larger
-- `count`. A reporter that retries after a restart therefore cannot duplicate an
-- event, and no consumer has to remember how far it has read.
--
-- Dialect differences, both deliberate: `DATETIME(6)` keeps microseconds so two
-- events a reader must tell apart are not merged onto one second, and the
-- collation is utf8mb4_bin because the subject and actor are identities, not
-- labels -- under a case-insensitive collation two identities that differ only
-- in case would collapse onto one row and retire the other.

CREATE TABLE IF NOT EXISTS resource_events (
    kind          VARCHAR(32)   NOT NULL,
    occurred_at   DATETIME(6)   NOT NULL,
    subject       VARCHAR(191)  NOT NULL,
    actor         VARCHAR(191)  NOT NULL,
    reading       VARCHAR(191)  NOT NULL,
    reading_value DOUBLE        NULL,
    from_value    BIGINT        NULL,
    to_value      BIGINT        NULL,
    outcome       VARCHAR(32)   NOT NULL,
    detail        JSON          NOT NULL,
    count         BIGINT        NOT NULL DEFAULT 1,
    reported_at   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    folded_at     DATETIME(6)   NULL,
    PRIMARY KEY (kind, occurred_at, subject, actor),
    INDEX idx_resource_events_folded_at (folded_at),
    INDEX idx_resource_events_occurred_at (occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
