-- Migration: 031_principle_ledger
-- The principle ledger's source of truth, its intake and its refusal intake:
-- three tables keyed by the principle's slug. See the Postgres migration of the
-- same name for why each one is a table, why `state` is nullable, and why the
-- fold stamps `folded_at` after writing rather than before.
--
-- Dialect differences are deliberate and the same as `change_ledger`'s:
--   * `state` is an ENUM rather than a CHECKed VARCHAR, because MySQL 5.7 has no
--     CHECK, and an ENUM that does not contain a value refuses it;
--   * the collation is utf8mb4_bin rather than utf8mb4_unicode_ci. A slug is an
--     identity compared byte for byte against the registry's own spelling, and
--     under a case-insensitive collation two slugs differing only in case would
--     compare equal and collapse onto one row -- retiring one principle
--     silently. MySQL's ENUM is NULLable here, which is what makes the third
--     answer (a look that could not settle) storable at all;
--   * the timestamps are DATETIME, as everywhere else in this schema.

CREATE TABLE IF NOT EXISTS principle_ledger (
    slug            VARCHAR(191) NOT NULL,
    lead            TEXT,
    carrier_path    TEXT,
    carrier_sha256  VARCHAR(64),
    host_gate       TEXT,
    platform_gate   TEXT,
    state           ENUM('enforced', 'unenforced') NULL,
    refusals        BIGINT NOT NULL DEFAULT 0,
    last_refused_at DATETIME NULL,
    first_seen_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    measured_at     DATETIME NULL,
    measured_rev    VARCHAR(64) NULL,
    PRIMARY KEY (slug),
    INDEX idx_principle_ledger_state (state, measured_at),
    INDEX idx_principle_ledger_refusals (refusals)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS principle_ledger_declared (
    slug           VARCHAR(191) NOT NULL,
    source         VARCHAR(512) NOT NULL,
    lead           TEXT,
    carrier_path   TEXT,
    carrier_sha256 VARCHAR(64),
    host_gate      TEXT,
    declared_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    folded_at      DATETIME NULL,
    PRIMARY KEY (slug, source),
    INDEX idx_principle_ledger_declared_folded_at (folded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS principle_refusals (
    slug        VARCHAR(191) NOT NULL,
    refused_at  DATETIME NOT NULL,
    source      VARCHAR(256) NOT NULL,
    count       BIGINT NOT NULL DEFAULT 1,
    reported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    folded_at   DATETIME NULL,
    PRIMARY KEY (slug, refused_at, source),
    INDEX idx_principle_refusals_folded_at (folded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
