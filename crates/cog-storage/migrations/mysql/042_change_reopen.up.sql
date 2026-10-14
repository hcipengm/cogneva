-- Migration: 042_change_reopen
-- A row the account had as done, whose check a later pass found no longer holds.
-- See the Postgres migration of the same name for the whole of the reasoning:
-- moving a row out of `landed` clears `landed_at`, so without a row here the
-- account cannot tell "it called this done and was wrong" from "nothing ever
-- measured it", and only the first is a fact about the tree having moved.
--
-- utf8mb4_bin with the table, as every identity column here is: the identity is
-- compared byte-exact, and a case-insensitive collation would make two identities
-- that differ only in case one row.
--
-- MySQL has no `DEFAULT` on a nullable TIMESTAMP the way Postgres does (a
-- TIMESTAMP column is NOT NULL with an implicit default unless declared
-- otherwise), so `observed_at` takes its default explicitly and `was_landed_at`
-- is nullable on purpose, as on the Postgres side.

CREATE TABLE IF NOT EXISTS change_reopen (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    observed_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kind          VARCHAR(32)  NOT NULL,
    scope         VARCHAR(191) NOT NULL,
    literal       VARCHAR(512) NOT NULL,
    was_landed_at DATETIME     NULL,
    measured_rev  VARCHAR(64)  NOT NULL DEFAULT '',
    PRIMARY KEY (id),
    INDEX idx_change_reopen_observed_at (observed_at),
    INDEX idx_change_reopen_identity (kind, scope, literal, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
