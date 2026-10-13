-- Migration: 037_principle_refusals_briefed
-- Whether the principle a gate refused on had been told to the executor before
-- it broke it. See the Postgres migration of the same name for why a refusal
-- needs this at all, and why `unknown` is a value rather than an absence.
--
-- An ENUM rather than a CHECKed VARCHAR, for the same reason `check_kind` is:
-- the closed set is what the column is for, and an ENUM refuses a value outside
-- it without depending on the server version supporting CHECK. The three names
-- are compared byte-exact, so the table's utf8mb4_bin collation carries here as
-- it does for every identity column in this schema.

ALTER TABLE principle_refusals
    ADD COLUMN briefed ENUM('briefed', 'unbriefed', 'unknown') NOT NULL DEFAULT 'unknown';
