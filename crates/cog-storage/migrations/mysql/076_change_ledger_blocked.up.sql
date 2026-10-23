-- Migration: 076_change_ledger_blocked
--
-- See the Postgres migration of the same name for what the block means and why
-- it is a flag beside the state rather than a fifth state.
--
-- Dialect differences, the same as `074_retire_reason_answered_elsewhere`'s:
-- MySQL 5.7 has no CHECK, so the closed set is an ENUM. The all-or-nothing rule
-- over the three columns has no MySQL 5.7 spelling short of a trigger, so it is
-- enforced by the writer on this dialect rather than by the table -- the one
-- direction the account is weaker here, stated rather than left to be
-- discovered.
ALTER TABLE change_ledger
    ADD COLUMN blocked_reason
        ENUM('pool_unavailable', 'upstream_reading', 'owner_decision',
             'external_input', 'dependency') NULL,
    ADD COLUMN unblock_condition TEXT NULL,
    ADD COLUMN blocked_since TIMESTAMP NULL;

CREATE INDEX change_ledger_blocked_idx
    ON change_ledger (blocked_reason, blocked_since);
