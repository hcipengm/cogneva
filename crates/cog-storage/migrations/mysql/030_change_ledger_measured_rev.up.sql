-- Migration: 030_change_ledger_measured_rev
-- Which revision a row's state was measured against. See the Postgres migration of
-- the same name for why the ledger needs it, and why it is nullable rather than
-- defaulted: a row no look has settled has no revision, and inventing one would
-- report an unmeasured state as a measured one.
--
-- utf8mb4_bin with the table, as every identity column here is: a revision is
-- compared byte-exact, and a case-insensitive collation would make two different
-- revisions compare equal.

ALTER TABLE change_ledger
    ADD COLUMN measured_rev VARCHAR(64) NULL;
