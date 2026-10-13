-- Migration: 035_drop_principle_debt
-- Drops the debt dispatcher's own series, which nothing writes any more. See
-- the Postgres migration of the same name for why the table goes rather than
-- staying empty, and why the series is not migrated anywhere.

DROP TABLE IF EXISTS principle_debt;
