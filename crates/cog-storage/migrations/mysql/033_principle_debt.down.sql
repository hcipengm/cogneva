-- Migration: 033_principle_debt (down)
-- Dropping the table loses the dispatcher's whole series. See the Postgres
-- migration of the same name.

DROP TABLE IF EXISTS principle_debt;
