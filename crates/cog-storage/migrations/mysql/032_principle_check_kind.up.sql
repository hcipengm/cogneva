-- Migration: 032_principle_check_kind
-- Which of the four ways a principle can be checked this one is meant to be
-- checked by. See the Postgres migration of the same name for why the ledger
-- needs it, why NULL is a value rather than a gap, and why the four names are
-- stored rather than the three "tiers" they group into.
--
-- Dialect differences are the same as `change_ledger`'s:
--   * `check_kind` is an ENUM rather than a CHECKed VARCHAR, because MySQL 5.7 has
--     no CHECK, and an ENUM that does not contain a value refuses it -- which is
--     the closed-set property the column is for;
--   * it is NULLable, which is what makes "nobody has declared a tier" storable
--     apart from any of the four;
--   * the table's utf8mb4_bin collation carries, as every identity column here
--     does. The names are compared as the store writes them, so the collation
--     makes no difference to the four values themselves, but it keeps the column
--     reading the same on both dialects.

ALTER TABLE principle_ledger
    ADD COLUMN check_kind ENUM('shape', 'tree', 'live', 'judgement') NULL;

ALTER TABLE principle_ledger_declared
    ADD COLUMN check_kind ENUM('shape', 'tree', 'live', 'judgement') NULL;
