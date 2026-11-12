-- Migration: 100_runtime_principle (down)
-- The account's rows go with the table: nothing else references them, and a
-- rule that stayed behind with no table to belong to would be neither in force
-- nor readable.
DROP INDEX IF EXISTS runtime_principle_in_force;
DROP TABLE IF EXISTS runtime_principle;
