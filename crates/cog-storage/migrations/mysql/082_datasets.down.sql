-- Migration: 082_datasets (down)
--
-- Drops the catalog only. The objects it described stay in the store: the table
-- is the only record of what they are, so a rollback that deleted both would
-- destroy data while calling itself a rollback.
DROP TABLE IF EXISTS datasets;
