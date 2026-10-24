-- Migration: 082_datasets (down)
--
-- Drops the catalog. The objects it described are NOT deleted here: this table
-- is the only record of what they are, so removing it and the bytes in one
-- step would destroy data under the appearance of a rollback. Collect the
-- objects named by these rows first if that is the intent.
DROP TABLE IF EXISTS datasets;
