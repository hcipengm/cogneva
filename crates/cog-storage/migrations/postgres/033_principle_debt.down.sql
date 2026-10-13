-- Migration: 033_principle_debt (down)
-- Dropping the table loses the dispatcher's whole series: the rows are written
-- by the loop and by nothing else, so there is no other copy to rebuild them
-- from. The tree's debt files survive (they are in git) and the loop starts a
-- fresh series on its next pass, but every "the debt count then" is gone with
-- the table.

DROP TABLE IF EXISTS principle_debt;
