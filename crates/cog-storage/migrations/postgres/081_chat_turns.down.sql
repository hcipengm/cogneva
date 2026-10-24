-- Migration: 081_chat_turns (down)
--
-- Drops the transcript this migration created. The two tables it retired are
-- NOT recreated here: their definitions live in 008 and 009, which is where a
-- rollback that far would reinstate them from, and copying that DDL into this
-- file would be a third definition of a schema no code writes to.
DROP TABLE IF EXISTS chat_turns;
