-- Migration: 083_chat_turn_failure (down)
-- Dropping the column loses the record of which exchanges went unanswered, so
-- those rows become indistinguishable from answered ones. That is what going
-- back to the previous build implies: it knew no such state.
ALTER TABLE chat_turns DROP COLUMN IF EXISTS failure;
