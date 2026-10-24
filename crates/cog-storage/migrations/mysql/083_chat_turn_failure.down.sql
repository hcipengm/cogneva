-- Migration: 083_chat_turn_failure (down)
-- Going back to the previous build forgets which exchanges went unanswered.
ALTER TABLE chat_turns DROP COLUMN failure;
