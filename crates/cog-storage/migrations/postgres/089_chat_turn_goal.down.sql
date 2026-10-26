-- Migration: 089_chat_turn_goal (down)
-- Dropping the column forgets which conversations handed work to the
-- orchestrator; a reopened page then shows every conversation as a plain chat.
-- That is what going back to the previous build implies: it kept the goal only
-- on the live frame.
ALTER TABLE chat_turns DROP COLUMN IF EXISTS goal_id;
