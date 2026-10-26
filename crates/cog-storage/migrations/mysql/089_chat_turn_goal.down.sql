-- Migration: 089_chat_turn_goal (down)
-- Going back to the previous build forgets which conversations handed work to
-- the orchestrator.
ALTER TABLE chat_turns DROP COLUMN goal_id;
