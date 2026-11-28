-- Migration: 094_chat_turn_attachments (down)
-- Going back to the previous build forgets what was pasted into earlier
-- conversations: the words stay, the picture they were about does not.
ALTER TABLE chat_turns DROP COLUMN attachments;
