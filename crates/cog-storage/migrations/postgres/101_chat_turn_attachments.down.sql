-- Migration: 101_chat_turn_attachments (down)
-- Dropping the column forgets what was pasted into earlier conversations: a
-- reopened page then shows the words without the picture they were about. The
-- media blocks were never anywhere else, so going back to the previous build
-- implies exactly that loss.
ALTER TABLE chat_turns DROP COLUMN IF EXISTS attachments;
