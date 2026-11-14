DROP INDEX IF EXISTS idx_chat_turns_owner;
ALTER TABLE chat_turns DROP COLUMN IF EXISTS owner;
