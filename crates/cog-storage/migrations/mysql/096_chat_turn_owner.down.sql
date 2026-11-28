DROP INDEX idx_chat_turns_owner ON chat_turns;
ALTER TABLE chat_turns DROP COLUMN owner;
