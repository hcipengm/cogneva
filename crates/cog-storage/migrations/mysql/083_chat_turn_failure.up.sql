-- Migration: 083_chat_turn_failure
-- See the Postgres migration of the same name for why a turn that never got a
-- reply is still recorded.
ALTER TABLE chat_turns ADD COLUMN failure TEXT NULL;
