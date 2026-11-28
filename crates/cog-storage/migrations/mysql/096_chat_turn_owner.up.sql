-- Migration: 096_chat_turn_owner
-- Who a conversation belongs to, recorded where the conversation is. Same
-- reasoning as postgres 103: the socket knows the person at handshake, the
-- transcript did not keep them, so a listing could not answer "mine".
--
-- Rows written before this column keep the empty string: they were recorded
-- without an owner, and that is what the empty string says. VARCHAR(191) matches
-- session_id so the two key columns compare under the same collation.
ALTER TABLE chat_turns ADD COLUMN owner VARCHAR(191) NOT NULL DEFAULT '';

CREATE INDEX idx_chat_turns_owner ON chat_turns (owner, session_id, id);
