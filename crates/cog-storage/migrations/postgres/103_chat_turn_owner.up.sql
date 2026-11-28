-- Migration: 103_chat_turn_owner
-- Who a conversation belongs to, recorded where the conversation is.
--
-- The transcript could not answer "which conversations are mine" because no row
-- carried an owner: the socket has established a person at handshake for a
-- while (the spend ledger is keyed by them), but the transcript kept only the
-- session id. A listing built on that could only offer every conversation to
-- every reader.
--
-- The owner goes on this table rather than into a second one beside it, for the
-- reason 081 gives: a session's owner is a fact about the session, and a table
-- of sessions beside a table of its lines is the shape that let the two homes
-- disagree before.
--
-- Rows written before this column keep the empty string. They were recorded
-- without an owner, and the empty string says exactly that: they belong to
-- nobody the platform can name, so no per-person listing shows them. Backfilling
-- a guess here would put a name on a fact nobody measured.
ALTER TABLE chat_turns ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT '';

-- The listing reads one person's sessions newest-first; the transcript and the
-- trim keep using (session_id, id DESC).
CREATE INDEX IF NOT EXISTS idx_chat_turns_owner ON chat_turns (owner, session_id, id);
