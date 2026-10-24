-- Migration: 081_chat_turns
-- One home for the chat transcript, and it is this table.
--
-- Until now the session face had two definitions that disagreed. `chat_sessions`
-- and `messages` (008, 009) describe sessions owned by a user, with a foreign key
-- to `users`; no code ever wrote them, and both were empty. What the gateway
-- actually kept was a `HashMap` in its own process, capped at 256 sessions and
-- evicted oldest-first -- so a restart, a rollout or an OOM kill lost every
-- conversation, while the schema still said the history was durable. A record of
-- what happens and a record of what was promised, in the same database, is the
-- defect; this migration removes one of them.
--
-- The surviving shape is the one the writer can actually fill. The chat socket is
-- not authenticated, so a `user_id NOT NULL` column would have forced the writer
-- to invent an owner for every conversation and record that invention as a fact.
-- The key here is the session id the client already sends, and an owner, when
-- there is one, becomes a column on this table rather than a second table beside
-- it.
--
-- The retired tables are dropped rather than left in place: an empty table that
-- says "sessions are kept" is a claim, not a schema. They are dropped only after
-- being verified empty; if some installation did hold rows there, this migration
-- fails loudly instead of destroying them. `to_regclass` rather than a plain
-- count, so a database that never had them passes instead of erroring.
DO $$
DECLARE
    stale BIGINT := 0;
    held BIGINT;
BEGIN
    IF to_regclass('public.chat_sessions') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM chat_sessions' INTO held;
        stale := stale + held;
    END IF;
    IF to_regclass('public.messages') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM messages' INTO held;
        stale := stale + held;
    END IF;
    IF stale > 0 THEN
        RAISE EXCEPTION
            'chat_sessions/messages hold % row(s); this migration retires them and will not drop '
            'a transcript it has no way to carry over. Move those rows to chat_turns by hand and '
            're-run.', stale;
    END IF;
END $$;

-- No CASCADE: if anything still depends on these tables, that dependency is
-- something a person needs to look at, not something to collect silently.
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS chat_sessions;

CREATE TABLE IF NOT EXISTS chat_turns (
    id BIGSERIAL PRIMARY KEY,
    -- The id the client sends with every chat message. Not empty: an empty id
    -- would pool unrelated conversations into one transcript.
    session_id TEXT NOT NULL CHECK (length(btrim(session_id)) > 0),
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One index for the two reads: the newest rows of one session (the context
-- window) and the trim that keeps a session's rows bounded.
CREATE INDEX IF NOT EXISTS idx_chat_turns_session ON chat_turns (session_id, id DESC);
