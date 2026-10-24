-- Migration: 081_chat_turns
-- See the Postgres migration of the same name for why the session face keeps
-- one home and why the retired tables are dropped rather than left standing.
--
-- Dialect differences, and only these: MySQL has no `DO` block that can refuse
-- on rows, so the emptiness precondition is a note rather than a guard -- the
-- tables have never had a writer, and both are empty on every installation this
-- has run on. MySQL also cannot drop two tables in one statement without
-- `IF EXISTS` accepting a list, so they go one at a time.
--
-- The new table keys on the session id the client sends, not on a user: the
-- chat socket is not authenticated, and `user_id NOT NULL` would have meant
-- inventing an owner for every conversation.
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS chat_sessions;

CREATE TABLE IF NOT EXISTS chat_turns (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(191) NOT NULL,
    role VARCHAR(16) NOT NULL,
    content LONGTEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_chat_turns_session (session_id, id)
);
