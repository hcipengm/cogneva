-- Migration: 086_agent_messages
-- What one unit said to another, and what came back.
--
-- A command sent to an agent had two ends and no record of either meeting: the
-- inbox stream carried the bytes, the ask route opened a reply stream, and once
-- both were read and acknowledged neither side could answer "what did I say to
-- this agent, and did it answer". The caller's only copy was the HTTP response
-- of the call itself -- and a deployment reading that response once, in a
-- terminal, kept nothing.
--
-- `outcome` is the settled shape of one send: queued (delivered, no answer
-- asked for), replied, timed_out (the wait this request asked for expired),
-- refused (the route declined before delivery) and failed (delivery or the
-- reply stream broke). It is one column rather than null checks across `reply`:
-- "no reply" and "not yet known" are different facts, and a reader that has to
-- infer which one it is from a null will get it wrong.
--
-- `reply` holds the answer only for `replied`. For `failed` the error text goes
-- in `outcome_detail` instead, because an error is not something the agent said
-- and must not read back as a transcript line from it.
CREATE TABLE IF NOT EXISTS agent_messages (
    id BIGSERIAL PRIMARY KEY,
    from_unit TEXT NOT NULL CHECK (length(btrim(from_unit)) > 0),
    to_unit TEXT NOT NULL CHECK (length(btrim(to_unit)) > 0),
    kind TEXT NOT NULL CHECK (kind IN ('prompt', 'steering', 'continue', 'abort', 'reset')),
    body TEXT NOT NULL,
    outcome TEXT NOT NULL
        CHECK (outcome IN ('queued', 'replied', 'timed_out', 'refused', 'failed')),
    reply TEXT,
    outcome_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One conversation, newest first: the reader asks for a peer's latest lines.
CREATE INDEX IF NOT EXISTS idx_agent_messages_peer
    ON agent_messages (to_unit, id DESC);
-- The same conversation from the other side, so a peer's own history is not a
-- full scan of everyone else's traffic.
CREATE INDEX IF NOT EXISTS idx_agent_messages_sender
    ON agent_messages (from_unit, id DESC);
