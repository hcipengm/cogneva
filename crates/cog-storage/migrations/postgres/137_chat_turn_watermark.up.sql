-- 会话投影折到流上的哪个位置。没有这一格，`rebuild_chat_turns` 只能整段重放；有了它，
-- 一段会话每次只折水位之后的事实。水位按会话记：两段会话折到的地方本来就不是同一个数。
CREATE TABLE IF NOT EXISTS chat_turn_watermarks (
    session_id  TEXT        PRIMARY KEY,
    folded_seq   BIGINT      NOT NULL,
    folded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
