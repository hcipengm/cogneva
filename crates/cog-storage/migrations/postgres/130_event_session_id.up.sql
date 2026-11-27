-- 会话是信封的一格，不是载荷里的一段 JSON。
--
-- 一次对话的轮、消息、工具调用都挂在同一个会话上；此前会话只写在
-- `attributes->>'session'` 里，按会话读要走表达式、无法按列分组，也没有列索引。
-- 列只加不改历史：旧行这一格是 NULL，读侧的空会话与「没有会话」因此分得开。
ALTER TABLE cogneva_event ADD COLUMN IF NOT EXISTS session_id TEXT;

-- 只给有会话的行建索引：外部情报、A2A 提交这些一线事实没有会话，不该占索引。
CREATE INDEX IF NOT EXISTS cogneva_event_session_id_idx
    ON cogneva_event (session_id)
    WHERE session_id IS NOT NULL;
