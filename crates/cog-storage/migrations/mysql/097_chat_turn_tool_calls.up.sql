-- Migration: 097_chat_turn_tool_calls
-- What the answer ran, and who ran it, kept with the answer. Same reasoning as
-- postgres 104: the tool frames lived on the socket, so a reopened conversation
-- lost the panel built from them (the todo list a conversation maintains).
--
-- LONGTEXT for the calls: a turn that ran many tools with large results is a
-- single JSON document, and TEXT's 64 KiB limit truncates it silently.
-- `agent_id` is VARCHAR(191) so it compares under the same collation as the
-- other key columns.
ALTER TABLE chat_turns ADD COLUMN tool_calls LONGTEXT;
ALTER TABLE chat_turns ADD COLUMN agent_id VARCHAR(191);
