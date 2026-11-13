-- Migration: 095_chat_turn_model_content
-- See the Postgres migration of the same name for why a command keeps the
-- instructions it expanded into.
--
-- LONGTEXT, not TEXT: a skill's SKILL.md is a document, and the ones shipped
-- here run past 64 KiB. TEXT would take the row and truncate it, and a model
-- handed half a procedure reads exactly like a model that ignored half of it.
--
-- Null for every row that is not a command.
ALTER TABLE chat_turns ADD COLUMN model_content LONGTEXT NULL;
