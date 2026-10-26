-- Migration: 089_chat_turn_goal
-- See the Postgres migration of the same name for why a hand-off turn keeps its
-- goal id. VARCHAR(64) holds a uuid's 36 characters with room to spare; the
-- column is nullable so "answered here" stays distinguishable from "a goal with
-- an empty id".
ALTER TABLE chat_turns ADD COLUMN goal_id VARCHAR(64) NULL;
