-- Migration: 056_token_face_beat (down)
-- Dropping this table discards beats. See the Postgres file: the host files
-- still hold what was written there, but a beat already rotated out of those
-- files is gone with the table and cannot be replayed.

DROP TABLE IF EXISTS token_face_beat;
