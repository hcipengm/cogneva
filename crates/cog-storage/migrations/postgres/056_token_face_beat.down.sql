-- Migration: 056_token_face_beat (down)
-- Dropping this table discards beats, and they cannot all be rebuilt. The TSV
-- files on the host still hold the beats written there, but a beat is written
-- once and the files are the only other copy -- a beat that was ingested from a
-- file since rotated away is gone with this table. The append-only column set
-- also means a re-created table would not be the same account of the same past.

DROP TABLE IF EXISTS token_face_beat;
