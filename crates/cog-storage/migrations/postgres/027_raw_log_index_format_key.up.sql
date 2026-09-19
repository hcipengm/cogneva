-- Migration: 027_raw_log_index_format_key
-- Widen the raw_log_index key to include the file's format.
--
-- The key was (stream_name, log_date), but a raw file is identified by
-- (stream, date, format): the logger rotates on the date *and* on the
-- configured format, so changing the format opens a second file for the same
-- stream and date instead of re-encoding the first. Both files then promoted
-- onto one key, and the later upsert overwrote the earlier row — leaving one
-- archived file whose name appeared nowhere in the index. After a cold
-- promotion that file exists only in the object store, findable by listing it
-- and by nothing else.
--
-- Existing rows are backfilled from the path they point at, which is where the
-- format was recorded all along.

ALTER TABLE raw_log_index
    ADD COLUMN IF NOT EXISTS format VARCHAR(16) NOT NULL DEFAULT 'jsonl';

UPDATE raw_log_index
    SET format = 'proto'
    WHERE storage_path LIKE '%.proto.bin%';

ALTER TABLE raw_log_index DROP CONSTRAINT IF EXISTS raw_log_index_pkey;
ALTER TABLE raw_log_index ADD PRIMARY KEY (stream_name, log_date, format);
