-- Migration: 027_raw_log_index_format_key
-- Widen the raw_log_index key to include the file's format. See the Postgres
-- migration of the same name for why: the key was (stream_name, log_date) while
-- a raw file is identified by (stream, date, format), so the second file for a
-- date overwrote the first file's row and lost its name.

ALTER TABLE raw_log_index
    ADD COLUMN format VARCHAR(16) NOT NULL DEFAULT 'jsonl';

UPDATE raw_log_index
    SET format = 'proto'
    WHERE storage_path LIKE '%.proto.bin%';

ALTER TABLE raw_log_index DROP PRIMARY KEY;
ALTER TABLE raw_log_index ADD PRIMARY KEY (stream_name, log_date, format);
