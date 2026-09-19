-- Migration: 027_raw_log_index_format_key (down)
-- Narrowing the key back discards the second file's row for any date that holds
-- two formats, so the archived file it names becomes unreachable again.
ALTER TABLE raw_log_index DROP PRIMARY KEY;
ALTER TABLE raw_log_index ADD PRIMARY KEY (stream_name, log_date);
ALTER TABLE raw_log_index DROP COLUMN format;
