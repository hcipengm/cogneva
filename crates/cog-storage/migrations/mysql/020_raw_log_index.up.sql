-- Migration: 020_raw_log_index
-- Raw data log index for locating protobuf files in object storage.
-- The runtime treats `(stream_name, log_date)` as the logical primary key.

CREATE TABLE IF NOT EXISTS raw_log_index (
    stream_name ENUM('session_raw', 'task_raw', 'agent_raw', 'llm_raw', 'tool_raw', 'system_raw', 'transport_raw') NOT NULL,
    log_date DATE NOT NULL,
    storage_path TEXT NOT NULL,
    record_count INT UNSIGNED NOT NULL DEFAULT 0,
    size_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    checksum VARCHAR(128) NOT NULL,
    first_at DATETIME NOT NULL,
    last_at DATETIME NOT NULL,
    is_hot BOOLEAN NOT NULL DEFAULT true,
    encoding ENUM('protobuf', 'protobuf_zstd3', 'protobuf_zstd9', 'parquet') NOT NULL DEFAULT 'protobuf',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_name, log_date),
    INDEX idx_stream_date (stream_name, log_date DESC),
    INDEX idx_path (storage_path(255)),
    INDEX idx_time_range (stream_name, first_at, last_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
