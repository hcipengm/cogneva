ALTER TABLE tasks CHANGE declared_duration_seconds timeout_seconds INT UNSIGNED NOT NULL DEFAULT 300;
