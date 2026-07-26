-- Migration: 023_explainability_partition
-- MySQL does not support BRIN indexes; use a regular B-tree index on timestamp.
-- Partitioning is added via RANGE COLUMNS.

ALTER TABLE explainability
    PARTITION BY RANGE COLUMNS (timestamp) (
        PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
        PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
        PARTITION p_future VALUES LESS THAN MAXVALUE
    );

CREATE INDEX idx_explain_brin ON explainability (timestamp);
