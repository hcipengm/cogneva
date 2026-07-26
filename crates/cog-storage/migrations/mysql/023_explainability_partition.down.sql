-- Migration: 023_explainability_partition (down)
ALTER TABLE explainability REMOVE PARTITIONING;
DROP INDEX idx_explain_brin ON explainability;
