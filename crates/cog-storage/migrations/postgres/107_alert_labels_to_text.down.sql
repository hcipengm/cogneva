DROP INDEX IF EXISTS idx_alerts_source_firing;
ALTER TABLE alerts ALTER COLUMN labels TYPE JSONB USING labels::jsonb;
ALTER TABLE alerts DROP COLUMN IF EXISTS source;
