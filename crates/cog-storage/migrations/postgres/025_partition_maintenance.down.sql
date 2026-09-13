-- Migration: 025_partition_maintenance (down)
-- Reverses 025_partition_maintenance. Dropping the DEFAULT partition discards
-- whatever it holds — those rows were routed there because no monthly
-- partition covered them, and no other partition can hold them, so the
-- rollback is lossy by nature. Restore from backup before running this if the
-- tables are live.

DO $$
DECLARE
    tbl   record;
    month date;
    part  text;
BEGIN
    FOR tbl IN
        SELECT * FROM (VALUES
            ('messages',            'messages'),
            ('audit_logs',          'audit_logs'),
            ('billing_records',     'billing_records'),
            ('raw_log_index',       'raw_log_index'),
            ('explainability_part', 'explainability')
        ) AS v(parent, prefix)
    LOOP
        part := tbl.prefix || '_default';
        EXECUTE format('DROP TABLE IF EXISTS %I', part);

        month := DATE '2026-07-01';
        WHILE month <= (date_trunc('month', now())::date + INTERVAL '2 months') LOOP
            part := format('%s_y%sm%s', tbl.prefix, to_char(month, 'YYYY'), to_char(month, 'MM'));
            EXECUTE format('DROP TABLE IF EXISTS %I', part);
            month := (month + INTERVAL '1 month')::date;
        END LOOP;
    END LOOP;
END $$;

-- Put raw_log_index back on the plain boolean. Warm and cold both collapse to
-- is_hot = false, which is the pre-025 state.
ALTER TABLE raw_log_index DROP COLUMN IF EXISTS is_hot;
ALTER TABLE raw_log_index DROP CONSTRAINT IF EXISTS raw_log_index_tier_check;
ALTER TABLE raw_log_index DROP COLUMN IF EXISTS tier;
ALTER TABLE raw_log_index ADD COLUMN IF NOT EXISTS is_hot BOOLEAN NOT NULL DEFAULT true;

