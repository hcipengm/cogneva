-- Migration: 025_partition_maintenance
-- The five time-series tables were created with partitions for 2026-05 and
-- 2026-06 only. PostgreSQL routes a row to the partition whose range covers
-- the key; with no partition covering the key and no DEFAULT partition there
-- is nowhere to put the row, so the insert fails. Every month since 2026-07
-- has been outside the window.
--
-- Partition keys per table: messages/audit_logs/billing_records use
-- created_at, raw_log_index uses log_date, explainability_part uses timestamp.
-- explainability_part's partitions carry the parent's old name prefix, so the
-- prefix is spelled out rather than derived from the parent name.
--
-- This closes the gap and installs a DEFAULT partition per table, which is the
-- property that matters: an insert can no longer fail for lack of a partition,
-- no matter what runtime maintenance does. The running application keeps the
-- rolling window open from here and reports whenever DEFAULT receives rows,
-- since rows landing there mean the window fell behind.

DO $$
DECLARE
    tbl      record;
    month    date;
    part     text;
    existing boolean;
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
        -- Monthly partitions from the first uncovered month through two months
        -- ahead, so the window is already open when the month rolls over.
        month := DATE '2026-07-01';
        WHILE month <= (date_trunc('month', now())::date + INTERVAL '2 months') LOOP
            part := format('%s_y%sm%s', tbl.prefix, to_char(month, 'YYYY'), to_char(month, 'MM'));
            SELECT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = part AND n.nspname = current_schema()
            ) INTO existing;
            IF NOT existing THEN
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    part, tbl.parent, month, (month + INTERVAL '1 month')::date
                );
            END IF;
            month := (month + INTERVAL '1 month')::date;
        END LOOP;

        -- Catch-all. Created last so the monthly partitions above are attached
        -- against an empty DEFAULT and cannot conflict with rows already in it.
        part := tbl.prefix || '_default';
        SELECT EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = part AND n.nspname = current_schema()
        ) INTO existing;
        IF NOT existing THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF %I DEFAULT', part, tbl.parent);
        END IF;
    END LOOP;
END $$;

-- raw_log_index recorded its tier as `is_hot BOOLEAN`, which cannot tell the
-- warm tier from the cold one. The tier migrator writes both, and the in-memory
-- index store round-trips both, so a warm file read back from PostgreSQL would
-- look cold and the two stores would answer tier queries differently. Record
-- the tier itself and keep `is_hot` as a value derived from it, so there is one
-- source of truth and the boolean still reads as designed.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'raw_log_index' AND column_name = 'tier'
    ) THEN
        ALTER TABLE raw_log_index ADD COLUMN tier VARCHAR(8) NOT NULL DEFAULT 'hot';
        ALTER TABLE raw_log_index
            ADD CONSTRAINT raw_log_index_tier_check CHECK (tier IN ('hot', 'warm', 'cold'));

        ALTER TABLE raw_log_index DROP COLUMN IF EXISTS is_hot;
        ALTER TABLE raw_log_index
            ADD COLUMN is_hot BOOLEAN GENERATED ALWAYS AS (tier = 'hot') STORED;
    END IF;
END $$;

