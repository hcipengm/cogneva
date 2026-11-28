ALTER TABLE cog_observability_events ALTER COLUMN payload TYPE JSONB USING payload::jsonb;
ALTER TABLE cog_observability_logs   ALTER COLUMN metadata TYPE JSONB USING metadata::jsonb;
ALTER TABLE cog_observability_squads ALTER COLUMN agents   TYPE JSONB USING agents::jsonb;
