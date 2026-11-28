ALTER TABLE cog_events    ALTER COLUMN payload TYPE JSONB USING payload::jsonb;
ALTER TABLE cog_dag_state ALTER COLUMN state   TYPE JSONB USING state::jsonb;
