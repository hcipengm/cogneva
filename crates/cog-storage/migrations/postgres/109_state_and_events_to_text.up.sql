-- 两列都是整块读的载荷，没有一条按 JSON 键查它们：存 text，语法上不可查询。
ALTER TABLE IF EXISTS cog_events    ALTER COLUMN payload TYPE TEXT USING payload::text;
ALTER TABLE IF EXISTS cog_dag_state ALTER COLUMN state   TYPE TEXT USING state::text;
