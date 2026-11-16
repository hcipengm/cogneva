-- 库只存列：这三列都是整块读的载荷，没有一条按 JSON 键查它们，所以存 text。
-- text 在语法上不可查询——要按某个键查，那一步是「给它加一列」，不是在这里开 JSON 函数。
ALTER TABLE IF EXISTS cog_observability_events ALTER COLUMN payload TYPE TEXT USING payload::text;
ALTER TABLE IF EXISTS cog_observability_logs   ALTER COLUMN metadata TYPE TEXT USING metadata::text;
ALTER TABLE IF EXISTS cog_observability_squads ALTER COLUMN agents   TYPE TEXT USING agents::text;
