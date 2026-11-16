-- 库只存列：这九列都是整块读写，没有一条按 JSON 键查它们，所以存 text。
ALTER TABLE IF EXISTS cog_guard_audit_log ALTER COLUMN messages      TYPE TEXT USING messages::text;
ALTER TABLE IF EXISTS cog_guard_audit_log ALTER COLUMN tool_call     TYPE TEXT USING tool_call::text;
ALTER TABLE IF EXISTS cog_hook_events      ALTER COLUMN payload      TYPE TEXT USING payload::text;
ALTER TABLE IF EXISTS cog_snapshots        ALTER COLUMN agent_state  TYPE TEXT USING agent_state::text;
ALTER TABLE IF EXISTS cog_snapshots        ALTER COLUMN context_window TYPE TEXT USING context_window::text;
ALTER TABLE IF EXISTS pge_stage_product    ALTER COLUMN change_identity TYPE TEXT USING change_identity::text;
ALTER TABLE IF EXISTS pge_stage_product    ALTER COLUMN product      TYPE TEXT USING product::text;
ALTER TABLE IF EXISTS cog_taste_intents    ALTER COLUMN payload      TYPE TEXT USING payload::text;
ALTER TABLE IF EXISTS cog_taste_intents    ALTER COLUMN evidence_refs TYPE TEXT USING evidence_refs::text;
