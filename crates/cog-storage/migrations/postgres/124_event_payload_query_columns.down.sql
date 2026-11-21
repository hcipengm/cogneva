DROP INDEX IF EXISTS idx_cogneva_event_llm_call_at;
ALTER TABLE cogneva_event
    DROP COLUMN IF EXISTS tool_path,
    DROP COLUMN IF EXISTS cache_created_tokens,
    DROP COLUMN IF EXISTS cached_tokens,
    DROP COLUMN IF EXISTS task_id_text,
    DROP COLUMN IF EXISTS owner_name,
    DROP COLUMN IF EXISTS upstream_name,
    DROP COLUMN IF EXISTS api_style,
    DROP COLUMN IF EXISTS code_rev,
    DROP COLUMN IF EXISTS ledger_rev,
    DROP COLUMN IF EXISTS call_result,
    DROP COLUMN IF EXISTS price_book,
    DROP COLUMN IF EXISTS cost_micros,
    DROP COLUMN IF EXISTS output_tokens,
    DROP COLUMN IF EXISTS input_tokens;
