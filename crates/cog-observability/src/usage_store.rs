//! PostgreSQL-backed LLM token usage ledger.
//!
//! The gateway is the single point all LLM traffic converges on, which makes it
//! the only place per-call token consumption can be metered. Unlike the quota
//! crate's tenant-billing schema (keyed by user/workspace), this ledger records
//! infrastructure-level usage keyed by upstream+model: it answers "how many
//! tokens did we burn, where, and on what" without needing tenant context that
//! proxied calls do not carry.

use sqlx::PgPool;

/// One metered LLM call.
#[derive(Debug, Clone)]
pub struct LlmUsageRecord {
    /// Pool identity of the upstream that served the call.
    pub upstream: String,
    /// Wire protocol family (`openai` / `anthropic`).
    pub api_style: String,
    /// Real model name as configured on the upstream.
    pub model: String,
    /// `ok` or `error` — errors carry zero tokens but keep the attempt visible.
    pub result: String,
    /// Calling component as normalized by the gateway (`self_review`,
    /// `agent:<role>`, `unknown`); lets per-actor token spend be audited.
    pub actor: String,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// Wall time of the call; for streams this is first-byte to last-byte.
    pub latency_ms: u64,
}

/// PostgreSQL usage store.
#[derive(Clone)]
pub struct LlmUsageStore {
    pool: PgPool,
}

impl LlmUsageStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn init_schema(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS gateway_llm_usage (
                id            UUID PRIMARY KEY,
                ts            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                upstream      TEXT NOT NULL,
                api_style     TEXT NOT NULL,
                model         TEXT NOT NULL,
                result        TEXT NOT NULL,
                tokens_input  BIGINT NOT NULL DEFAULT 0,
                tokens_output BIGINT NOT NULL DEFAULT 0,
                latency_ms    BIGINT NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        // Additive migration for the per-actor dimension; old rows stay
        // attributable as "unknown" without a table rewrite.
        sqlx::query(
            "ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT 'unknown'",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_gateway_llm_usage_ts ON gateway_llm_usage (ts)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record(&self, r: &LlmUsageRecord) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateway_llm_usage
                (id, upstream, api_style, model, result, actor,
                 tokens_input, tokens_output, latency_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(&r.upstream)
        .bind(&r.api_style)
        .bind(&r.model)
        .bind(&r.result)
        .bind(&r.actor)
        .bind(r.tokens_input as i64)
        .bind(r.tokens_output as i64)
        .bind(r.latency_ms as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
