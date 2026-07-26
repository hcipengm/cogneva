use async_trait::async_trait;
use cog_core::{CheckType, GuardAuditRecorder, GuardResult};
use cog_core::{Message, ToolCall};
use sqlx::PgPool;

/// PostgreSQL-backed audit recorder for guardrail decisions.
/// Stores every input check, output check, and tool call check in an
/// append-only table for compliance, audit, and security analytics.
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct PostgresAuditRecorder {
    pool: PgPool,
}

impl PostgresAuditRecorder {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the required table and indexes if they do not exist.
    pub async fn init_schema(&self) -> cog_core::SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_guard_audit_log (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                guard_type TEXT NOT NULL,
                check_type TEXT NOT NULL,
                verdict TEXT NOT NULL,
                reason TEXT,
                rule TEXT,
                input_hash TEXT NOT NULL,
                messages JSONB,
                response TEXT,
                tool_call JSONB
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| cog_core::SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_guard_audit_timestamp ON cog_guard_audit_log(timestamp)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| cog_core::SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_guard_audit_verdict ON cog_guard_audit_log(verdict)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| cog_core::SFError::Database(e.to_string()))?;

        Ok(())
    }

    fn hash_input(messages: &[Message]) -> String {
        let text: String = messages.iter().map(|m| m.content()).collect();
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    fn hash_text(text: &str) -> String {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert(
        &self,
        guard_type: &str,
        check_type: CheckType,
        result: &GuardResult,
        input_hash: String,
        messages: Option<serde_json::Value>,
        response: Option<String>,
        tool_call: Option<serde_json::Value>,
    ) {
        let (verdict, reason, rule) = match result {
            GuardResult::Pass => ("pass".to_string(), None, None),
            GuardResult::Block { reason, rule } => (
                "block".to_string(),
                Some(reason.clone()),
                Some(rule.clone()),
            ),
            GuardResult::Warn { reason, rule } => {
                ("warn".to_string(), Some(reason.clone()), Some(rule.clone()))
            }
        };

        let check_type_str = match check_type {
            CheckType::Input => "input",
            CheckType::Output => "output",
            CheckType::ToolCall => "tool_call",
        };

        if let Err(e) = sqlx::query(
            r#"
            INSERT INTO cog_guard_audit_log
                (guard_type, check_type, verdict, reason, rule, input_hash, messages, response, tool_call)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(guard_type)
        .bind(check_type_str)
        .bind(verdict)
        .bind(reason)
        .bind(rule)
        .bind(input_hash)
        .bind(messages)
        .bind(response)
        .bind(tool_call)
        .execute(&self.pool)
        .await
        {
            tracing::warn!("PostgresAuditRecorder insert failed: {}", e);
        }
    }
}

#[async_trait]
impl GuardAuditRecorder for PostgresAuditRecorder {
    async fn record_input_check(&self, messages: &[Message], result: &GuardResult) {
        let messages_json =
            serde_json::to_value(messages.iter().map(|m| m.content()).collect::<Vec<_>>()).ok();
        self.insert(
            "composite",
            CheckType::Input,
            result,
            Self::hash_input(messages),
            messages_json,
            None,
            None,
        )
        .await;
    }

    async fn record_output_check(&self, response: &str, result: &GuardResult) {
        self.insert(
            "composite",
            CheckType::Output,
            result,
            Self::hash_text(response),
            None,
            Some(response.to_string()),
            None,
        )
        .await;
    }

    async fn record_tool_check(&self, tool: &ToolCall, result: &GuardResult) {
        let tool_json = serde_json::to_value(tool).ok();
        self.insert(
            "composite",
            CheckType::ToolCall,
            result,
            String::new(),
            None,
            None,
            tool_json,
        )
        .await;
    }
}
