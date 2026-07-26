/// PostgreSQL-backed explainability storage.
/// Uses sqlx + JSONB for structured AI decision records.
/// All trait methods map to indexed SQL queries.
use crate::explainability::ExplainabilityBackend;
use crate::ExplainabilityRecord;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

/// Postgres explainability backend.
#[derive(Clone)]
pub struct PostgresExplainabilityBackend {
    pool: PgPool,
}

impl PostgresExplainabilityBackend {
    /// Create a new backend from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connect from a database URL.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self::new(pool))
    }
}

#[async_trait::async_trait]
impl ExplainabilityBackend for PostgresExplainabilityBackend {
    async fn insert(&self, record: ExplainabilityRecord) -> anyhow::Result<()> {
        let data = serde_json::json!({
            "reasoning_chain": record.reasoning_chain,
            "metadata": record.metadata,
        });

        sqlx::query(
            r#"
            INSERT INTO explainability
                (record_id, session_id, task_id, timestamp, agent_id,
                 decision_type, confidence, model, input_tokens, output_tokens, data)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (record_id) DO UPDATE SET
                session_id = EXCLUDED.session_id,
                task_id = EXCLUDED.task_id,
                timestamp = EXCLUDED.timestamp,
                agent_id = EXCLUDED.agent_id,
                decision_type = EXCLUDED.decision_type,
                confidence = EXCLUDED.confidence,
                model = EXCLUDED.model,
                input_tokens = EXCLUDED.input_tokens,
                output_tokens = EXCLUDED.output_tokens,
                data = EXCLUDED.data
            "#,
        )
        .bind(&record.record_id)
        .bind(&record.session_id)
        .bind(&record.task_id)
        .bind(record.timestamp)
        .bind(&record.agent_id)
        .bind(&record.decision_type)
        .bind(record.confidence as f32)
        .bind(&record.model)
        .bind(record.input_tokens as i64)
        .bind(record.output_tokens as i64)
        .bind(data)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn query_by_task(&self, task_id: &str) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT record_id, session_id, task_id, timestamp, agent_id,
                   decision_type, confidence, model, input_tokens, output_tokens, data
            FROM explainability
            WHERE task_id = $1
            ORDER BY timestamp DESC
            "#,
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_record).collect())
    }

    async fn query_by_agent(&self, agent_id: &str) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT record_id, session_id, task_id, timestamp, agent_id,
                   decision_type, confidence, model, input_tokens, output_tokens, data
            FROM explainability
            WHERE agent_id = $1
            ORDER BY timestamp DESC
            "#,
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_record).collect())
    }

    async fn query_by_decision_type(
        &self,
        decision_type: &str,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT record_id, session_id, task_id, timestamp, agent_id,
                   decision_type, confidence, model, input_tokens, output_tokens, data
            FROM explainability
            WHERE decision_type = $1
            ORDER BY timestamp DESC
            "#,
        )
        .bind(decision_type)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_record).collect())
    }

    async fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT record_id, session_id, task_id, timestamp, agent_id,
                   decision_type, confidence, model, input_tokens, output_tokens, data
            FROM explainability
            WHERE timestamp >= $1 AND timestamp <= $2
            ORDER BY timestamp DESC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_record).collect())
    }

    async fn query_by_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<ExplainabilityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT record_id, session_id, task_id, timestamp, agent_id,
                   decision_type, confidence, model, input_tokens, output_tokens, data
            FROM explainability
            WHERE session_id = $1
            ORDER BY timestamp DESC
            "#,
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_record).collect())
    }

    async fn get_by_id(&self, record_id: &str) -> anyhow::Result<Option<ExplainabilityRecord>> {
        let row = sqlx::query(
            r#"
            SELECT record_id, session_id, task_id, timestamp, agent_id,
                   decision_type, confidence, model, input_tokens, output_tokens, data
            FROM explainability
            WHERE record_id = $1
            "#,
        )
        .bind(record_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(row_to_record))
    }

    async fn count_by_task(&self, task_id: &str) -> anyhow::Result<usize> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM explainability WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count as usize)
    }

    async fn aggregate_confidence_by_model(&self) -> anyhow::Result<HashMap<String, f64>> {
        let rows = sqlx::query(
            r#"
            SELECT model, AVG(confidence) as avg_conf, COUNT(*) as cnt
            FROM explainability
            WHERE confidence IS NOT NULL AND model IS NOT NULL AND model != ''
            GROUP BY model
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut result = HashMap::new();
        for row in rows {
            let model: String = row.try_get("model")?;
            let avg: f32 = row.try_get("avg_conf")?;
            result.insert(model, avg as f64);
        }

        Ok(result)
    }
}

fn row_to_record(row: sqlx::postgres::PgRow) -> ExplainabilityRecord {
    let data: serde_json::Value = row.try_get("data").unwrap_or_default();
    let reasoning_chain = data
        .get("reasoning_chain")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let metadata = data
        .get("metadata")
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    ExplainabilityRecord {
        record_id: row.try_get("record_id").unwrap_or_default(),
        session_id: row.try_get("session_id").ok(),
        task_id: row.try_get("task_id").ok(),
        agent_id: row.try_get("agent_id").ok(),
        decision_type: row.try_get("decision_type").unwrap_or_default(),
        confidence: row.try_get::<f32, _>("confidence").unwrap_or(0.0) as f64,
        reasoning_chain,
        model: row.try_get("model").unwrap_or_default(),
        input_tokens: row.try_get::<i64, _>("input_tokens").unwrap_or(0) as u64,
        output_tokens: row.try_get::<i64, _>("output_tokens").unwrap_or(0) as u64,
        metadata,
        timestamp: row.try_get("timestamp").unwrap_or_else(|_| Utc::now()),
    }
}
